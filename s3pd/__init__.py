import mmap
import os
import contextlib
from tempfile import NamedTemporaryFile
from multiprocessing import Pool, current_process
from urllib.parse import urlparse
from io import BytesIO

import botocore
import boto3


LINK_SENTINEL = '#S3LINK#'

@contextlib.contextmanager
def shm_file(size, destination):
    """Create a named shared memory and return its file object.

    :param size: Size of the file to create, in bytes.
    :param destination: Path of the file to create, or `None` to create a
        temporary file in /dev/shm.
    :return: A tuple `(file object, path)` to be used from a context manager.
    """
    if destination:
        with open(destination, 'w+b') as shmfile:
            os.truncate(shmfile.fileno(), size)
            shmfile.seek(0)
            yield shmfile, destination
    else:
        with NamedTemporaryFile(mode='wb', prefix='s3-', dir='/dev/shm') as shmfile:
            os.truncate(shmfile.fileno(), size)
            shmfile.seek(0)
            yield shmfile, shmfile.name

@contextlib.contextmanager
def shm_map(fileno, offset, size):
    """Create a memory map of a file or shared memory and return it.

    :param fileno: File descriptor of the file on which to create the memory
        map.
    :param offset: Offset in the file to map, in bytes.
    :param size: Size of the mapping, in bytes.
    :return: The memory map object.
    """
    assert offset % mmap.ALLOCATIONGRANULARITY == 0
    shm = mmap.mmap(fileno=fileno, length=size, offset=offset)
    yield shm
    shm.close()

def get_filesize(client, bucket, key, version=None):
    """Return the size of file on S3.

    :param client: The client to use for performing the query.
    :param bucket: Name of the S3 bucket.
    :param key: Path inside the bucket (without leading `/`)
    :param version: The file version to retrieve, or None
    :return: The file size, in bytes.
    """
    args = {
        'Bucket': bucket,
        'Key': key,
        **({'VersionId': version} if version else {}),
    }
    return client.head_object(**args)['ContentLength']

def create_chunks(chunksize, filesize):
    """Generate list of constant size chunks from a filesize.

    The chunksize should be a multiple of mmap.ALLOCATIONGRANULARITY (most
    likely 4KB). Example:

        >>> create_chunks(5, 12)
        [(0, 4), (5, 9), (10, 11)]

    :param chunksize: Desired chunk size, in bytes.
    :param filesize: Provided file size, in bytes.
    :return: A list of tuples `(offset first, offset last)` for every chunk
        of the file. Each offset will have exactly the same size (`chunksize`)
        except of course the last one if the file size is not divisible by
        the chunk size.
    """
    return [
        (i, min(i+chunksize-1, filesize-1)) for i in range(0, filesize, chunksize)]

def create_client(signed=True):
    """Create a boto client.

    :param signed: If `False` return a client not signing requests.
    :return: The `boto3.Client`.
    """
    if signed:
        return boto3.client('s3')
    else:
        return boto3.client(
            's3',
            config=botocore.config.Config(signature_version=botocore.UNSIGNED))

def download_chunk(
        bucket, key, shmfileno, offset_first, offset_last, signed, version=None):
    """Worker function to download a chunk of the file.

    :param bucket: Name of the S3 bucket.
    :param key: Path inside the bucket (without leading `/`)
    :param shmfileno: File descriptor for an opened destination file.
    :param offset_first: Start position of the chunk to download.
    :param offset_last: Last position of the chunk to download.
    :param version: The file version to retrieve, or None
    :return: Nothing, the chunk is directly copied in the destination file.
    """
    client = create_client(signed)
    args = {
        'Bucket': bucket,
        'Key': key,
        'Range': 'bytes=%s-%s' % (offset_first, offset_last),
        **({'VersionId': version} if version else {}),
    }
    with shm_map(shmfileno, offset_first, offset_last - offset_first + 1) as shmmap:
        chunk = client.get_object(**args)['Body']
        chunk._raw_stream.readinto(shmmap)

def resolve_link(bucket, key, client, depth=10):
    """Resolve S3 links to target key.

    :param bucket: Name of the S3 bucket.
    :param key: Path inside the bucket (without leading `/`)
    :param client: boto3 client to use when performing requests.
    :param depth: Maximum number of link indirections before stopping.
    """
    # Stop after too many link indirections
    assert depth > 0, 'Too many levels of link indirections'

    filesize = get_filesize(client, bucket, key)

    # There is no need to resolve files with a size >1KB, these could not
    # realistically be links
    if filesize > 1024:
        return bucket, key

    with BytesIO() as stream:
        client.download_fileobj(Bucket=bucket, Key=key, Fileobj=stream)
        # In case decoding utf-8 fails, then we are not in a presence of a link
        try:
            content = stream.getvalue().decode('utf-8').strip()
        except:
            return bucket, key

    # Check whether this file is a link
    if not content.startswith(LINK_SENTINEL):
        return bucket, key

    url = content[len(LINK_SENTINEL):]
    parsed_url = urlparse(url)
    path = parsed_url.path

    return resolve_link(
        # In case the link url ommits the s3://bucket/ part, then
        # assume it is a key relative to the current bucket
        bucket=parsed_url.netloc or bucket,
        # S3 keys do not start with /
        key=path if not path.startswith('/') else path[1:],
        client=client,
        depth=depth-1)

def s3pd(
        url, processes=8, chunksize=67108864, destination=None, func=None,
        signed=True, version=None):
    """Main entry point to download an s3 file in parallel.

    Example to download a file locally:
        >>> s3pd(url='s3://bucket/file.txt', destination='/tmp/file.txt')

    Example to load an HDF5 file from S3:
        >>> df = s3pd(url='s3://bucket/file.h5', func=pd.read_hdf)

    :param url: S3 address of the file to download, using the 's3' scheme
        such as `s3://bucket-name/file/to/download.txt`.
    :param processes: Number of processes to use for the download, default
        to 8. Forced to 1 if not the main process when using multiprocessing.
    :param chunksize: Size of each chunk to download, default to 64MB.
    :param destination: Destination path for the downloaded file, including the
        filename. If `None`, a temporary file is created in /dev/shm and you
        should provide a `func` to apply on the filename and return. This is
        useful if just want to apply a function (e.g. loading) on a remote
        file.
    """
    assert chunksize % mmap.ALLOCATIONGRANULARITY == 0

    parsed_url = urlparse(url)
    bucket = parsed_url.netloc
    key = parsed_url.path[1:]
    client = create_client(signed)

    bucket, key = resolve_link(bucket, key, client)

    filesize = get_filesize(client, bucket, key, version=version)
    chunks = create_chunks(chunksize, filesize)

    # Prevent multiprocessing children to fork
    if current_process().daemon:
        processes = 1

    with shm_file(filesize, destination) as (shmfile, shmfilename):
        download_tasks = [
            (bucket, key, shmfile.fileno(), *chunk, signed, version)
            for chunk in chunks]

        if processes == 1:
            for task in download_tasks:
                download_chunk(*task)
        else:
            with Pool(processes=processes) as pool:
                pool.starmap(download_chunk, download_tasks)

        if func:
            return func(shmfilename)
