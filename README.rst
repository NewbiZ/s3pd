S3 parallel downloader
----------------------

`s3pd` is both a python library and a command line tool to download files from S3 in parallel.

Compared to e.g. `boto3`, `s3pd` is able to efficiently download chunks of files in parallel, resulting in a x5 to x10 download speed compared to `AWSCLI` / `boto`.

Installation
------------

.. code-block:: bash

    $ pip install s3pd

Usage: CLI
----------

.. code-block:: bash

    $ s3pd --help
    Download S3 files concurrently.
    
    Usage:
        s3pd [options] <SOURCE> [<DESTINATION>]
    
    Options:
        -p,--processes=<PROCESSES>      Number of concurrent download processes
                                        [default: 4]
        -c,--chunksize=<CHUNKSIZE>      Size of chunks for each worker, in bytes
                                        [default: 8388608]
        -u,--unsigned                   Use unsigned requests

The `destination` argument is optional. In this case, the file will be downloaded temporarily in a shared memory. This is useful if you do not want the file itself,
but just benchmark the download speed.

The proper values for `chunksize` and `processes` depends a lot on your setup. For huge files on AWS, I would recommend 64/128/512MB chunksize with 16 processes for near good performance (tested on VPC endpoints to reach ~1GB/s).

Usage: python library
---------------------

The main entry point of the library is the `s3pd` function.

.. code-block:: python

    s3pd(url, processes=8, chunksize=67108864, destination=None, func=None, signed=True)

- `url`: an URL using the `s3` scheme, such as `s3://bucket/key/to/file`.
- `processes`: number of concurrent download processes.
- `chunksize`: size of each download chunk (in bytes).
- `signed`: used signed or unsigned requests.
- `destination`: destination filename where the file will be downloaded. Mutually exclusive with the `func` parameter.
- `func`: when this is provided, the file will be downloaded to a shared memory, the provided function applied on the filename, and the temporary file destroyed. This is particularly useful when you want to transparently load a file from S3 and process it without keeping the file (and your API only support filenames and not `fileobjs`).

Example
-------

Download a huge file from S3 using 16 processes and 64MB chunksize:

.. code-block:: python

    from s3pd import s3pd

    store = s3pd(url='s3://bucket/key/to/file.h5', chunksize=64*1024*1024, processes=16, func=pd.HDFStore)

Author
------

- Aurelien Vallee <vallee.aurelien@gmail.com>
