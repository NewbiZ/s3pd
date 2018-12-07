"""Download S3 files concurrently.

Usage:
    s3pd [options] <SOURCE> [<DESTINATION>]

Options:
    -p,--processes=<PROCESSES>      Number of concurrent download processes
                                    [default: 4]
    -c,--chunksize=<CHUNKSIZE>      Size of chunks for each worker, in bytes
                                    [default: 8388608]
    -u,--unsigned                   Use unsigned requests
"""
from docopt import docopt

from s3pd import s3pd

def main():
    args = docopt(__doc__, version='1.0.0')

    source = args['<SOURCE>']
    destination = args['<DESTINATION>']

    processes = int(args['--processes'])
    chunksize = int(args['--chunksize'])
    signed = not args['--unsigned']

    s3pd(
        url=source,
        processes=processes,
        chunksize=chunksize,
        destination=destination,
        signed=signed)
