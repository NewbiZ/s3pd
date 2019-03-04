import os
from setuptools import setup, find_packages

def get_version():
    """Read version from s3pd/version.py.
    """
    return open('s3pd/version.py').read().split("'")[1]


setup(
    name='s3pd',
    version=get_version(),
    packages=find_packages(),
    author='Aurelien Vallee',
    author_email='vallee.aurelien@gmail.com',
    description='S3 parallel downloader',
    long_description='Download files from S3 using multiple processes',
    install_requires=[
        'boto3 >=1.7.14, <2',
        'botocore >=1.10.14, <2',
        'docopt >=0.6.2',
    ],
    entry_points="""
        [console_scripts]
        s3pd = s3pd.cli:main
    """,
)
