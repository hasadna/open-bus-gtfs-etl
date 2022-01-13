import time
from os import path

from setuptools import setup, find_packages

if path.exists("VERSION.txt"):
    # this file can be written by CI tools (e.g. Travis)
    with open("VERSION.txt") as version_file:
        VERSION = version_file.read().strip().strip("v")
else:
    VERSION = str(time.time())

setup(
    name='open-bus-gtfs-etl',
    version=VERSION,
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'open-bus-gtfs-etl = open_bus_gtfs_etl.cli:main',
        ]
    },
)
