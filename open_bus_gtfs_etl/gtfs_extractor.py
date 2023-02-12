import os
import shutil
import time
from contextlib import closing
from logging import getLogger
from pathlib import Path
from typing import Dict, List
from urllib import request
from urllib.error import URLError
import ssl

from pydantic import BaseModel
from tqdm import tqdm

GTFS_METADATA_FILE = '.gtfs_metadata.json'

logger = getLogger(__file__)


class DownloadingException(Exception):
    pass


class FileConfig(BaseModel):
    # pylint: disable=missing-class-docstring
    url: str
    local_name: str


class GtfsExtractorConfig(BaseModel):
    # pylint: disable=missing-class-docstring
    gtfs_file: FileConfig
    tariff_file: FileConfig
    cluster_file: FileConfig
    trip_id_to_date_file: FileConfig
    download_retries_delay: List[int]


GTFS_EXTRACTOR_CONFIG: GtfsExtractorConfig = GtfsExtractorConfig.parse_obj({
  "gtfs_file": {
    "url": "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip",
    "local_name": "israel-public-transportation.zip"
  },
  "tariff_file": {
    "url": "https://gtfs.mot.gov.il/gtfsfiles/Tariff.zip",
    "local_name": "Tariff.zip"
  },
  "cluster_file": {
    "url": "https://gtfs.mot.gov.il/gtfsfiles/ClusterToLine.zip",
    "local_name": "ClusterToLine.zip"
  },
  "trip_id_to_date_file": {
    "url": "https://gtfs.mot.gov.il/gtfsfiles/TripIdToDate.zip",
    "local_name": "TripIdToDate.zip"
  },
  "download_retries_delay": [30, 60, 120, 240, 240, 240, 240, 240, 240, 240, 240, 240, 240]
})


class GTFSFiles(BaseModel):
    """
    GTFSFiles represents collection of GTFS files paths in local machine
    """
    gtfs: Path
    tariff: Path
    cluster_to_line: Path
    trip_id_to_date: Path


class GtfsRetriever:
    """
    GtfsRetriever is utility to manage downloading GTFS files and creating metadata file
    """
    def __init__(self, folder: Path, app_config: GtfsExtractorConfig = GTFS_EXTRACTOR_CONFIG):
        self.folder = folder
        self.app_config = app_config

    def retrieve_gtfs_files(self) -> GTFSFiles:
        os.makedirs(self.folder, exist_ok=True)

        args: Dict[str, FileConfig] = dict(gtfs=self.app_config.gtfs_file,
                                           tariff=self.app_config.tariff_file,
                                           cluster_to_line=self.app_config.cluster_file,
                                           trip_id_to_date=self.app_config.trip_id_to_date_file)

        gtfs_files = GTFSFiles(**{k: Path(self.folder, v.local_name) for k, v in args.items()})

        tries = self.app_config.download_retries_delay.copy()
        tries.append(0)

        for i, delay_in_sec in enumerate(tries, start=1):
            try:
                for item in tqdm(args.values()):
                    self.download_file_from_ftp(url=item.url, local_file=Path(self.folder, item.local_name))

                with open(Path(self.folder, GTFS_METADATA_FILE), 'w') as metadata_file:
                    metadata_file.write(gtfs_files.json())

                return gtfs_files

            except URLError as err:
                logger.exception(f"Failed to Download GTFS Files. {i} tryout of "
                                 f"{len(self.app_config.download_retries_delay)} tryouts. Going to wait {delay_in_sec} "
                                 f"sec before the next try. error: {str(err)}")
                time.sleep(delay_in_sec)

        raise DownloadingException('Failed to Download GTFS Files.')

    @staticmethod
    def download_file_from_ftp(url, local_file: Path):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with closing(request.urlopen(url, context=ctx)) as downloaded_content:
            with local_file.open('wb') as target_file:
                shutil.copyfileobj(downloaded_content, target_file)
