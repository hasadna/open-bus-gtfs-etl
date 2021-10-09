import os
import shutil
from contextlib import closing
from pathlib import Path
from typing import Dict
from urllib import request

from pydantic import BaseModel
from tqdm import tqdm

GTFS_METADATA_FILE = '.gtfs_metadata.json'
__CONFIGURATION_FILE_NAME = "gtfs_extractor.config"


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


GTFS_EXTRACTOR_CONFIG = GtfsExtractorConfig.parse_file(Path(__file__)
                                                       .parent.joinpath(__CONFIGURATION_FILE_NAME))


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

        for item in tqdm(args.values()):
            self.download_file_from_ftp(url=item.url, local_file=Path(self.folder, item.local_name))

        with open(Path(self.folder, GTFS_METADATA_FILE), 'w') as metadata_file:
            metadata_file.write(gtfs_files.json())

        return gtfs_files

    @staticmethod
    def download_file_from_ftp(url, local_file: Path):
        with closing(request.urlopen(url)) as downloaded_content:
            with local_file.open('wb') as target_file:
                shutil.copyfileobj(downloaded_content, target_file)
