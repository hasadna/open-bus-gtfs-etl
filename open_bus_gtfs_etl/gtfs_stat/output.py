import logging
from pathlib import Path

import pandas as pd


def save_dataframe_to_file(data_frame: pd.DataFrame,
                           output_path: Path) -> None:

    output_file_type = output_path.suffixes
    output_path: str = output_path.as_posix()

    if output_file_type == ['.pkl', '.gz']:
        data_frame.to_pickle(output_path, compression='gzip')
    elif output_file_type == ['.csv', '.gz']:
        data_frame.to_csv(output_path, index=False, compression='gzip')
    elif output_file_type == ['.csv']:
        data_frame.to_csv(output_path, index=False)
    else:
        err = NotImplementedError(f'Saving dataframe as {output_file_type} is not supported')
        logging.error(err)

        raise err


def read_stat_file(path: Path):
    return pd.read_csv(path)
