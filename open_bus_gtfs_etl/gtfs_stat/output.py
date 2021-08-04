import logging
from pathlib import Path

import pandas as pd


def __extract_file_type(file_path: str):
    return file_path.split('.', 1)[1]


def save_dataframe_to_file(data_frame: pd.DataFrame,
                           output_path: Path) -> None:
    output_path: str = output_path.as_posix()
    output_file_type = __extract_file_type(output_path)

    if output_file_type == 'pkl.gz':
        data_frame.to_pickle(output_path, compression='gzip')
    elif output_file_type == 'csv.gz':
        data_frame.to_csv(output_path, index=False, compression='gzip')
    elif output_file_type == 'csv':
        data_frame.to_csv(output_path, index=False)
    else:
        err = NotImplementedError(f'Saving dataframe as {output_file_type} is not supported')
        logging.error(err)
        raise err
