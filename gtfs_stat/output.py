import logging

import pandas as pd


def __extract_file_type(file_path: str):
    return file_path.split('.', 1)[1]


def save_dataframe_to_file(df: pd.DataFrame,
                           output_path: str) -> None:
    output_file_type = __extract_file_type(output_path)

    if output_file_type == 'pkl.gz':
        df.to_pickle(output_path, compression='gzip')
    elif output_file_type == 'csv.gz':
        df.to_csv(output_path, index=False, compression='gzip')
    elif output_file_type == 'csv':
        df.to_csv(output_path, index=False)
    else:
        err = NotImplementedError(f'Saving dataframe as {output_file_type} is not supported')
        logging.error(err)
        raise err
