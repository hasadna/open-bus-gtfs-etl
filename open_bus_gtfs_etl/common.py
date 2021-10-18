import os
import shutil
import tempfile
from contextlib import contextmanager

import requests


@contextmanager
def safe_open_write(filename, *args, **kwargs):
    with tempfile.TemporaryDirectory() as tempdir:
        temp_filename = os.path.join(tempdir, "file")
        with open(temp_filename, *args, **kwargs) as f:
            yield f
        shutil.move(temp_filename, filename)


def http_stream_download(filename, **requests_kwargs):
    with requests.get(stream=True, **requests_kwargs) as res:
        res.raise_for_status()
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with safe_open_write(filename, 'wb') as f:
            for chunk in res.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
