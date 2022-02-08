import shutil

from . import common


def main(date):
    date = common.parse_date_str(date)
    dated_workdir = common.get_dated_workdir(date)
    print("Deleting dated workdir {}".format(dated_workdir))
    shutil.rmtree(dated_workdir, ignore_errors=True)
