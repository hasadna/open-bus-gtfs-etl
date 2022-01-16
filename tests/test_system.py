import os
import unittest
from pathlib import Path

from click.testing import CliRunner

from open_bus_gtfs_etl.cli import download, analyze, load_to_db

@unittest.skip("takes too long and depands on DB")
class TestLoader(unittest.TestCase):
    def test_download(self):
        os.chdir(path=Path(__file__).parent.parent)
        runner = CliRunner()
        result = runner.invoke(download, [])
        print(result)
        self.assertEqual(0, result.exit_code)

    def test_analyze(self):
        os.chdir(path=Path(__file__).parent.parent)
        runner = CliRunner()
        result = runner.invoke(analyze, [])
        print(result)
        self.assertEqual(0, result.exit_code)

    def test_load_to_db(self):
        os.chdir(path=Path(__file__).parent.parent)
        runner = CliRunner()
        result = runner.invoke(load_to_db, [])
        print(result)
        self.assertEqual(0, result.exit_code)
