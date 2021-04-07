
import unittest, os

from S3netCDF4.utils.split import split_into_CFA
from S3netCDF4.utils.agg import aggregate_into_CFA

TESTFILE = "/Users/BNL28/Data/nc/ta_Amon_HadCM3_rcp45_r10i1p1_200601-203012.nc"


def nca_equivalence(ncfile1, ncfile2):
    """ Do these two files describe the same content?"""
    raise NotImplementedError("This doesn't mean the test has failed, just the test code is not finished")


class TestSplit(unittest.TestCase):
    """ All the necessary splitter tests"""

    def setUp(self):
        self.ncafile1 = '/tmp/things1.nca'
        self.ncapath = '/tmp/things1/things1.ta.*'
        self.ncafile2 = '/tmp/things2.nca'

    def test_data_available(self):
        """ Test there is an input dataset available."""
        assert os.path.exists(TESTFILE)

    def test_file_handles(self):
        """ Test we can open a file for write without fully qualifying it's name.
        See issue:24 """
        raise NotImplementedError

    def test_auto_split_and_agg_round_trip(self):
        """ Test the sensible split and aggregate """

        # for now use real disk ...
        input = TESTFILE
        subarray_size = 50*1024*1024
        subarray_path = ""
        subarray_shape = "[2, 17,  73, 96]"
        cfa_version = "0.4"

        split_into_CFA(self.ncafile1, input,
                       subarray_path,
                       subarray_shape,
                       int(subarray_size),
                       cfa_version)

        axis = 'time'
        common_date = None
        cfa_version = "0.5"

        aggregate_into_CFA(self.ncafile2,
                           self.ncapath,
                           axis,
                           cfa_version,
                           common_date)

        # This fails for two reasons, the routine isn't written, but the ncafiles are
        # currently in different formats because of issue:23
        self.assertTrue(nca_equivalence(self.ncafile1, self.ncafile2))

