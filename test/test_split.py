
import unittest, os

from S3netCDF4.utils.split import split_into_CFA
from S3netCDF4.utils.agg import aggregate_into_CFA
from S3netCDF4.CFA._CFAClasses import CFADataset
from S3netCDF4._s3netCDF4 import s3Dataset

TESTFILE = "/Users/BNL28/Data/nc/ta_Amon_HadCM3_rcp45_r10i1p1_200601-203012.nc"


def nca_equivalence(ncfile1, ncfile2, variable='ta'):
    """ Do these two files describe the same content?"""
    # Let's start by comparing a few important things

    x = s3Dataset(ncfile1)
    y = s3Dataset(ncfile2)

    # First let's just check a data record
    xx = x.variables[variable]
    yy = y.variables[variable]

    assert (xx.shape == yy.shape).all(), "CFA data arrays are not the same shape"

    assert len(xx.shape) == 4, "Unexpected variable shape for comparison"

    xx = xx[:, 0, 0, 0].flatten()
    yy = yy[:, 0, 0, 0].flatten()

    # We don't do all data coz it would take a long time
    assert (xx == yy).all(), "Data in arrays does not match"

    # now check file headers

    raise NotImplementedError("This doesn't mean the test has failed, just the test code is not finished")

    #return statement needed

class TestSplit(unittest.TestCase):
    """ All the necessary splitter tests"""

    def setUp(self):
        self.ncafile1 = '/tmp/things1.nca'
        self.ncapath = '/tmp/things1/things1.ta.*'
        self.ncafile2 = '/tmp/things2.nca'

    def _split_and_aggregate(self, cfa1, cfa2):
        # for now use real disk ...
        input = TESTFILE
        subarray_size = 50 * 1024 * 1024
        subarray_path = ""
        subarray_shape = "[2, 17,  73, 96]"

        split_into_CFA(self.ncafile1, input,
                       subarray_path,
                       subarray_shape,
                       int(subarray_size),
                       cfa1)

        axis = 'time'
        common_date = None

        aggregate_into_CFA(self.ncafile2,
                           self.ncapath,
                           axis,
                           cfa2,
                           common_date)

    def test_data_available(self):
        """ Test there is an input dataset available."""
        assert os.path.exists(TESTFILE)

    def test_file_handles(self):
        """ Test we can open a file for write without fully qualifying it's name.
        See issue:24 """
        raise NotImplementedError

    def test_auto_split_and_agg_round_trip1(self):
        """ Test the sensible split and aggregate
         with both at CFA 0.4 """

        self._split_and_aggregate("0.4", "0.4")

        self.assertTrue(nca_equivalence(self.ncafile1, self.ncafile2))

    def test_auto_split_and_agg_round_trip2(self):
        """ Test the sensible split and aggregate
         with different CFA versions """

        self._split_and_aggregate("0.4", "0.5")

        self.assertTrue(nca_equivalence(self.ncafile1, self.ncafile2))