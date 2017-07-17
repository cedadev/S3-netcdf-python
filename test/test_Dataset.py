"""
Test the S3 enabled version of netCDF4.

Author: Neil Massey
Date:   10/07/2017
"""
import sys, os
sys.path.append(os.path.expanduser("~/Coding/S3-netcdf-python/"))

from S3netCDF4 import s3Dataset as Dataset
from netCDF4._netCDF4 import Dataset as NC4Dataset

from timeit import default_timer as timer
from tempfile import NamedTemporaryFile

# test S3 dataset and filesystem dataset
S3_DATASET_PATH = "s3://minio/cru-ts-3.24.01/data/tmp/cru_ts3.24.01.1951.1960.tmp.dat.nc"
NC_DATASET_PATH  = "/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1951.1960.tmp.dat.nc"
NC4_DATASET_PATH = "/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.2011.2015.tmp/cru_ts3.24.01.2011.2015.tmp.dat_20110116.nc"
S3_NC4_DATASET_PATH = "s3://minio/cru-ts-3.24.01/data/tmp/bbb0-cru_ts3.24.01.2011.2015.tmp.dat.nca/cru_ts3.24.01.2011.2015.tmp.dat_20110116.nc"
S3_NOT_NETCDF_PATH = "s3://minio/cru-ts-3.24.01/Botley_Timetable_Sept2016v4.pdf"

def test_open_dataset():
    #nc_data = Dataset(NC_DATASET_PATH)
    #print nc_data
    #nc4_data = Dataset(NC4_DATASET_PATH)
    #print nc4_data
    start = timer()
    s3_data = Dataset(S3_DATASET_PATH, mode='r', delete_from_cache=True)
    end = timer()
    print s3_data
    print end - start
    s3_data.close() # this is necessary to delete from the cache

    s3_nc4_data = Dataset(S3_NC4_DATASET_PATH, mode='r')
    print s3_nc4_data
    s3_nc4_data.close()
    not_nc_data = Dataset(S3_NOT_NETCDF_PATH)

if __name__ == "__main__":
    test_open_dataset()