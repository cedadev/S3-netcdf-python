import sys, os
sys.path.append(os.path.expanduser("~/Coding/S3-netcdf-python/"))

from S3netCDF4._s3netCDFIO import *

S3_DATASET_PATH = "s3://minio/cru-ts-3.24.01/data/tmp/cru_ts3.24.01.1951.1960.tmp.dat.nc"
NC_DATASET_PATH  = "/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1951.1960.tmp.dat.nc"
S3_NOT_NETCDF_PATH = "s3://minio/cru-ts-3.24.01/Botley_Timetable_Sept2016v4.pdf"

def test_s3_open_dataset():
    # get the netCDF filedetails.  If the source file is on S3 storage then the file will be streamed into memory or
    #   cache.  If it is on a POSIX file system then the file name will be returned
    file_details = get_netCDF_file_details(S3_DATASET_PATH, 'r')
    print file_details


def test_file_open_dataset():
    file_details = get_netCDF_file_details(NC_DATASET_PATH, 'r')
    print file_details


def test_s3_open_not_netcdf():
    file_details = get_netCDF_file_details(S3_NOT_NETCDF_PATH, 'r')
    print file_details


if __name__ == "__main__":
    test_s3_open_dataset()
    test_file_open_dataset()
    test_s3_open_not_netcdf()