import sys, os
import time
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import num2date, date2num
sys.path.append(os.path.expanduser("~/Coding/S3-netcdf-python/"))

from S3netCDF4._s3netCDF4 import s3Dataset as Dataset

S3_DATASET_PATH = "s3://minio/cru-ts-3.24.01/data/tmp/cru_ts3.24.01.1951.1960.tmp.dat.nc"
NC_DATASET_PATH  = "/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1951.1960.tmp.dat.nc"
S3_NOT_NETCDF_PATH = "s3://minio/cru-ts-3.24.01/Botley_Timetable_Sept2016v4.pdf"
S3_WRITE_NETCDF_PATH = "s3://minio/test-bucket/test1/test2/netcdf_test.nc"
S3_CFA_PATH = "s3://minio/weather-at-home/data/1314Floods/a_series/hadam3p_eu_a7tz_2013_1_008571189_0/a7tzga.pdl3dec.nca"

def test_s3_open_dataset():
    """Test opening a netCDF file from the object store"""
    nc_file = Dataset(S3_DATASET_PATH, 'r', diskless=True)
    print nc_file


def test_file_open_dataset():
    """Test opening a netCDF file directly from the filesystem"""
    nc_file = Dataset(NC_DATASET_PATH, 'r')
    print nc_file


def test_s3_open_not_netcdf():
    """Test opening a file on the object store that is NOT a netCDF file"""
    nc_file = Dataset(S3_NOT_NETCDF_PATH, 'r')
    print nc_file


def test_s3_open_cfa():
    nc_file = Dataset(S3_CFA_PATH, 'r')
    print nc_file._file_details

def test_s3_write_dataset():
    """Test writing a netCDF file to the object store"""
    # create a NETCDF4 file and upload to S3 storage
    # this just follows the tutorial at http://unidata.github.io/netcdf4-python/
    s3_data = Dataset(S3_WRITE_NETCDF_PATH, mode='w', format="NETCDF4", diskless=True)
    # create the dimensions
    leveld = s3_data.createDimension("level", None)  # two dimensions are unlimited (level and time)
    timed = s3_data.createDimension("time", None)
    nlats = 1481
    nlons = 1960
    latd = s3_data.createDimension("lat", nlats)
    lond = s3_data.createDimension("lon", nlons)
    # create the dimension variables
    times = s3_data.createVariable("time", "f8", ("time",))
    levels = s3_data.createVariable("level", "i4", ("level",))
    latitudes = s3_data.createVariable("lat", "f4", ("lat",))
    longitudes = s3_data.createVariable("lon", "f4", ("lon",))
    # create the field variable
    temp = s3_data.createVariable("tmp", "f4", ("time", "level", "lat", "lon"))

    # add some attributes
    s3_data.description = "bogus example script"
    s3_data.history = "Created " + time.ctime(time.time())
    s3_data.source = "netCDF4 python module tutorial"
    latitudes.units = "degrees north"
    longitudes.units = "degrees east"
    levels.units = "hPa"
    temp.units = "K"
    times.units = "hours since 0001-01-01 00:00:00.0"
    times.calendar = "gregorian"

    # add data to the lat / lon dimension variables
    lats = [-90.0 + x * 180.0 / (nlats - 1) for x in range(0, nlats)]
    latitudes[:] = lats
    lons = np.arange(-180.0, 180.0, 360.0 / nlons)
    longitudes[:] = lons

    # add some field data
    temp[0:5, 0:10, :, :] = np.random.uniform(size=(5, 10, nlats, nlons))

    # assign time and levels file
    levels[:] = [1000., 850., 700., 500., 300., 250., 200., 150., 100., 50.]

    # fill in times
    dates = [datetime(2001, 3, 1) + n * timedelta(hours=12) for n in range(temp.shape[0])]
    times[:] = date2num(dates, units=times.units, calendar=times.calendar)

    s3_data.close()

    # it's been written so now open it
#    s3_read_data = Dataset(S3_WRITE_NETCDF_PATH, mode='r', diskless=True)
#    print s3_read_data


if __name__ == "__main__":
    test_s3_open_dataset()
    test_file_open_dataset()
    test_s3_write_dataset()
    test_s3_open_not_netcdf()
