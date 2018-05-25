import sys, os
import time
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import num2date, date2num

from S3netCDF4._s3netCDF4 import s3Dataset as Dataset

S3_NETCDF_PATH = "s3://minio/test-bucket/test1/test2/netcdf_test.nc"

def test_s3_write_dataset():
    """Test writing a netCDF file to the object store"""
    # create a NETCDF4 file and upload to S3 storage
    # this just follows the tutorial at http://unidata.github.io/netcdf4-python/
    with Dataset(S3_NETCDF_PATH, mode='w', diskless=False, format="CFA4") as s3_data:

        # create levels data
        levels_data = [1000., 850., 700., 500., 300., 250., 200., 150., 100., 50.]

        # create the dimensions
        # two dimensions are unlimited (level and time)
        leveld = s3_data.createDimension("level", len(levels_data))
        timed = s3_data.createDimension("time", 365)
        nlats = 196
        nlons = 256
        latd = s3_data.createDimension("lat", nlats)
        lond = s3_data.createDimension("lon", nlons)

        # create the dimension variables and their attributes
        times = s3_data.createVariable("time", "f8", ("time",))
        times.units = "hours since 0001-01-01 00:00:00.0"
        times.calendar = "gregorian"

        levels = s3_data.createVariable("level", "i4", ("level",))
        levels.units = "hPa"
        levels.standard_name = "air_pressure"

        latitudes = s3_data.createVariable("lat", "f4", ("lat",))
        latitudes.units = "degrees north"
        latitudes.standard_name = "grid_latitude"

        longitudes = s3_data.createVariable("lon", "f4", ("lon",))
        longitudes.units = "degrees east"
        longitudes.standard_name = "grid_longitude"

        # create the field variable
        temp = s3_data.createVariable(
            "tmp",
            "f4",
            ("time", "level", "lat", "lon"),
            fill_value = 2e20
        )
        temp.units = "K"
        temp.standard_name = "air_temperature"
        temp.missing_value = 2e20

        # add some global attributes
        s3_data.description = "bogus example script"
        s3_data.history = "Created " + time.ctime(time.time())
        s3_data.source = "netCDF4 python module tutorial"

        # add data to the lat / lon dimension variables
        lats = [-90.0 + x * 180.0 / (nlats - 1) for x in range(0, nlats)]
        latitudes[:] = lats
        lons = np.arange(-180.0, 180.0, 360.0 / nlons)
        longitudes[:] = lons

        # fill in times
        dates = [datetime(2001, 3, 1) + n * timedelta(hours=24) for n in range(temp.shape[0])]
        times[:] = date2num(dates, units=times.units, calendar=times.calendar)

        # fill in levels
        levels[:] = levels_data

        # add some field data
        temp[:, :, :, :] = np.random.uniform(
            size=(times.shape[0], levels.shape[0], nlats, nlons)
        )

if __name__ == "__main__":
    test_s3_write_dataset()
