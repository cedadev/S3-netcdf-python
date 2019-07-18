# from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
import netCDF4
from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
import numpy as np
import time

def create_test_dataset(ncd, shape=[10,10,10,10]):
    """Create a test dataset for a netCDF file"""
    ncd.history = "memory leak test of memio version of python-netcdf4 library"

    # create the dimension, the variable, add the variable values and some
    # metadata
    time_dim = ncd.createDimension("time", shape[0], axis_type="T")
    time_var = ncd.createVariable("time", np.float32, "time")
    time_var[:] = np.arange(0, shape[0])
    time_var.units = "days since 2000-01-01"

    level_dim = ncd.createDimension("level", shape[1], axis_type="Z")
    level_var = ncd.createVariable("level", np.float32, "level")
    level_var[:] = np.arange(0, shape[1])*100
    level_var.standard_name = "height above sea-level"
    level_var.units = "m"

    latitude_dim = ncd.createDimension("latitude", shape[2], axis_type="Y")
    latitude_var = ncd.createVariable("latitude", np.float32, "latitude")
    latitude_vals = 90.0 - np.arange(0, shape[2]) * 180.0/(shape[2]-1)
    latitude_var[:] = latitude_vals
    latitude_var.standard_name = "latitude"
    latitude_var.units = "degrees north"

    longitude_dim = ncd.createDimension("longitude", shape[3], axis_type="X")
    longitude_var = ncd.createVariable("longitude", np.float32, "longitude")
    longitude_vals = np.arange(0, shape[3]) * 360.0/shape[3]
    longitude_var[:] = longitude_vals
    longitude_var.standard_name = "longitude"
    longitude_var.units = "degrees east"

    # create the field variable and data
    tmp_var = ncd.createVariable("tmp", np.float32,
                                 ("time", "level", "latitude", "longitude"))
    tmp_var.standard_name = "temperature"
    tmp_var.units = "degrees C"
    tmp_var[:] = np.random.random(shape) * 60.0 - 20.0

# test file
# x = s3Dataset(
#     "/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1901.1910.tmp.dat.nc"
# )
# x.close()
# print(x.variables)
# print(x.variables['tmp'])

# test S3
# x = s3Dataset(
#     "s3://cedadev-o/buckettest/cru_ts3.24.01.2011.2015.tmp.dat.nca", "r"
# )
# #print(x.variables)
# #print(x.variables['tmp'])
# x.close()

t_ac = 0

y1 = s3Dataset(
    "s3://cedadev-o/buckettest/netCDF4_test_1.nc", format="NETCDF4", mode='w'
)
create_test_dataset(y1, shape=[100,19,145,192])

# time just the upload of the file on close
t0 = time.time()
y1.close()
t1 = time.time()
t_ac += (t1-t0)

y2 = s3Dataset(
    "s3://cedadev-o/buckettest/netCDF4_test_2.nc", format="NETCDF4", mode='w'
)
create_test_dataset(y2, shape=[100,19,145,192])

t0 = time.time()
y2.close()
t1 = time.time()
t_ac += (t1-t0)

y3 = s3Dataset(
    "s3://cedadev-o/buckettest/netCDF4_test_3.nc", format="NETCDF4", mode='w'
)
create_test_dataset(y3, shape=[100,19,145,192])

t0 = time.time()
y3.close()
t1 = time.time()
t_ac += (t1-t0)

y4 = s3Dataset(
    "s3://cedadev-o/buckettest/netCDF4_test_4.nc", format="NETCDF4", mode='w'
)
create_test_dataset(y4, shape=[100,19,145,192])

t0 = time.time()
y4.close()
t1 = time.time()
t_ac += (t1-t0)

y5 = s3Dataset(
    "s3://cedadev-o/buckettest/netCDF4_test_5.nc", format="NETCDF4", mode='w'
)
create_test_dataset(y5, shape=[100,19,145,192])

t0 = time.time()
y5.close()
t1 = time.time()
t_ac += (t1-t0)

print(t_ac)
#
# z = s3Dataset(
#     "/Users/dhk63261/Archive/netCDF4_test_local.nc", format="NETCDF4", mode='w'
# )
# z.close()
