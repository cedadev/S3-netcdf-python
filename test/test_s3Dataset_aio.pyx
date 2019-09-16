from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
from netCDF4._netCDF4 import Dataset
import numpy as np
import json
import cProfile, pstats

def create_test_dataset_nc4(ncd, shape=[30,1,192,145]):
    """Create a test dataset for a netCDF file"""
    ncd.history = "Test of CFA-0.5 format"

    # create a group
    group = ncd.createGroup("test_group")
    group.group_class = "Surface variables"

    # create the dimension, the variable, add the variable values and some
    # metadata    time_dim = group.createDimension("time", shape[0], axis_type="T")

    print("Creating time")
    time_dim = group.createDimension("time", shape[0])
    time_var = group.createVariable("time", np.float32, ("time",))
    time_var[:] = np.arange(0, shape[0])
    time_var.units = "days since 2000-01-01"
    time_var.axis = "T"

    print("Creating level")
    level_dim = group.createDimension("level", shape[1])
    level_var = group.createVariable("level", np.float32, ("level",))
    level_var[:] = np.arange(0, shape[1])*100
    level_var.standard_name = "height above sea-level"
    level_var.units = "m"

    print("Creating latitude")
    latitude_dim = group.createDimension("latitude", shape[2])
    latitude_var = group.createVariable("latitude", np.float32, ("latitude",))
    latitude_vals = 90.0 - np.arange(0, shape[2]) * 180.0/(shape[2]-1)
    latitude_var[:] = latitude_vals
    latitude_var.standard_name = "latitude"
    latitude_var.units = "degrees north"
    latitude_var.setncatts({"name": "value", "test":234235})

    print("Creating longitude")
    longitude_dim = group.createDimension("longitude", shape[3])
    longitude_var = group.createVariable("longitude", np.float32, ("longitude",))
    longitude_vals = np.arange(0, shape[3]) * 360.0/shape[3]
    longitude_var[:] = longitude_vals
    longitude_var.standard_name = "longitude"
    longitude_var.units = "degrees east"

    print("Creating tmp")
    # create the field variable and data
    tmp_var = group.createVariable("tmp", np.float32,
                                    ("time", "level", "latitude", "longitude"),
                                    fill_value=20e2
                                  )
    tmp_var.standard_name = "temperature"
    tmp_var.units = "degrees C"
    #tmp_var[:] = np.random.random(shape) * 60.0 - 20.0
    scl_var = ncd.createVariable("scl", np.float32)

    # time = ncd.createDimension("time", shape[0])
    # ncd.renameDimension("time", "time_again")
    # tmp2_var = ncd.createVariable("tmp2", np.float32, ("time_again",))
    # tmp2_var = ncd.createVariable("tmp3", np.float32, ("time_again",))

def create_test_dataset_nc3(ncd, shape=[30,1,192,145]):
    """Create a test dataset for a netCDF file"""
    ncd.history = "testing S3-netCDF3"

    time_dim = ncd.createDimension("time", shape[0])
    time_var = ncd.createVariable("time", np.float32, ("time",))
    time_var[:] = np.arange(0, shape[0])
    time_var.units = "days since 2000-01-01"
    time_var.axis = "T"

    level_dim = ncd.createDimension("level", shape[1])
    level_var = ncd.createVariable("level", np.float32, ("level",))
    level_var[:] = np.arange(0, shape[1])*100
    level_var.standard_name = "height above sea-level"
    level_var.units = "m"

    latitude_dim = ncd.createDimension("latitude", shape[2])
    latitude_var = ncd.createVariable("latitude", np.float32, ("latitude",))
    latitude_vals = 90.0 - np.arange(0, shape[2]) * 180.0/(shape[2]-1)
    latitude_var[:] = latitude_vals
    latitude_var.standard_name = "latitude"
    latitude_var.units = "degrees north"
    latitude_var.setncatts({"name": "value", "test":234235})

    longitude_dim = ncd.createDimension("longitude", shape[3])
    longitude_var = ncd.createVariable("longitude", np.float32, ("longitude",))
    longitude_vals = np.arange(0, shape[3]) * 360.0/shape[3]
    longitude_var[:] = longitude_vals
    longitude_var.standard_name = "longitude"
    longitude_var.units = "degrees east"

    # create the field variable and data
    tmp_var = ncd.createVariable("tmp", np.float32,
                                  ("time", "level", "latitude", "longitude"),
                                   fill_value=20e2,
                                )
    tmp_var.standard_name = "temperature"
    tmp_var.units = "degrees C"
    #tmp_var[:] = np.random.random(shape) * 60.0 - 20.0
    scl_var = ncd.createVariable("scl", np.float32)

# print("Regular NC4 file")
# ds = s3Dataset(
#     "/Users/dhk63261/Test/netCDF4_test_nc.nc", format="NETCDF4", mode='w'
# )
# res_deg = 1.25
# create_test_dataset_nc4(ds, shape=[365, 60, 180/res_deg, 360/res_deg])
# ds.close()

# print("Regular NC3 file")
# ds = s3Dataset(
#     "/Users/dhk63261/Test/netCDF3_test_nc.nc", format="NETCDF3_CLASSIC", mode='w'
# )
# res_deg = 2.0
# create_test_dataset_nc3(ds, shape=[365, 19, 180/res_deg, 360/res_deg])
# ds.close()


# print("Write CFA 0.4 file")
# ds = s3Dataset(
#     "s3://cedadev-o/nrmassey/netCDF4_test_0.4.nc", format="CFA4", mode='w',
#     cfa_version="0.4"
# )
# res_deg = 1.0
# create_test_dataset_nc4(ds, shape=[365, 60, 180/res_deg, 360/res_deg])
# ds.close()
#

print("Write CFA 0.4 file")
ds = s3Dataset(
#    "s3://cedadev-o/nrmassey/netCDF4_test_0.5.nc", format="CFA4", mode='w',
    "/Users/dhk63261/Test/netCDF4_test_0.4.nc", format="CFA4", mode='w',
    cfa_version="0.4"
)
res_deg = 1.0
create_test_dataset_nc4(ds, shape=[365, 60, 180/res_deg, 360/res_deg])
ds.close()

print("Write CFA 0.5 file")
ds = s3Dataset(
#    "s3://cedadev-o/nrmassey/netCDF4_test_0.5.nc", format="CFA4", mode='w',
    "/Users/dhk63261/Test/netCDF4_test_0.5.nc", format="CFA4", mode='w',
    cfa_version="0.5"
)
res_deg = 1.0
create_test_dataset_nc4(ds, shape=[365, 60, 180/res_deg, 360/res_deg])
print(ds.groups["test_group"].variables["tmp"][0,0,0:10,20:30])
ds.close()

# very high-res dataset
# hi_res_deg=0.0125
# create_test_dataset(ds, shape=[365*4, 200, 180/hi_res_deg, 360/hi_res_deg])
# ds.close()

print("Read CFA 0.5 file")
dr = s3Dataset(
    "/Users/dhk63261/Test/netCDF4_test_0.5.nc", mode='r',
)
#print(dr.variables, dr.groups, dr.dimensions)
x = dr.groups["test_group"]
tmp = x.variables["tmp"]
print(tmp[0,0,90:180,:])
# v = dr.variables["scl"]
