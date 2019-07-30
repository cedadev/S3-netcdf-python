from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
from netCDF4._netCDF4 import Dataset
import numpy as np
import cProfile, pstats

def create_test_dataset(ncd, shape=[30,1,192,145]):
    """Create a test dataset for a netCDF file"""
    ncd.history = "memory leak test of memio version of python-netcdf4 library"

    # create a group
    group = ncd.createGroup("test_group")
    group.group_class = "Wotsits"

    # create the dimension, the variable, add the variable values and some
    # metadata    time_dim = group.createDimension("time", shape[0], axis_type="T")

    time_dim = group.createDimension("time", shape[0])
    time_var = group.createVariable("time", np.float32, ("time",))
    time_var[:] = np.arange(0, shape[0])
    time_var.units = "days since 2000-01-01"
    time_var.axis = "T"

    level_dim = group.createDimension("level", shape[1])
    level_var = group.createVariable("level", np.float32, ("level",))
    level_var[:] = np.arange(0, shape[1])*100
    level_var.standard_name = "height above sea-level"
    level_var.units = "m"

    latitude_dim = group.createDimension("latitude", shape[2])
    latitude_var = group.createVariable("latitude", np.float32, ("latitude",))
    latitude_vals = 90.0 - np.arange(0, shape[2]) * 180.0/(shape[2]-1)
    latitude_var[:] = latitude_vals
    latitude_var.standard_name = "latitude"
    latitude_var.units = "degrees north"
    latitude_var.setncatts({"name": "value", "test":234235})

    longitude_dim = group.createDimension("longitude", shape[3])
    longitude_var = group.createVariable("longitude", np.float32, ("longitude",))
    longitude_vals = np.arange(0, shape[3]) * 360.0/shape[3]
    longitude_var[:] = longitude_vals
    longitude_var.standard_name = "longitude"
    longitude_var.units = "degrees east"

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

# ds = s3Dataset(
#     "/Users/dhk63261/Test/netCDF4_test_4.nc", format="CFA4", mode='w'
# )
#
# #hi_res_deg=0.0125
# res_deg = 1.0
#
# # very high-res dataset
# #create_test_dataset(ds, shape=[365*4, 200, 180/hi_res_deg, 360/hi_res_deg])
# # regular res dataset
# create_test_dataset(ds, shape=[365, 60, 180/res_deg, 360/res_deg])
# tmp_var = ds.groups["test_group"].variables["tmp"]
# ds.close()

dr = s3Dataset(
    "/Users/dhk63261/Test/netCDF4_test_4.nc", mode='r'
)
# print(dr)
cfa_var = dr._cfa_dataset.getGroup("test_group").getVariable("tmp")
x = (cfa_var[240:260,0:20,0:20,0:20])
grp = dr.groups["test_group"]
scl = dr.variables["scl"]
tmp_var = grp.variables["tmp"]
dr.close()