from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
from S3netCDF4._Exceptions import APIException
import numpy as np
import unittest
import os

DEBUG = False

def create_test_dataset(s3_ds, format, cfa_version, shape=[30,1,192,145]):
    """Create a test dataset for a netCDF file"""
    s3_ds.history = "Test of s3netCDF: format: {} cfa_version: {}".format(
        format, cfa_version
    )

    # create a group if this is a netCDF4 (or CFA4 equivalent) file
    if format == "NETCDF4" or format == "CFA4":
        group = s3_ds.createGroup("test_group")
    # otherwise for netCDF3 files the group is the dataset
    else:
        group = s3_ds
    group.group_class = "Surface variables"

    # create the dimension, the variable, add the variable values and some
    # metadata
    if DEBUG:
        print("\t . Creating time")
    time_dim = group.createDimension("time", shape[0])
    time_var = group.createVariable("time", np.float32, ("time",))
    time_var[:] = np.arange(0, shape[0])
    time_var.units = "days since 2000-01-01"
    time_var.axis = "T"

    if DEBUG:
        print("\t . Creating level")
    level_dim = group.createDimension("level", shape[1])
    level_var = group.createVariable("level", np.float32, ("level",))
    level_var[:] = np.arange(0, shape[1])*100
    level_var.standard_name = "height above sea-level"
    level_var.units = "m"

    if DEBUG:
        print("\t . Creating latitude")
    latitude_dim = group.createDimension("latitude", shape[2])
    latitude_var = group.createVariable("latitude", np.float32, ("latitude",))
    latitude_vals = 90.0 - np.arange(0, shape[2]) * 180.0/(shape[2]-1)
    latitude_var[:] = latitude_vals
    latitude_var.standard_name = "latitude"
    latitude_var.units = "degrees north"
    latitude_var.setncatts({"name": "value", "test":234235})

    if DEBUG:
        print("\t . Creating longitude")
    longitude_dim = group.createDimension("longitude", shape[3])
    longitude_var = group.createVariable("longitude", np.float32, ("longitude",))
    longitude_vals = np.arange(0, shape[3]) * 360.0/shape[3]
    longitude_var[:] = longitude_vals
    longitude_var.standard_name = "longitude"
    longitude_var.units = "degrees east"

    if DEBUG:
        print("\t . Creating tmp")
    # create the field variable and data
    subarray_shape = np.array(
        [12, shape[1], shape[2], shape[3]],
        dtype='i'
    )
    tmp_var = group.createVariable("tmp", np.float32,
                                    ("time", "level", "latitude", "longitude"),
                                    fill_value=2e2,
                                    subarray_shape=subarray_shape
                                  )
    tmp_var.standard_name = "temperature"
    tmp_var.units = "degrees C"
    tmp_var.setncattr("long_name", "Surface temperature at 1m")
    tmp_var._FillValue = np.float32(2e20)  # strict typing matches variable

    if DEBUG:
        print("\t . Writing data")

    # write a single scalar of data
    scl_var = s3_ds.createVariable("scl", np.float32)

    # write a vector of data
    vec_dim = s3_ds.createDimension("vector", 128)
    vec_var = s3_ds.createVariable("vector", np.int32, ("vector",))
    vec_var[:] = 12+np.arange(0,128)
    velocity = s3_ds.createVariable("velocity", np.float32, ("vector",))
    velocity.units = "ms-1"

def get_file_path(path_stub, format, cfa_version=None):
    """Get the path to the file for reading or writing.
    Based on the path_stub, the format and cfa_version.
    """
    file_name = "{}_{}".format(path_stub, format)
    if cfa_version is not None:
        file_name += "_cfa{}".format(cfa_version)
    file_name += ".nc"
    return file_name

def test_s3Dataset_write(path_stub, format="NETCDF4", cfa_version="0.4",
                         resolution_degrees=1.5):
    """Test writing out a s3Dataset, for one of the various permutations of:
        1. file format (netCDF3 or netCDF4)
        2. whether it is a S3-netCDF / CFA file or a plain netCDF file
        3. the CFA version (0.4 or 0.5)
    """
    # build a file name from the path stub, the format and the cfa_version
    # don't use os.path.join as it doesn't handle URLs and paths
    file_name = get_file_path(path_stub, format, cfa_version)
    if DEBUG:
        print("Test writing {}".format(file_name))
    # open the dataset
    ds = s3Dataset(file_name, format=format, mode='w', cfa_version=cfa_version,
                   diskless=False, persist=False)
    # construct the shape:
    shape=[365, 1, 180.0/resolution_degrees+1, 360.0/resolution_degrees]
    # create the data inside the dataset
    create_test_dataset(ds, format, cfa_version, shape)
    if DEBUG:
        print(ds.groups["test_group"].variables["tmp"])
        print(ds.variables["scl"])

    if format == "CFA4" or format == "NETCDF4":
        tmp_var = ds.groups["test_group"].variables["tmp"]
    else:
        tmp_var = ds.variables["tmp"]
    tmp_var[:,:,:,:] = 250.0
    vel_var = ds.variables["velocity"]
    vel_var[0] = 10.0
    ds.close()
    return True

def test_s3Dataset_read(path_stub, format="NETCDF4", cfa_version=None):
    """Test writing out a s3Dataset, for one of the various permutations of:
        1. file format (netCDF3 or netCDF4)
        2. whether it is a S3-netCDF / CFA file or a plain netCDF file
        3. the CFA version (0.4 or 0.5)
    """
    file_name = get_file_path(path_stub, format, cfa_version)
    if DEBUG:
        print("Test reading {}".format(file_name))
    # open the dataset
    dr = s3Dataset(file_name, mode='r')
    if DEBUG:
        print(dr.groups)

    if format == "NETCDF4" or format == "CFA4":
        grp = dr.groups["test_group"]
    else:
        grp = dr

    if DEBUG:
        print(grp.variables["tmp"])
        print(dr.variables["scl"])

    tmp_var = grp.variables["tmp"]
    x = tmp_var[:,0,0,0]
    dr.close()
    return True

class s3DatasetTest(unittest.TestCase):
    # static class members
    # all path stubs the same
    path_stub = os.environ["HOME"] + "/Test/s3Dataset_test"
    res_deg = 2.5

    #
    def test_NETCDF4_CFA0_4(self):
        self.assertTrue(
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "NETCDF4", "0.4", s3DatasetTest.res_deg
            )
        )
        self.assertTrue(
            test_s3Dataset_read(s3DatasetTest.path_stub, "NETCDF4", "0.4")
        )

    def test_NETCDF4_CFA0_5(self):
        self.assertTrue(
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "NETCDF4", "0.5", s3DatasetTest.res_deg
            )
        )
        self.assertTrue(
            test_s3Dataset_read(s3DatasetTest.path_stub, "NETCDF4", "0.5")
        )

    def test_NETCDF3_CFA0_4(self):
        self.assertTrue(
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "NETCDF3_CLASSIC", "0.4", s3DatasetTest.res_deg
            )
        )
        self.assertTrue(
            test_s3Dataset_read(s3DatasetTest.path_stub, "NETCDF3_CLASSIC", "0.4")
        )

    def test_NETCDF3_CFA0_5(self):
        with self.assertRaises(APIException):
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "NETCDF3_CLASSIC", "0.5", s3DatasetTest.res_deg
            )

    def test_CFA4_CFA0_4(self):
        self.assertTrue(
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "CFA4", "0.4", s3DatasetTest.res_deg
            )
        )
        self.assertTrue(
            test_s3Dataset_read(s3DatasetTest.path_stub, "CFA4", "0.4")
        )

    def test_CFA4_CFA0_5(self):
        self.assertTrue(
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "CFA4", "0.5", s3DatasetTest.res_deg
            )
        )
        self.assertTrue(
            test_s3Dataset_read(s3DatasetTest.path_stub, "CFA4", "0.5")
        )

    def test_CFA3_CFA0_4(self):
        self.assertTrue(
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "CFA3", "0.4", s3DatasetTest.res_deg
            )
        )
        self.assertTrue(
            test_s3Dataset_read(s3DatasetTest.path_stub, "CFA3", "0.4")
        )

    def test_CFA3_CFA0_5(self):
        with self.assertRaises(APIException):
            test_s3Dataset_write(
                s3DatasetTest.path_stub, "CFA3", "0.5", s3DatasetTest.res_deg
            )

if __name__ == '__main__':
    unittest.main()
