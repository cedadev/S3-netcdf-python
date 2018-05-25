import sys, os
import time
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import num2date, date2num

from S3netCDF4._s3netCDF4 import s3Dataset as Dataset

S3_NETCDF_PATH = "s3://minio/test-bucket/test1/test2/netcdf_test.nc"

def test_s3_read_dataset():
    with Dataset(S3_NETCDF_PATH, mode='r', persist=True) as s3_data:
        # get a list of the variables
        print s3_data.getVariables()
        # get the temperature variable
        tmp = s3_data.getVariable("tmp")
        print tmp
        # get the first timestep and level of the tmp data
        temp_t0_z0 = tmp[0,0,:,:]
        print np.mean(temp_t0_z0)

if __name__ == "__main__":
    test_s3_read_dataset()
