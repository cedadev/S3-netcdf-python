# s3-netCDF-python Tutorial 1
# Purpose: Read a time series from a CMIP6 dataset
# Author : Neil Massey
# Date   : 12/05/2020

from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
import numpy as np

# Dataset (Master Array File) location, this is on the Caringo object store,
# using the alias defined in the config file in the user's
# home directory: ~/.s3nc.json
#data_location = "s3://cedadev-o/cmip6/CMIP/MOHC/HadGEM3-GC31-MM/historical/r1i1p1f3/day/tas/gn/tas_day_HadGEM3-GC31-MM_historical_r1i1p1f3_gn.nc"
data_location = "/Users/dhk63261/Test/s3Dataset_test_dev_CFA4_cfa0.5.nc"
var_name = "tas"

# We open the Master Array File just like opening a netCDF Dataset
s3_ds = Dataset(data_location, 'r')

# We can inspect the dataset by printing it, just like in netcdf4-python
print("CFA DATASET: ", s3_ds)

# We can also examine the variables in the Dataset
print("VARIABLES: ", s3_ds.variables)

# and the groups in the Dataset
print("GROUPS:    ", s3_ds.groups)

# We can then get a variable from the Dataset
var = s3_ds.variables[var_name]
# and inspect it
print("TAS:       ", var)

# we can get a timeseries for one year timeseries by slicing the variable:
# this will return a numpy array
var_data = var[:360, 45, 45]

# calculate seasonal means
print(np.mean(var_data[0:90]), np.mean(var_data[90:180]), np.mean(var_data[180:240]), np.mean(var_data[240:360]))
