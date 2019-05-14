# from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
import netCDF4
from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset

# test file
x = s3Dataset("/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1901.1910.tmp.dat.nc")
x.close()
# print(x.variables)
# print(x.variables['tmp'])

# test S3
x = s3Dataset("s3://cedadev-o/buckettest/cru_ts3.24.01.2011.2015.tmp.dat.nca")
x.close()
#print(x.variables)
#print(x.variables['tmp'])

y = s3Dataset("s3://cedadev-o/buckettest/netCDF4_test.nc", format="NETCDF4", mode='w')
y.close()

z = s3Dataset("/Users/dhk63261/Archive/netCDF4_test_local.nc", format="NETCDF4", mode='w')
z.close()
