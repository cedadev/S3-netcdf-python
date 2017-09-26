from cf.netcdf.read import read as netcdf_read
z1 = netcdf_read("s3://minio/weather-at-home/data/1314Floods/a_series/hadam3p_eu_a7tz_2013_1_008571189_0/a7tzga.pdl3dec.nca")
geop = z1.select("geopotential_height")[0]
print geop.mean()

z2 = netcdf_read("s3://minio/weather-at-home/data/1314Floods/a_series/hadam3p_eu_a7tz_2013_1_008571189_0/a7tzga.pdl3dec.nc")
print geop.mean()