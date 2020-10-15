S3netCDF-python

Changes between v2.0.4 and 2.0.5
--------------------------------
1. Added support for reading unequal sized partitions.  These may occur in files written by the s3_nc_cfa_agg.py program.

Changes between v0.2 and v2.0:
------------------------------
1. complete rewrite
2. v0.5 CFA
3. partition matrix represented internally by netCDF Dataset
4. user can supply sub array size when creating variable
5. cacheless operation, except for read of very large files
6. intelligent memory handling
7. excellent sparse-array handling
8. complete compliance with netCDF4 API interface
