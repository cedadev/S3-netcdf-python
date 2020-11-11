S3netCDF-python

Changes between v2.0.5 and v2.0.6
---------------------------------
1. Update the s3_nc_cfa_agg.py program so that it is compatible with more models and Datasets in CMIP6.  This relates mostly to the way the time dimension is recorded, and the calendar type.
2. Changed the way that the indexing for unequal sized partitions is calculated.  It is now (potentially) slower, but more robust.

Changes between v2.0.4 and v2.0.5
---------------------------------
1. Added support for reading unequal sized partitions.  These may occur in files written by the s3_nc_cfa_agg.py program.

Changes between v2.0.3 and v2.0.4
---------------------------------
1. s3nc_cfa_agg.py now uses FileManager.request_file rather than FileManager._open.  More elegant and API focused.
2. FileManager.request_file is now compatible with passing globs into it as the filename parameter.

Changes between v2.0.2 and v2.0.3
---------------------------------
1. Fixed a problem where a BytesIO buffer was being passed by reference rather than copied, leading to a "file operation on unopened file" error.
2. Corrected install procedure in README.
3. Corrected bug in test_s3Dataset_read.

Changes between v2.0.1 and v2.0.2
---------------------------------
1. Fixed unreleased file for Datasets on disk
2. Fixed incorrect parsing for CFA 0.4

Changes between v0.2 and v2.0.1:
--------------------------------
1. complete rewrite
2. v0.5 CFA
3. partition matrix represented internally by netCDF Dataset
4. user can supply sub array size when creating variable
5. cacheless operation, except for read of very large files
6. intelligent memory handling
7. excellent sparse-array handling
8. complete compliance with netCDF4 API interface
