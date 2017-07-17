s3-netCDF4-python
=================

An extension module to netCDF4-python to enable reading and writing netCDF files and
CFA-netcdf files from / to object stores which have an S3 HTTP interface.

Current version as of 20170717
==============================

Reads a netCDF file from an object store either directly into memory, for a small file, or
to a cache in the user file space if the file is large.