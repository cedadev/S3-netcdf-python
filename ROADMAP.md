Roadmap for improvements to s3netCDF-python
===========================================

1. Improve documentation, provide more examples and tutorials
2. Add support for unequal partition sizes (completed in v2.0.5)
3. Add support for striding in slices e.g. [1:20:2]
4. Add support for streaming files greater than memory to disk / cache
5. Make more use of Cython features - add types for all variables in .pyx files
6. More unit tests and continuous integration
7. Add Compatibility with xarray and Zarr: read and write xarray / Zarr files,
i.e. the master array file is an xarray JSON attributes file, and provide
support for Zarr with a CFA master-array file, i.e. the chunks are Zarr but the
master-array file is CFA-netCDF.
8. Upgrade aiobotocore to latest.  v1.0+ has an API that breaks previous
version. (completed in v2.0.5)
9. Add Dask support for parallel workflows.
