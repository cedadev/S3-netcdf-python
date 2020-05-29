# S3-netcdf-python Makefile
# Simple makefile for compiling the Cython externals when developing

# Setup.py will build these externals once on installation, so it is not
# necessary to run this Makefile on installation for a user.
# This Makefile only needs to be used when developing.

all:
	python setup.py build_ext --inplace

clean:
	rm -f *.so *.c
	rm -f ./S3netCDF4/Backends/*.so ./S3netCDF4/Backends/*.c
	rm -f ./S3netCDF4/CFA/Parsers/*.so ./S3netCDF4/CFA/Parsers/*.c
	rm -f ./S3netCDF4/CFA/*.so ./S3netCDF4/CFA/*.c
	rm -f ./S3netCDF4/Managers/*.so ./S3netCDF4/Managers/*.c
	rm -f ./S3netCDF4/*.so ./S3netCDF4/*.c
