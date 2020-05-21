all:
	python setup.py build_ext --inplace
    
clean:
	rm -f *.so *.c
	rm -f ./S3netCDF4/Backends/*.so ./S3netCDF4/Backends/*.c
	rm -f ./S3netCDF4/CFA/Parsers/*.so ./S3netCDF4/CFA/Parsers/*.c
	rm -f ./S3netCDF4/CFA/*.so ./S3netCDF4/CFA/*.c
	rm -f ./S3netCDF4/Managers/*.so ./S3netCDF4/Managers/*.c
	rm -f ./S3netCDF4/*.so ./S3netCDF4/*.c
