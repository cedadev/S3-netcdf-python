"""
Input / output functions for reading / writing individual netCDF files from / to either S3 object storage
or to a file on a POSIX file system.

Author: Neil Massey
Date:   07/09/2017
"""

from _Exceptions import *
from CFA._CFAClasses import *

def _get_netCDF_filetype(file_handle):
    """
       Read the first four bytes from the stream and interpret the magic number.
       See NC_interpret_magic_number in netcdf-c/libdispatch/dfile.c

       Check that it is a netCDF file before fetching any data and
       determine what type of netCDF file it is so the temporary empty file can
       be created with the same type.

       The possible types are:
       `NETCDF3_CLASSIC`, `NETCDF4`,`NETCDF4_CLASSIC`, `NETCDF3_64BIT_OFFSET` or `NETCDF3_64BIT_DATA`
       or
       `NOT_NETCDF` if it is not a netCDF file - raise an exception on that

       :return: string filetype
    """
    # use the file handle to seek to 0 and read the first 6 bytes - these are
    # the netCDF identifier header
    file_handle.seek(0)
    data = file_handle.read(6)

    # start with NOT_NETCDF as the file_type
    file_version = 0
    file_type = 'NOT_NETCDF'

    # check whether it's a netCDF file (how can we tell if it's a
    # NETCDF4_CLASSIC file?
    if data[1:4] == b'HDF':
        # netCDF4 (HD5 version)
        file_type = 'NETCDF4'
        file_version = 5
    elif (data[0] == b'\016' and data[1] == b'\003' and \
          data[2] == b'\023' and data[3] == b'\001'):
        file_type = 'NETCDF4'
        file_version = 4
    elif data[0:3] == b'CDF':
        file_version = data[3]
        if file_version == 1:
            file_type = 'NETCDF3_CLASSIC'
        elif file_version == '2':
            file_type = 'NETCDF3_64BIT_OFFSET'
        elif file_version == '5':
            file_type = 'NETCDF3_64BIT_DATA'
        else:
            file_version = 1 # default to one if no version
    else:
        file_type = 'NOT_NETCDF'
        file_version = 0
    # seek back to 0
    file_handle.seek(0)
    return file_type, file_version
