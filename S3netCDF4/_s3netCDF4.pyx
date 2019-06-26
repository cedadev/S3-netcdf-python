"""
S3 enabled version of netCDF4.
Allows reading and writing of netCDF files to object stores via AWS S3.

Requirements: botocore, psutil, netCDF4, Cython

Author:  Neil Massey
Date:    10/07/2017
Updated: 13/05/2019
"""

# Python library module imports
from psutil import virtual_memory
import types

# This module inherits from the standard UniData netCDF4 implementation
# import as netCDF4 to avoid confusion with the S3netCDF4 module
import netCDF4._netCDF4 as netCDF4

from _Exceptions import *
from CFA._CFAClasses import *
from CFA._CFAFunctions import *
from CFA._CFAParsers import *
from S3netCDF4.Managers._FileManager import FileManager

class s3DatasetIntercept(type):
    """Metaclass that overrides some class attributes in the netCDF.Dataset
    class and remaps them to s3Dataset methods."""
    def __getattribute__(self, attrname):
        return netCDF4.Dataset.__getattribute__(self, attrname)

# these are class attributes that only exist at the python level (not in the
# netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 '_file_manager', '_file_object', '_cfa_metadata', '_mode',
 '_interpret_netCDF_filetype', 'file_object'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member
       functions to allow the read and write of netCDF files and CFA formatted
       netCDF files to an object store accessed via an AWS S3 HTTP API.
    """

    @property
    def file_object(self):
        return self._file_object

    def __init__(self, filename, mode='r', clobber=True, format='DEFAULT',
               diskless=False, persist=False, keepweakref=False, memory=None,
               **kwargs):
        """Duplication of the __init__ method, so that it can be used with
        asyncio.  Python reserved methods cannot be declared as `async`
        """
        # Create a file manager object and keep it
        self._file_manager = FileManager()
        self._mode = mode

        # open the file and record the file handle - make sure we open it in
        # binary mode
        if 'b' not in mode:
            fh_mode = mode + 'b'

        self._file_object = self._file_manager.open(filename, mode=fh_mode)

        # set the file up for write mode
        if mode == 'w':
            # check the format for writing - allow CFA4 in arguments and default
            # to CFA4 for writing so as to distribute files across subarrays
            if format == 'CFA4' or format == 'DEFAULT':
                file_type = 'NETCDF4'
                self._cfa_metadata = CFADataset(
                    name=filename,
                    format='NETCDF4'
                )
            elif format == 'CFA3':
                file_type = 'NETCDF3_CLASSIC'
                self._cfa_metadata = CFADataset(
                    name=filename,
                    format='NETCDF3_CLASSIC'
                )
            else:
                file_type = format
                self._cfa_metadata = None

            if self.file_object.remote_system:
                # call the base constructor
                netCDF4.Dataset.__init__(
                    self, "inmemory.nc", mode=mode, clobber=clobber,
                    format=file_type, diskless=True, persist=persist,
                    keepweakref=keepweakref, memory=1, **kwargs
                )
            else:
                netCDF4.Dataset.__init__(
                    self, filename, mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, **kwargs
                )

        # handle read-only mode
        elif mode == 'r':
            # get the header data
            data = self.file_object.read_from(0, 6)
            file_type, file_version = s3Dataset._interpret_netCDF_filetype(data)
            # check what the file type is a netCDF file or not
            if file_type == 'NOT_NETCDF':
                raise IOError("File: {} is not a netCDF file".format(filename))
                # read the file in, or create it
            if self.file_object.remote_system:
                # stream into memory
                nc_bytes = self.file_object.read()
                # call the base constructor
                netCDF4.Dataset.__init__(
                    self, "inmemory.nc", mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, memory=nc_bytes, **kwargs
                )
            else:
                netCDF4.Dataset.__init__(
                    self, filename, mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, **kwargs
                )
        else:
            # no other modes are supported
            raise APIException("Mode " + mode + " not supported.")

    def close(self):
        """Close the Dataset."""
        # call the base class close method
        nc_bytes = netCDF4.Dataset.close(self)
        self.file_object.close(nc_bytes)

    def _getVariables(self):
        return netCDF4.Dataset.__getattribute__(self, "variables")

    def __getattribute__(self, attrname, *args, **kwargs):
        """Here we override several netCDF4.Dataset functionalities, in order
        to intercept and return CFA functionality instead."""
        if attrname == "variables":
            return self._getVariables()
        return netCDF4.Dataset.__getattribute__(self, attrname)

    def _interpret_netCDF_filetype(data):
        """
           Pass in the first four bytes from the stream and interpret the magic number.
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
        return file_type, file_version
