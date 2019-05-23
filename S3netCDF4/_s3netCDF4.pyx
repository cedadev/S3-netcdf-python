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
import asyncio
import inspect
import types

# This module inherits from the standard UniData netCDF4 implementation
# import as netCDF4 to avoid confusion with the S3netCDF4 module
import netCDF4._netCDF4 as netCDF4

from _Exceptions import *
from CFA._CFAClasses import *
from CFA._CFAFunctions import *
from CFA._CFAParsers import *
from _s3netCDFIO import _interpret_netCDF_filetype
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
 '_file_man', '_file_handle', '_cfa_metadata', '_mode', '_event_loop',
 '_async_system', '_remote_system'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member
       functions to allow the read and write of netCDF files and CFA formatted
       netCDF files to an object store accessed via an AWS S3 HTTP API.
    """

    def __init__(self, filename, mode='r', clobber=True, format='DEFAULT',
                 diskless=False, persist=False, keepweakref=False, memory=None,
                 **kwargs):
        """
        **`__init__(self, filename, mode="r", clobber=True, diskless=False,
           persist=False, keepweakref=False, format='NETCDF4')`**

        `S3netCDF4.Dataset` constructor
        See `netCDF4.Dataset` for full details of all the keywords
        """
        self._init_(filename, mode=mode, clobber=True, format='DEFAULT',
                     diskless=False, persist=False, keepweakref=False,
                     memory=None, **kwargs)

    async def _seek_zero_read(self, size=-1):
        """Async function to seek to zero then read a specified number of bytes.
        """
        # seek zero
        await self._file_handle.seek(0)
        # read
        data = await self._file_handle.read(size=size)
        await self._file_handle.seek(0)
        return data

    def _init_(self, filename, mode='r', clobber=True, format='DEFAULT',
               diskless=False, persist=False, keepweakref=False, memory=None,
               **kwargs):
        """Duplication of the __init__ method, so that it can be used with
        asyncio.  Python reserved methods cannot be declared as `async`
        """
        # Create a file manager object and keep it
        self._file_man = FileManager()
        self._mode = mode

        # open the file and record the file handle - make sure we open it in
        # binary mode
        if 'b' not in mode:
            fh_mode = mode + 'b'
        self._file_handle = self._file_man.open(filename, mode=fh_mode)
        # see if it's a remote dataset - if the file handle has a connect method
        try:
            if inspect.ismethod(self._file_handle.connect):
                self._remote_system = True
            else:
                self._remote_system = False
        except:
            self._remote_system = False

        try:
            # This all looks very bizarre - but it due to us using Cython,
            # rather than CPython and asyncio coroutines.
            # In CPython each coroutine function has 128 added to the code type
            # bit mask, whereas in Cython, 128 is not present in the bit mask.
            # This means that inspect.iscoroutine() fails to acknowledge that
            # a Cython compiled coroutine function is a coroutine function!!!
            # This workaround seems quite elegant, but relies on instantiating
            # the connection before it is optimum
            connection = self._file_handle.connect()
            if "coroutine" in str(connection):
                self._async_system = True
                self._event_loop = asyncio.get_event_loop()
                # create a task to be scheduled to connect to the remote server
                connect_task = self._event_loop.create_task(
                    connection
                )
            else:
                self._async_system = False
        except:
            self._async_system = False

        if (self._remote_system):
            # the async version will have connected in the code block above
            if not self._async_system:
                self._file_handle.connect()

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
            if self._remote_system:
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
            if async_system:
                # wait for the connect task to complete
                self._event_loop.run_until_complete(connect_task)

                # schedule a task to read the first 6 bytes of the stream and
                # wait for it to complete, then get the result
                get_filetype_task = self._event_loop.create_task(
                    self._seek_zero_read(6)
                )
                self._event_loop.run_until_complete(get_filetype_task)
                data = get_filetype_task.result()

            else:
                self._file_handle.seek(0)
                data = self._file_handle.read(6)
                # seek back to 0 ready for the read below
                self._file_handle.seek(0)

            file_type, file_version = _interpret_netCDF_filetype(data)
            # check what the file type is a netCDF file or not
            if file_type == 'NOT_NETCDF':
                raise IOError("File: {} is not a netCDF file".format(filename))
                # read the file in, or create it
            if self._remote_system:
                nc_bytes = self._event_loop.run_until_complete(
                    self._file_handle.read()
                )
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
            # check if file is a CFA file, for standard netCDF files
            # try:
            #     cfa = "CFA" in self.getncattr("Conventions")
            # except:
            #     cfa = False
            # # parse the cfa
            # if cfa:
            #     self._cfa_metadata = CFA_netCDFParser().read(self)
            # else:
            #     self._cfa_metadata = None
        else:
            # no other modes are supported
            raise APIException("Mode " + mode + " not supported.")

    def close(self):
        """Close the Dataset."""
        # call the base class close method
        nc_bytes = netCDF4.Dataset.close(self)
        # write any in-memory data into the file, if it is a remote file
        if 'w' in self._mode and self._remote_system and nc_bytes is not None:
            if self._async_system:
                self._event_loop.run_until_complete(
                    self._file_handle.write(nc_bytes)
                )
            else:
                self._file_handle.write(nc_bytes)
        # close the file handle
        if self._async_system:
            self._event_loop.run_until_complete(
                self._file_handle.close()
            )
        else:
            self._file_handle.close()

    def _getVariables(self):
        return netCDF4.Dataset.__getattribute__(self, "variables")

    def __getattribute__(self, attrname, *args, **kwargs):
        """Here we override several netCDF4.Dataset functionalities, in order
        to intercept and return CFA functionality instead."""
        if attrname == "variables":
            return self._getVariables()
        return netCDF4.Dataset.__getattribute__(self, attrname)
