"""
S3 enabled version of netCDF4.
Allows reading and writing of netCDF files to object stores via AWS S3.

Requirements: minio, psutil, netCDF4, Cython

Author: Neil Massey
Date:   10/07/2017
"""

# This module inherits from the standard netCDF4 implementation
# import as UniData netCDF4 to avoid confusion with the S3 module
import netCDF4._netCDF4 as netCDF4
from _s3netCDFIO import get_netCDF_file_details, put_netCDF_file, put_CFA_file
from _s3Exceptions import *

import os

# these are class attributes that only exist at the python level (not in the netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 '_file_details'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member functions to allow the
       read and write of netCDF file to an object store accessed via an AWS S3 HTTP API.
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

        # we've passed all the details of detecting whether this is an S3 or POSIX file to the function
        # get_netCDFFilename(filename).  Diskless == always_stream

        # get the file details
        self._file_details = get_netCDF_file_details(filename, mode, diskless)

        # switch on the read / write / append mode
        if mode == 'r' or mode == 'a' or mode == 'r+':             # read
            # check whether the memory has been set from get_netCDF_file_details (i.e. the file is streamed to memory)
            if self._file_details.memory != None or diskless:
                # we have to first create the dummy file (name held in file_details.memory) - check it exists before creating it
                if not os.path.exists(self._file_details.filename):
                    temp_file = netCDF4.Dataset(self._file_details.filename, 'w', format=self._file_details.format).close()
                # create the netCDF4 dataset from the data, using the temp_file
                netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=True, persist=False,
                                         keepweakref=keepweakref, memory=self._file_details.memory, **kwargs)
            else:
                # not in memory but has been streamed to disk
                netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=False, persist=persist,
                                         keepweakref=keepweakref, memory=None, **kwargs)

        elif mode == 'w':           # write
            # check the format for writing - allow CFA4 in arguments and default to it as well
            # we DEFAULT to CFA4 for writing to S3 object stores so as to distribute files across objects
            if format == 'CFA4' or format == 'DEFAULT':
                format = 'NETCDF4'
                self._file_details.format = format
                self._file_details.cfa_file = True
            elif format == 'CFA3':
                format = 'NETCDF3_CLASSIC'
                self._file_details.format = format
                self._file_details.cfa_file = True
            else:
                self._file_details.cfa_file = False

            # for writing a file, all we have to do is check that the containing folder in the cache exists
            if self._file_details.filename != "":   # first check that it is not a diskless file
                cache_dir = os.path.dirname(self._file_details.filename)
                # create all the sub folders as well
                if not os.path.isdir(cache_dir):
                    os.makedirs(cache_dir)

            # if the file is diskless and an S3 file then we have to persist so that we can upload the file to S3
            if self._file_details.s3_uri != "" and diskless:
                persist = True

            netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber, format=format,
                                     diskless=diskless, persist=persist, keepweakref=keepweakref, memory=None,
                                     **kwargs)
        else:
            # no other modes are supported
            raise s3APIException("Mode " + mode + " not supported.")


    def __enter__(self):
        """Allows objects to be used with a `with` statement."""
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """Allows objects to be used with a `with` statement."""
        self.close()


    def close(self):
        """Close the netCDF file.  If it is a S3 file and the mode is write then upload to the storage."""
        # close the netCDF file first - needed to finish writing to disk
        netCDF4.Dataset.close(self)
        if (self._file_details.filemode == 'w' or
            self._file_details.filemode == "r+" or
            self._file_details.filemode == 'a'):
            # get the filename - either the s3_uri or the filename
            if self._file_details.s3_uri != "":
                filename = self._file_details.s3_uri
            else:
                filename = self._file_details.filename
            # if it's a CFA file then write out the master CFA file and the sub CF netCDF files
            if self._file_details.cfa_file:
                put_CFA_file(filename, format=self._file_details.format)
            else:
                put_netCDF_file(filename)
