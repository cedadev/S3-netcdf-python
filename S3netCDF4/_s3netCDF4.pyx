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
from _s3Client import *
from _s3Exceptions import *
import os

# these are class attributes that only exist at the python level (not in the netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 'file_mode', 'cfa_file',
 's3_user_config', 's3_client',
 's3_host_name', 's3_object_name', 's3_bucket_name',
 's3_delete_from_cache', 's3_cache_filename'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset, object):
    """
    Inherit the UniData netCDF4 Dataset class and override some key member functions to allow the
    read and write of netCDF file to an object store accessed via an AWS S3 HTTP API.
    """

    def __init__(self, filename, mode='r', clobber=True, format='NETCDF4',
                 diskless=False, persist=False, keepweakref=False, memory=None,
                 delete_from_cache=False, **kwargs):
        """
        **`__init__(self, filename, mode="r", clobber=True, diskless=False,
           persist=False, keepweakref=False, format='NETCDF4')`**

        `S3netCDF4.Dataset` constructor
        See `netCDF4.Dataset` for full details of all the keywords
        """

        # check whether this is an S3 Dataset - identified via "s3://" being in the filename
        if "s3://" in filename:
            # it's an S3 Dataset

            # create the s3 client
            self.s3_client = s3Client(filename)

            # Switch on file mode, read, write or append.  Unbuffered sharing is not permitted.
            # Store the file mode we need it when we close the file to write to the S3 server
            self.file_mode = mode
            #
            if mode == 'r':             # read
                # Check whether the object exists
                if not self.s3_client.object_exists():
                    raise s3IOException("Error: " + self.s3_client.get_full_url() + " not found.")

                # check whether this object is a netCDF file
                file_type, file_version = self.get_netCDF_filetype()
                if file_type == "NOT_NETCDF" or file_version == 0:
                    raise s3IOException("Error: " + self.s3_client.get_full_url() + " is not a netCDF file.")

                # store whether we should delete from the cache when we're done with the file
                self.s3_delete_from_cache = delete_from_cache

                # check whether we should stream this object - use diskless to indicate the file should be read into
                # memory - overriden if the file is larger than max_file_size_for_memory
                if self.s3_client.should_stream_to_cache():
                    # stream the file to the cache
                    self.s3_cache_filename = self.s3_client.stream_to_cache()
                    # create the netCDF4 dataset with the cached file
                    netCDF4.Dataset.__init__(self, self.s3_cache_filename, mode=mode, clobber=clobber, format=format,
                                             diskless=False, persist=persist, keepweakref=keepweakref, memory=None,
                                             **kwargs)
                else:
                    # set the cache filename to the empty string
                    self.s3_cache_filename = ""
                    # the netCDF library needs the filepath even for files created from memory
                    filepath = self.s3_client.get_cache_location()
                    # get the data from the object
                    data_from_object = self.s3_client.stream_to_memory()
                    # create a temporary file in the cache location with the same filetype
                    temp_file_name = filepath + "/" + file_type + "_dummy.nc"
                    # check it exists before creating it
                    if not os.path.exists(temp_file_name):
                        temp_file = netCDF4.Dataset(temp_file_name, 'w', format=file_type).close()
                    # create the netCDF4 dataset from the data, using the temp_file
                    netCDF4.Dataset.__init__(self, temp_file_name, mode=mode, clobber=clobber, format=format,
                                             diskless=True, persist=False, keepweakref=keepweakref, memory=data_from_object,
                                             **kwargs)
                # check if file is a CFA file
                try:
                    self.cfa_file = "CFA" in self.__getattribute__("Conventions")
                except:
                    self.cfa_file = False

            elif mode == 'w':           # write
                # determine the filename: take the last part of the filename and add the cache directory
                self.s3_cache_filename = self.s3_client.get_cache_location() + "/" + filename.split("/")[-1]
                # always remove from cache
                self.s3_delete_from_cache = True
                # if diskless then write to file so we can upload that file to S3
                if diskless:
                    persist = True
                    clobber = True

                # Instantiate the base class
                netCDF4.Dataset.__init__(self, self.s3_cache_filename, mode=mode, clobber=clobber, format=format,
                                         diskless=diskless, persist=persist, keepweakref=keepweakref, memory=None,
                                         **kwargs)

            elif mode == 'a' or mode == 'r+':   # append
                raise NotImplementedError

            else:
                # no other modes are supported
                raise s3APIException("Mode " + mode + " not supported.")
        else:
            # it's not an S3 Dataset so just call the base class with all the arguments
            netCDF4.Dataset.__init__(self, filename, mode, clobber, format, diskless,
                                     persist, keepweakref, memory, **kwargs)


    def __exit__(self, exc_type, exc_val, exc_tb):
        """Allows objects to be used with a `with` statement."""
        self.close()


    def get_netCDF_filetype(self):
        """
           Read the first four bytes from the stream and interpret the magic number.
           See NC_interpret_magic_number in netcdf-c/libdispatch/dfile.c

           Check that it is a netCDF file before fetching any data and
           determine what type of netCDF file it is so the temporary empty file can
           be created with the same type.

           The possible types are:
           `NETCDF3_CLASSIC`, `NETCDF4`,`NETCDF4_CLASSIC`, `NETCDF3_64BIT_OFFSET` or `NETCDF3_64BIT_DATA
           or
           `NOT_NETCDF` if it is not a netCDF file - raise an exception on that

           :return: string filetype
        """
        # open the url/bucket/object as an s3_object and read the first 4 bytes
        try:
            s3_object = self.s3_client.get_partial(0, 4)
        except BaseException:
            raise s3IOException(self.s3_client.get_full_url() + " not found")

        # start with NOT_NETCDF as the file_type
        file_version = 0

        # check whether it's a netCDF file (how can we tell if it's a NETCDF4_CLASSIC file?
        if s3_object.data[1:5] == 'HDF':
            # netCDF4 (HD5 version)
            file_type = 'NETCDF4'
            file_version = 5
        elif (s3_object.data[0] == '\016' and s3_object.data[1] == '\003' and s3_object.data[2] == '\023' and s3_object.data[3] == '\001'):
            file_type = 'NETCDF4'
            file_version = 4
        elif s3_object.data[0:3] == 'CDF':
            file_version = ord(s3_object.data[3])
            if file_version == 1:
                file_type = 'NETCDF3'
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


    def close(self):
        """
        Close the S3 file
        """
        # close the netCDF4 file first for all modes
        netCDF4.Dataset.close(self)
        # if the filemode is write then upload to S3 storage
        if self.file_mode == 'w':
            # check / create the bucket
            self.s3_client.create_bucket()
            # write to the S3 storage
            self.s3_client.s3_write(self.s3_cache_filename)

        # close the s3 client to call the cache management
        self.s3_client.close()