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
from _s3WriteCFA import *
import os

# these are class attributes that only exist at the python level (not in the netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 '_s3_file', '_file_mode', '_cfa_file',
 '_s3_user_config', '_s3_client',
 '_s3_delete_from_cache', '_s3_cache_filename'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset, object):
    """
    Inherit the UniData netCDF4 Dataset class and override some key member functions to allow the
    read and write of netCDF file to an object store accessed via an AWS S3 HTTP API.
    """

    def __init__(self, filename, mode='r', clobber=True, format='DEFAULT',
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
            self._s3_file = True

            # create the s3 client
            self._s3_client = s3Client(filename)

            # we have different defaults for the read and write - as the read automatically handles CFA files
            # from the netCDF attribute
            if format == 'DEFAULT':
                format = 'NETCDF4'

            # Switch on file mode, read, write or append.  Unbuffered sharing is not permitted.
            # Store the file mode as we need it when we close the file to write to the S3 server
            self._file_mode = mode
            #
            if mode == 'r':             # read
                # Check whether the object exists
                if not self._s3_client.object_exists():
                    raise s3IOException("Error: " + self._s3_client.get_full_url() + " not found.")

                # check whether this object is a netCDF file
                file_type, file_version = self._get_netCDF_filetype()
                if file_type == "NOT_NETCDF" or file_version == 0:
                    raise s3IOException("Error: " + self._s3_client.get_full_url() + " is not a netCDF file.")

                # store whether we should delete from the cache when we're done with the file
                self._s3_delete_from_cache = delete_from_cache

                # check whether we should stream this object - use diskless to indicate the file should be read into
                # memory - overriden if the file is larger than max_file_size_for_memory
                if self._s3_client.should_stream_to_cache():
                    # stream the file to the cache
                    self._s3_cache_filename = self._s3_client.stream_to_cache()
                    # create the netCDF4 dataset with the cached file
                    netCDF4.Dataset.__init__(self, self._s3_cache_filename, mode=mode, clobber=clobber, format=format,
                                             diskless=False, persist=persist, keepweakref=keepweakref, memory=None,
                                             **kwargs)
                else:
                    # set the cache filename to the empty string
                    self._s3_cache_filename = ""
                    # the netCDF library needs the filepath even for files created from memory
                    filepath = self._s3_client.get_cache_location()
                    # get the data from the object
                    data_from_object = self._s3_client.stream_to_memory()
                    # create a temporary file in the cache location with the same filetype
                    temp_file_name = filepath + "/" + file_type + "_dummy.nc"
                    # check it exists before creating it
                    if not os.path.exists(temp_file_name):
                        temp_file = netCDF4.Dataset(temp_file_name, 'w', format=file_type).close()
                    # create the netCDF4 dataset from the data, using the temp_file
                    netCDF4.Dataset.__init__(self, temp_file_name, mode=mode, clobber=clobber, format=format,
                                             diskless=True, persist=False, keepweakref=keepweakref, memory=data_from_object,
                                             **kwargs)
                # check if file is a CFA file, for standard netCDF files
                try:
                    self._cfa_file = "CFA" in self.__getattribute__("Conventions")
                except:
                    self._cfa_file = False

            elif mode == 'w':           # write
                # check the format for writing - allow CFA4 in arguments and default to it as well
                # we have different defaults for read and write - write defaults to distributed files across objects
                if format == 'DEFAULT':
                    format = 'CFA4'

                if format == 'CFA4':
                    format = 'NETCDF4'
                    self._cfa_file = True
                elif format == 'CFA3':
                    format = 'NETCDF3'
                    self._cfa_file = True
                else:
                    self._cfa_file = False

                # determine the filename: take the last part of the filename and add the cache directory
                self._s3_cache_filename = self._s3_client.get_cache_location() + "/" + filename.split("/")[-1]
                # always remove from cache
                self._s3_delete_from_cache = True
                # if diskless then write to file so we can upload that file to S3
                if diskless:
                    persist = True
                    clobber = True

                # Instantiate the base class - diskless if a cfa file
                if self._cfa_file:
                    diskless = True
                netCDF4.Dataset.__init__(self, self._s3_cache_filename, mode=mode, clobber=clobber, format=format,
                                         diskless=diskless, persist=persist, keepweakref=keepweakref, memory=None,
                                         **kwargs)

            elif mode == 'a' or mode == 'r+':   # append
                raise NotImplementedError

            else:
                # no other modes are supported
                raise s3APIException("Mode " + mode + " not supported.")
        else:
            # it's not an S3 Dataset so just call the base class with all the arguments
            self._s3_file = False
            self._cfa_file = False
            netCDF4.Dataset.__init__(self, filename, mode, clobber, format, diskless,
                                     persist, keepweakref, memory, **kwargs)


    def __enter__(self):
        """Allows objects to be used with a `with` statement."""
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """Allows objects to be used with a `with` statement."""
        self.close()


    def _get_netCDF_filetype(self):
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
            s3_object = self._s3_client.get_partial(0, 4)
        except BaseException:
            raise s3IOException(self._s3_client.get_full_url() + " not found")

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

        if self._s3_file:

            # if the filemode is write then upload to S3 storage
            if self._file_mode == 'w':
                # check / create the bucket
                self._s3_client.create_bucket()

                # is it a cfa (multi file) file?
                if self._cfa_file:
                    # its a cfa_file so initialise and write the MasterArray
                    cfa_ma = CFAfile(self, self._s3_cache_filename, self._s3_client.get_max_object_size())

                    # get the base_path - this is the full url without the final (file) part
                    full_url = self._s3_client.get_full_url()
                    s3_object = full_url.split("/")[-1]
                    s3_base_uri = full_url[:-len(s3_object)]

                    # write the cfa master file
                    cfa_filename = cfa_ma.write(s3_base_uri)
                    s3_object_uri = s3_base_uri + cfa_filename

                    # upload the cfa master file to s3 storage
                    self._s3_client.write_object(s3_object_uri, cfa_filename)

                    # now write the subfiles and upload
                    for varnum in range(0, cfa_ma.get_number_of_variables()):
                        for sa in range(0, cfa_ma.get_number_of_subarrays(varnum)):
                            sa_filename = cfa_ma.write_subarray(varnum, sa)
                            s3_sa_uri = s3_base_uri + sa_filename
                            self._s3_client.write_object(s3_sa_uri, sa_filename)
                else:
                    # it's not a cfa file so just write the whole thing to the S3 storage
                    self._s3_client.write(self._s3_cache_filename)

            # close the s3 client to call the cache management
            self._s3_client.close()

        # now close the netCDF4 file first for all modes
        netCDF4.Dataset.close(self)
