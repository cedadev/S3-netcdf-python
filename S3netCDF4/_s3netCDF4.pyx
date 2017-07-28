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
from _s3functions import *
from _s3Exceptions import *
import os

# these are class attributes that only exist at the python level (not in the netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 's3_user_config', 's3_client',
 's3_host_name', 's3_object_name', 's3_bucket_name',
 's3_stream_to_cache', 's3_delete_from_cache', 's3_cache_filename'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset, object):
    """
    Inherit the UniData netCDF4 Dataset class and override some key member functions to allow the
    read and write of netCDF file to an object store accessed via an AWS S3 HTTP API.
    """

    def __init__(self, filename, mode='r', clobber=True, format='NETCDF4',
                 diskless=False, persist=False, keepweakref=False, memory=None,
                 stream_to_cache=False, delete_from_cache=False, **kwargs):
        """
        **`__init__(self, filename, mode="r", clobber=True, diskless=False,
           persist=False, keepweakref=False, format='NETCDF4')`**

        `S3netCDF4.Dataset` constructor
        See `netCDF4.Dataset` for full details of all the keywords
        """

        # check whether this is an S3 Dataset - identified via "s3://" being in the filename
        if "s3://" in filename:
            # it's an S3 Dataset
            # First load in the configuration for the S3 server(s)
            self.s3_user_config = s3_read_user_config()
            # get the url, bucket and object from the endpoint
            self.s3_host_name, self.s3_bucket_name, self.s3_object_name = s3_map_endpoint_to_host_bucket_object(filename, self.s3_user_config)

            # full url for error messages
            alias = self.s3_user_config["hosts"][self.s3_host_name]["alias"]
            full_url = alias + "/" + self.s3_bucket_name + "/" + self.s3_object_name
            # create the client
            self.s3_client = s3_create_client(self.s3_host_name, self.s3_user_config)
            # Switch on file mode, read, write or append.  Unbuffered sharing is not permitted
            if mode == 'r':             # read
                # Check whether the object exists
                try:
                    object_stats = self.s3_client.stat_object(self.s3_bucket_name, self.s3_object_name)
                except BaseException:
                    raise s3IOException("Error: " + full_url + " not found.")

                # check whether this object is a netCDF file
                file_type, file_version = s3_get_netCDF_filetype(self.s3_host_name, self.s3_bucket_name, self.s3_object_name,
                                                                 self.s3_client, self.s3_user_config)
                if file_type == "NOT_NETCDF" or file_version == 0:
                    raise s3IOException("Error: " + full_url + " is not a netCDF file.")

                # store whether we should always stream to file
                self.s3_stream_to_cache = stream_to_cache
                self.s3_delete_from_cache = delete_from_cache
                # check whether we should stream this object
                if s3_should_stream_to_cache(self.s3_host_name, self.s3_bucket_name, self.s3_object_name,
                                             self.s3_client, self.s3_user_config, self.s3_stream_to_cache):
                    # stream the file to the cache
                    self.s3_cache_filename = s3_stream_to_cache(self.s3_host_name, self.s3_bucket_name, self.s3_object_name,
                                                                self.s3_client, self.s3_user_config)
                    # create the netCDF4 dataset with the cached file
                    netCDF4.Dataset.__init__(self, self.s3_cache_filename, mode=mode, clobber=clobber, format=format,
                                             diskless=diskless, persist=persist, keepweakref=keepweakref, memory=None,
                                             **kwargs)
                else:
                    # set the cache filename to the empty string
                    self.s3_cache_filename = ""
                    # the netCDF library needs the filepath even for files created from memory
                    filepath = self.s3_user_config["cache_location"]
                    # get the data from the object
                    data_from_object = s3_stream_to_memory(self.s3_host_name, self.s3_bucket_name, self.s3_object_name,
                                                           self.s3_client, self.s3_user_config)
                    # create a temporary file in the cache location with the same filetype
                    temp_file_name = filepath + "/" + file_type + "_dummy.nc"
                    # check it exists before creating it
                    if not os.path.exists(temp_file_name):
                        temp_file = netCDF4.Dataset(temp_file_name, 'w', format=file_type).close()
                    # create the netCDF4 dataset from the data, using the temp_file
                    netCDF4.Dataset.__init__(self, temp_file_name, mode=mode, clobber=clobber, format=format,
                                             diskless=True, persist=False, keepweakref=keepweakref, memory=data_from_object,
                                             **kwargs)

            elif mode == 'w':           # write
                raise NotImplementedError

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


    def close(self):
        """
        Close the S3 file
        """
        # close the netCDF4 file first
        netCDF4.Dataset.close(self)

        # delete the file from the cache if necessary
        if self.s3_cache_filename != "" and self.s3_delete_from_cache:
            # delete the file
            os.remove(self.s3_cache_filename)
            # check whether the directory is empty and remove it if it is
            dest_dir = os.path.dirname(self.s3_cache_filename)
            # this recursively cleans up the directories
            while dest_dir:
                if not os.listdir(dest_dir):
                    os.rmdir(dest_dir)
                    dest_dir = os.path.dirname(dest_dir)
                else:
                    dest_dir = None