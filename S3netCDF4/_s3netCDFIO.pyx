"""
Input / output functions for reading / writing individual netCDF files from / to either S3 object storage
or to a file on a POSIX file system.

Author: Neil Massey
Date:   07/09/2017
"""

from _s3Client import *
from _s3Exceptions import *
from CFA._CFAClasses import *

cdef class s3netCDFFile:
    """
       Class to return details of a netCDF file that may be on a POSIX file system, on S3 storage then
         streamed to a POSIX file cache or streamed from S3 directly into memory.
    """

    cdef public basestring filename
    cdef public basestring s3_uri
    cdef public basestring filemode
    cdef public basestring format
    cdef public basestring memory
    cdef public cfa_file

    def __init__(self, filename = "", s3_uri = "", filemode = 'r', memory = ""):
        """
        :param filename: the original filename on disk (or openDAP URI) or the filename of the cached file - i.e. where
                         the S3 file is streamed (for 'r' and 'a' filemodes) or created (for 'w' filemodes).
                         For memory streamed files this is the tempfile location.
        :param s3_uri: S3 URI for S3 files only
        :param filemode: 'r'ead | 'w'rite | 'a'ppend
        :param memory: the memory where the S3 file is streamed to, or None if filename on disk
        """

        self.filename = filename
        self.s3_uri = s3_uri
        self.filemode = filemode
        self.memory = memory
        self.format = 'NOT_NETCDF'

    def __repr__(self):
        return "s3netCDFFile"

    def __str__(self):
        ret_str = "<s3netCDFFile> (filename='"+ self.filename +\
                                "', s3_uri='" + self.s3_uri +\
                                "', filemode='" + self.filemode
        if self.memory == "":
            ret_str += "', memory=None'"
        else:
            ret_str += "', memory=allocated'"

        if self.cfa_file:
            ret_str += "', cfa_file=present'"

        ret_str += ")"
        return ret_str


def _get_netCDF_filetype(s3_client, bucket_name, object_name):
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
    # open the url/bucket/object as an s3_object and read the first 4 bytes
    try:
        s3_object = s3_client.get_partial(bucket_name, object_name, 0, 4)
    except BaseException:
        raise s3IOException(s3_client.get_full_url(bucket_name, object_name) + " not found")

    # start with NOT_NETCDF as the file_type
    file_version = 0
    file_type = 'NOT_NETCDF'

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


def get_endpoint_bucket_object(filename):
    """Return the endpoint, bucket and object, split out from the filename"""
    # Get the server, bucket and object from the URI: split the URI on "/" separator
    split_ep = filename.split("/")
    # get the s3 endpoint first
    s3_ep = "s3://" + split_ep[2]
    # now get the bucketname
    s3_bucket_name = split_ep[3]
    # finally get the object (prefix + object name) from the remainder of the
    s3_object_name = "/".join(split_ep[4:])

    return s3_ep, s3_bucket_name, s3_object_name


def get_netCDF_file_details(filename, filemode='r', diskless=False, persist=False, s3_client_config=None):
    """
    Get the details of a netCDF file which is either stored in S3 storage or on POSIX disk.
    If the file is on S3 storage, and the filemode is 'r' or 'a' then it will be streamed to either the cache or
      into memory, depending on the filesize and the value of <max_object_size_for_memory> in the .s3nc4.json config file.

    :param filename: filename on POSIX / URI on S3 storage
    :param filemode: 'r'ead | 'w'rite | 'a'ppend
    :return: s3netCDFFile
    """

    # create file_details
    file_details = s3netCDFFile(filemode=filemode)

    # handle S3 file first
    if "s3://" in filename:

        # record the s3_uri - empty string indicates not an s3_uri file
        file_details.s3_uri = filename

        # get the endpoint, bucket name, object name
        s3_ep, s3_bucket_name, s3_object_name = get_endpoint_bucket_object(filename)

        # create the s3 client
        s3_client = s3Client(s3_ep, s3_client_config)
        # get the full url for error messages
        full_url = s3_client.get_full_url(s3_bucket_name, s3_object_name)

        # if the filemode is 'r' or 'a' then we have to stream the file to either the cache or to memory
        if filemode == 'r' or filemode == 'a' or filemode == 'r+':

            # Check whether the object exists
            if not s3_client.object_exists(s3_bucket_name, s3_object_name):
                raise s3IOException("Error: " + full_url + " not found.")

            # check whether this object is a netCDF file
            file_type, file_version = _get_netCDF_filetype(s3_client, s3_bucket_name, s3_object_name)
            if file_type == "NOT_NETCDF" or file_version == 0:
                raise s3IOException("Error: " + full_url + " is not a netCDF file.")

            # retain the filetype
            file_details.format = file_type

            # check whether we should stream this object
            # - use diskless to indicate the file should be read into memory whatever its size
            # - user persist to indicate that the file should be cached whatever its size
            if (s3_client.should_stream_to_cache(s3_bucket_name, s3_object_name) and not diskless) or persist:
                # stream the file to the cache
                file_details.filename = s3_client.stream_to_cache(s3_bucket_name, s3_object_name)
            else:
                # the netCDF library needs to create a dummy file for files created from memory
                # one dummy file can be used for all of the memory streaming
                file_details.filename = s3_client.get_cache_location() + "/" + file_type + "_dummy.nc"
                # get the data from the object
                file_details.memory = s3_client.stream_to_memory(s3_bucket_name, s3_object_name)

        # if the filemode is 'w' then we just have to construct the cache filename and return it
        elif filemode == 'w':
            # get the cache file name
            file_details.filename = s3_client.get_cachefile_path(s3_bucket_name, s3_object_name)

        # the created file in
        else:
            # no other modes are supported
            raise s3APIException("Mode " + filemode + " not supported.")

    # otherwise just return the filename in file_details
    else:
        file_details.filename = filename

    return file_details


def put_netCDF_file(filename):
    """Write the netCDF file to object store if it contains s3://
       Otherwise do nothing (it is already written to disk).
       Assumes that the file is in the cache."""

    # handle S3 file first
    if "s3://" in filename:
        # get the endpoint, bucket name, object name
        s3_ep, s3_bucket_name, s3_object_name = get_endpoint_bucket_object(filename)

        # create the s3 client
        s3_client = s3Client(s3_ep)

        # write the file from the cache to the object store
        s3_client.write(s3_bucket_name, s3_object_name)


def put_CFA_file(filename, max_file_size=-1, format="NETCDF4"):
    """Write the CFA file and its constituent CF-netCDF files to object store if the filename contains s3://
       Otherwise write the CFA file (and CF-netCDF files) directly to disk."""
    if "s3://" in filename:
        # get the endpoint, bucket name, object name
        s3_ep, s3_bucket_name, s3_object_name = get_endpoint_bucket_object(filename)

        # create the s3 client
        s3_client = s3Client(s3_ep)

        # check / create the bucket
        s3_client.create_bucket(s3_bucket_name)

        # get the max_file_size from the config file if no overriding size is set
        if max_file_size == -1:
            max_file_size = s3_client.get_max_object_size()

        # write the cfa master file to the cache
        cache_filename = s3_client.get_cachefile_path(s3_bucket_name, s3_object_name)

        # open the file for reading
        nc_file = netCDF4.Dataset(cache_filename, mode='r')

        # initialize the cfa object and create the master array
        cfa_ma = CFAfile(nc_file, cache_filename, max_file_size, s3_url=filename)

        # write the master array into the cache
        cfa_ma.write(format = format)

        # manipulate the object name
        cfa_object_name = s3_object_name.replace(".nc", ".nca")
        # upload the file
        s3_client.write(s3_bucket_name, cfa_object_name)

        # now write each subfile and upload
        for varnum in range(0, cfa_ma.get_number_of_variables()):
            for sa in range(0, cfa_ma.get_number_of_subarrays(varnum)):
                sa_filename = cfa_ma.write_subarray(varnum, sa, format = format)
                sa_ep, sa_bucket_name, sa_object = get_endpoint_bucket_object(sa_filename)
                s3_client.write(sa_bucket_name, sa_object)
    else:
        # if max_file size is -1 (default) then reset it to 1MB
        if max_file_size == -1:
            max_file_size = 1024 * 1024
        # open the file for reading
        nc_file = netCDF4.Dataset(filename, mode='r')

        # initialize the cfa object and create the master array
        cfa_ma = CFAfile(nc_file, filename, max_file_size)

        # write the master array, set the base to be the filename(-.nc in write function)
        base_name = os.path.dirname(filename)
        cfa_ma.write(base_path = base_name, format = format)

        # now write each subfile
        for varnum in range(0, cfa_ma.get_number_of_variables()):
            for sa in range(0, cfa_ma.get_number_of_subarrays(varnum)):
                sa_filename = cfa_ma.write_subarray(varnum, sa, format = format)
