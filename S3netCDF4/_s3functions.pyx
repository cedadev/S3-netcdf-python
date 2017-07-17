"""
Functions to support S3 enabled version of netCDF4.

Author: Neil Massey
Date:   10/07/2017
"""

# using the minio client API as the interface to the S3 HTTP API
from minio import Minio
from psutil import virtual_memory
import json
import os

from _s3Exceptions import *

def s3_create_client(host_name, s3_user_config):
    # get the url, access key and secret key from the host data
    try:
        host_config = s3_user_config["hosts"][host_name]
        url_name = host_config['url']
        secret_key = host_config['secretKey']
        access_key = host_config['accessKey']
    except:
        raise s3IOException("Error in config file " + s3_user_config["filename"])

    # attach the client to the object store and get the object
    try:
        s3_client = Minio(url_name, access_key=access_key, secret_key=secret_key, secure=False)
    except BaseException:
        raise s3IOException("Error: " + url_name + " not found.")
    return s3_client



def s3_read_user_config():
    """
       Read the JSON config file for cfs3 from the user home directory.
       Config file is called: .cfs3.json
    """

    # get the config file name
    # get user home directory
    user_home = os.environ["HOME"]

    # add the config file name
    s3_user_config_filename = user_home + "/" + ".s3nc4.json"

    # open the file
    try:
        fp = open(s3_user_config_filename)

        # deserialize from the JSON
        s3_user_config = json.load(fp)

        # add the filename to the config so we can refer to it in error messages
        s3_user_config["filename"] = s3_user_config_filename

        # close the file and return
        fp.close()
    except IOError:
       raise s3IOException("User config file does not exist with path: " + s3_user_config_filename)
    return s3_user_config



def s3_map_endpoint_to_host_bucket_object(filename, s3_user_config):
    """
       Map the s3 endpoint (expressed as s3://) to a url (http://) using the configuration
       read in via s3_read_user_config
    """
    # get the s3 endpoint first
    split_ep = filename.split("/")
    s3_ep = "s3://" + split_ep[2]
    bucket_name = split_ep[3]

    # search through the "aliases" to find the http url for the s3 endpoint
    host_name = None
    try:
        hosts = s3_user_config["hosts"]
        for h in hosts:
            if s3_ep in hosts[h]['alias']:
                host_name = h
    except:
        raise s3IOException("Error in config file " + s3_user_config["filename"])

    # check whether the url was found in the config file
    if host_name == None:
       raise s3IOException(s3_ep + " was not found as an alias in the user config file " + s3_user_config["filename"])

    # set the object (prefix + object name)
    object_name = "/".join(split_ep[4:])
    return host_name, bucket_name, object_name



def s3_get_netCDF_filetype(host_name, bucket_name, object_name, s3_client, s3_user_config):
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
    alias = s3_user_config["hosts"][host_name]["alias"]
    full_url = alias + "/" + bucket_name + "/" + object_name
    try:
        s3_object = s3_client.get_partial_object(bucket_name, object_name, 0, 4)
    except BaseException:
        raise s3IOException(full_url + " not found")

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



def s3_should_stream_to_cache(host_name, bucket_name, object_name, s3_client, s3_user_config, stream_to_cache=False):
    """
       Determine whether the object should be streamed to a file, or into memory.
       The criteria are as follows:
         1. User specifies to stream to a file
         2. The object is bigger than the user_config["max_file_size_for_memory"]
         3. The object is larger than the amount of free RAM
       :return: boolean (True | False)
    """
    stream_to_file = False

    # This nested if..else.. is to improve performance - don't make any calls
    #  to object store or system processes unless we really have to.

    # First check - does the user want to stream to the file
    if stream_to_cache:
        stream_to_file = True
    else:
        # full url for error messages
        alias = s3_user_config["hosts"][host_name]["alias"]
        full_url = alias + "/" + bucket_name + "/" + object_name
        # get the size of the object
        try:
            object_stats = s3_client.stat_object(bucket_name, object_name)
        except BaseException:
            raise s3IOException("Error: " + full_url + " not found.")

        # check whether the size is greater than the user_config setting
        if object_stats.size > s3_user_config["max_file_size_for_memory"]:
            stream_to_file = True
        else:
            # check whether the size is greater than the free memory
            mem = virtual_memory()
            if object_stats.size > mem.available:
                stream_to_file = True

    return stream_to_file



def s3_stream_to_cache(host_name, bucket_name, object_name, s3_client, s3_user_config):
    """
       Download the object to the cache as a file.
       If it already exists then check whether the object on the object store is newer than the file on the disk.
       :return: string filename in the cache
    """
    # full url for error messages
    alias = s3_user_config["hosts"][host_name]["alias"]
    full_url = alias + "/" + bucket_name + "/" + object_name

    # Create the destination filepath
    dest_path = s3_user_config["cache_location"] + "/" + bucket_name + "/" + object_name

    # First check whether the file exists
    if os.path.exists(dest_path):
        # get the date of the object on the object store
        # these exceptions shouldn't really happen but I'm writing particularly defensive code!
        try:
            object_stats = s3_client.stat_object(bucket_name, object_name)
        except BaseException:
            raise s3IOException("Error: " + full_url + " not found.")
        # get the date of the corresponding file on the file system
        try:
            file_stats = os.stat(dest_path)
        except BaseException:
            raise IOError("Error: " + dest_path + " not found.")

        # the object on the object store is newer than the one on the disk
        if object_stats.last_modified > file_stats.st_mtime:
            # Redownload the file
            try:
                s3_client.fget_object(bucket_name, object_name, dest_path)
            except BaseException:
                raise s3IOException("Error: " + full_url + " not found.")
    else:
        # Does not exist so we have to download the file
        # first create the destination directory, if it doesn't exist
        dest_dir = os.path.dirname(dest_path)
        if not os.path.exists(dest_dir):
            os.makedirs(os.path.dirname(dest_dir))
        # now try downloading the file
        try:
            s3_client.fget_object(bucket_name, object_name, dest_path)
        except BaseException:
            raise s3IOException("Error: " + full_url + " not found.")

    return dest_path



def s3_stream_to_memory(host_name, bucket_name, object_name, s3_client, s3_user_config):
    """
       Download the object to some memory.
       :return: memory buffer containing the bytes of the netCDF file
    """
    # full url for error messages
    alias = s3_user_config["hosts"][host_name]["alias"]
    full_url = alias + "/" + bucket_name + "/" + object_name

    try:
        s3_object = s3_client.get_object(bucket_name, object_name)
    except BaseException:
        raise s3IOException("Error: " + full_url + " not found.")
    # stream the data
    return s3_object.data

