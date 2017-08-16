"""
Functions to support S3 enabled version of .

Author  : Neil Massey
Date    : 10/07/2017
Modified: 07/08/2017: Rewritten as a class so we don't have to pass so many variables around!
"""

# using the minio client API as the interface to the S3 HTTP API
from minio import Minio
from psutil import virtual_memory
import json
import os

from _s3Exceptions import *

class s3Client(object):

    # abstraction of s3_client operations on netCDF files as a class
    def __init__(self, uri_name):

        # First read the JSON config file for cfs3 from the user home directory.
        # Config file is called: .cfs3.json
        # get user home directory
        user_home = os.environ["HOME"]

        # add the config file name
        s3_user_config_filename = user_home + "/" + ".s3nc4.json"

        # open the file
        try:
            fp = open(s3_user_config_filename)
            # deserialize from the JSON
            self._s3_user_config = json.load(fp)
            # add the filename to the config so we can refer to it in error messages
            self._s3_user_config["filename"] = s3_user_config_filename
            # interpret the config file
            self.interpret_config_file()
            # close the config file
            fp.close()
        except IOError:
            raise s3IOException("User config file does not exist with path: " + s3_user_config_filename)

        # now split the URI on "/" separator
        split_ep = uri_name.split("/")
        # get the s3 endpoint first
        s3_ep = "s3://" + split_ep[2]
        # now get the bucketname
        self._bucket_name = split_ep[3]
        # finally set the object (prefix + object name)
        self._object_name = "/".join(split_ep[4:])

        # search through the "aliases" in the user_config file to find the http url for the s3 endpoint
        self._host_name = None
        try:
            hosts = self._s3_user_config["hosts"]
            for h in hosts:
                if s3_ep in hosts[h]['alias']:
                    self._host_name = h
        except:
            raise s3IOException("Error in config file " + self._s3_user_config["filename"])

        # check whether the url was found in the config file
        if self._host_name == None:
           raise s3IOException(s3_ep + " was not found as an alias in the user config file " + self._s3_user_config["filename"])

        # Now we have the host_name, get the url, access key and secret key from the host data
        try:
            self._host_config = self._s3_user_config["hosts"][self._host_name]
            url_name = self._host_config['url']
            secret_key = self._host_config['secretKey']
            access_key = self._host_config['accessKey']
            self._full_url = self._s3_user_config["hosts"][self._host_name]["alias"] + "/" + self._bucket_name + "/" + self._object_name
        except:
            raise s3IOException("Error in config file " + self._s3_user_config["filename"])

        # attach the client to the object store and get the object
        try:
            self._s3_client = Minio(url_name, access_key=access_key, secret_key=secret_key, secure=False)
        except BaseException:
            raise s3IOException("Error: " + url_name + " not found.")


    def close(self):
        """
           Close the s3 client - primarily to do the cache management
        """
            # # delete the file from the cache if necessary
        # if self.s3_cache_filename != "" and self.s3_delete_from_cache:
        #     # delete the file
        #     os.remove(self.s3_cache_filename)
        #     # check whether the directory is empty and remove it if it is
        #     dest_dir = os.path.dirname(self.s3_cache_filename)
        #     # this recursively cleans up the directories
        #     while dest_dir:
        #         if not os.listdir(dest_dir):
        #             os.rmdir(dest_dir)
        #             dest_dir = os.path.dirname(dest_dir)
        #         else:
        #             dest_dir = None
        pass

    def interpret_config_file(self):
        """
           Transform some of the variables in the config file, especially those file / memory sizes
           that are expressed in human readable form
        """
        # list of keys to convert
        keys_to_convert = ["max_object_size", "max_cache_size", "max_file_size_for_memory"]
        # list of file format sizes
        file_format_sizes = ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        # dictionary mapping to multiplier
        file_format_scale = {"B" : 1,
                             "kB" : 1e3,
                             "MB" : 1e6,
                             "GB" : 1e9,
                             "TB" : 1e12,
                             "EB" : 1e15,
                             "ZB" : 1e18,
                             "YB" : 1e21}

        for key in self._s3_user_config:
            if key in keys_to_convert:
                value = self._s3_user_config[key]
                if value.endswith(file_format_sizes):
                    suffix = value[-2:]
                    size = int(value[:-2])
                elif value[-1] == "B":
                    suffix = "B"
                    size = int(value[:-1])
                else:
                    suffix = "B"
                    size = int(value)
                # multiply by scalar
                size *= file_format_scale[suffix]
                # reassign to s3_user_config
                self._s3_user_config[key] = int(size)


    def get_partial(self, start, size):
        return self._s3_client.get_partial_object(self._bucket_name, self._object_name, start, size)


    def should_stream_to_cache(self, stream_to_cache=False):
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
            # get the size of the object
            try:
                object_stats = self._s3_client.stat_object(self._bucket_name, self._object_name)
            except BaseException:
                raise s3IOException("Error: " + self._full_url + " not found.")

            # check whether the size is greater than the user_config setting
            if object_stats.size > self._s3_user_config["max_file_size_for_memory"]:
                stream_to_file = True
            else:
                # check whether the size is greater than the free memory
                mem = virtual_memory()
                if object_stats.size > mem.available:
                    stream_to_file = True

        return stream_to_file


    def stream_to_cache(self):
        """
           Download the object to the cache as a file.
           If it already exists then check whether the object on the object store is newer than the file on the disk.
           :return: string filename in the cache
        """
        # Create the destination filepath
        dest_path = self._s3_user_config["cache_location"] + "/" + self._bucket_name + "/" + self._object_name

        # First check whether the file exists
        if os.path.exists(dest_path):
            # get the date of the object on the object store
            # these exceptions shouldn't really happen but I'm writing particularly defensive code!
            try:
                object_stats = self._s3_client.stat_object(self._bucket_name, self._object_name)
            except BaseException:
                raise s3IOException("Error: " + self._full_url + " not found.")
            # get the date of the corresponding file on the file system
            try:
                file_stats = os.stat(dest_path)
            except BaseException:
                raise IOError("Error: " + dest_path + " not found.")

            # the object on the object store is newer than the one on the disk
            if object_stats.last_modified > file_stats.st_mtime:
                # Redownload the file
                try:
                    self._s3_client.fget_object(bucket_name, object_name, dest_path)
                except BaseException:
                    raise s3IOException("Error: " + self._full_url + " not found.")
        else:
            # Does not exist so we have to download the file
            # first create the destination directory, if it doesn't exist
            dest_dir = os.path.dirname(dest_path)
            if not os.path.exists(dest_dir):
                os.makedirs(os.path.dirname(dest_dir))
            # now try downloading the file
            try:
                self._s3_client.fget_object(self._bucket_name, self._object_name, self._dest_path)
            except BaseException:
                raise s3IOException("Error: " + self._full_url + " not found.")

        return dest_path


    def stream_to_memory(self):
        """
           Download the object to some memory.
           :return: memory buffer containing the bytes of the netCDF file
        """
        try:
            s3_object = self._s3_client.get_object(self._bucket_name, self._object_name)
        except BaseException:
            raise s3IOException("Error: " + self._full_url + " not found.")
        # stream the data
        return s3_object.data


    def object_exists(self):
        """
           Check whether the object actually exists
        """
        try:
            object = self._s3_client.stat_object(self._bucket_name, self._object_name)
            return True
        except BaseException:
            return False


    def create_bucket(self):
        """
           Create a bucket on S3 storage
        """
        # check the bucket exists
        if not self._s3_client.bucket_exists(self._bucket_name):
            try:
                self._s3_client.make_bucket(self._bucket_name)
            except BaseException:
                raise s3IOException("Error: " + self._full_url + " cannot create bucket.")


    def write(self, s3_cache_filename):
        """
           Write a file in the cache to the s3 storage
        """
        try:
            self._s3_client.fput_object(self._bucket_name, self._object_name, s3_cache_filename)
        except BaseException:
            raise s3IOException("Error: " + self._full_url + " cannot write S3 object.")

    def write_object(self, s3_object_uri, s3_cache_filename):
        """
            Write a file in the cache to a named object / uri on the s3 storage
        """
        split_ep = s3_object_uri.split("/")
        # now get the bucketname
        bucket_name = split_ep[3]
        # finally set the object (prefix + object name)
        object_name = "/".join(split_ep[4:])

        # get the local name
        local_name = os.path.join(self.get_cache_location(), s3_cache_filename)

        try:
            self._s3_client.fput_object(bucket_name, object_name, local_name)
        except BaseException:
            raise s3IOException("Error: " + s3_object_uri + " cannot write S3 object.")


    def get_full_url(self):
        return self._full_url


    def get_cache_location(self):
        return self._s3_user_config["cache_location"]


    def get_max_object_size(self):
        return self._s3_user_config["max_object_size"]