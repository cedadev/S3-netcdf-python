"""
An S3 client that has a local file cache.  For use by CEDA S3 / object store read / write libraries.

Author  : Neil Massey
Date    : 10/07/2017
Modified: 07/08/2017: Rewritten as a class so we don't have to pass so many variables around!
Modified: 07/09/2017: Removed one client / one object mapping, and introduced strict filename mapping between
                      objects in the cache and objects on the S3 storage.
"""

# using the minio client API as the interface to the S3 HTTP API
from minio import Minio
from psutil import virtual_memory
import json
import os

from _s3Exceptions import *

def urljoin(*args):
    """
    Joins given arguments into a url. Trailing but not leading slashes are
    stripped for each argument.
    """
    url = "/".join(map(lambda x: str(x).rstrip('/'), args))
    return url


class s3ClientConfig(object):
    """Class to read in config file and interpret it"""

    def __init__(self):
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


    def __getitem__(self, name):
        """Get a value from the s3 config"""
        return self._s3_user_config[name]


class s3Client(object):

    # abstraction of s3_client operations on netCDF files as a class
    def __init__(self, s3_endpoint, s3_user_config = None):

        # create an s3 config and read it in, if it hasn't already been supplied
        if s3_user_config is None:
            self._s3_user_config = s3ClientConfig()
        else:
            self._s3_user_config = s3_user_config

        # search through the "aliases" in the user_config file to find the http url for the s3 endpoint
        self._host_name = None
        try:
            hosts = self._s3_user_config["hosts"]
            for h in hosts:
                if s3_endpoint in hosts[h]['alias']:
                    self._host_name = h
        except:
            raise s3IOException("Error in config file " + self._s3_user_config["filename"])

        # check whether the url was found in the config file
        if self._host_name == None:
           raise s3IOException(s3_endpoint + " was not found as an alias in the user config file " + self._s3_user_config["filename"])

        # Now we have the host_name, get the url, access key and secret key from the host data
        try:
            self._host_config = self._s3_user_config["hosts"][self._host_name]
            url_name = self._host_config['url']
            secret_key = self._host_config['secretKey']
            access_key = self._host_config['accessKey']
            self._url = self._s3_user_config["hosts"][self._host_name]["alias"]
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


    def get_partial(self, bucket_name, object_name, start, size):
        return self._s3_client.get_partial_object(bucket_name, object_name, start, size)


    def should_stream_to_cache(self, bucket_name, object_name):
        """
           Determine whether the object should be streamed to a file, or into memory.
           The criteria are as follows:
             1. The object is bigger than the user_config["max_file_size_for_memory"]
             2. The object is larger than the amount of free RAM
           :return: boolean (True | False)
        """
        stream_to_file = False

        # full url for error reporting
        full_url = urljoin(self._url, bucket_name, object_name)

        # This nested if..else.. is to improve performance - don't make any calls
        #  to object store or system processes unless we really have to.

        # get the size of the object
        try:
            object_stats = self._s3_client.stat_object(bucket_name, object_name)
        except BaseException:
            raise s3IOException("Error: " + full_url + " not found.")

        # check whether the size is greater than the user_config setting
        if object_stats.size > self._s3_user_config["max_file_size_for_memory"]:
            stream_to_file = True
        else:
            # check whether the size is greater than the free memory
            mem = virtual_memory()
            if object_stats.size > mem.available:
                stream_to_file = True

        return stream_to_file


    def get_cachefile_path(self, bucket_name, object_name):
        # Create the destination filepath
        cachefile_path = self._s3_user_config["cache_location"] + "/" + bucket_name + "/" + object_name
        return cachefile_path


    def stream_to_cache(self, bucket_name, object_name):
        """
           Download the object to the cache as a file.
           If it already exists then check whether the object on the object store is newer than the file on the disk.
           :return: string filename in the cache
        """

        # full url for error reporting
        full_url = urljoin(self._url, bucket_name, object_name)

        # get the path in the cache
        dest_path = self.get_cachefile_path(bucket_name, object_name)

        # First check whether the file exists
        if os.path.exists(dest_path):
            # get the date of the object on the object store
            # these exceptions shouldn't really happen but I'm writing particularly defensive code!
            try:
                object_stats = self._s3_client.stat_object(bucket_name, object_name)
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
                    self._s3_client.fget_object(bucket_name, object_name, dest_path)
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
                self._s3_client.fget_object(bucket_name, object_name, dest_path)
            except BaseException:
                raise s3IOException("Error: " + full_url + " not found.")

        return dest_path


    def stream_to_memory(self, bucket_name, object_name):
        """
           Download the object to some memory.
           :return: memory buffer containing the bytes of the netCDF file
        """
        # full url for error reporting
        full_url = urljoin(self._url, bucket_name, object_name)

        try:
            s3_object = self._s3_client.get_object(bucket_name, object_name)
        except BaseException:
            raise s3IOException("Error: " + full_url + " not found.")
        # stream the data
        return s3_object.data


    def object_exists(self, bucket_name, object_name):
        """
           Check whether the object actually exists
        """
        try:
            object = self._s3_client.stat_object(bucket_name, object_name)
            return True
        except BaseException:
            return False


    def create_bucket(self, bucket_name):
        """
           Create a bucket on S3 storage
        """
        # full url for error reporting
        full_url = urljoin(self._url, bucket_name, object_name)
        # check the bucket exists
        if not self._s3_client.bucket_exists(bucket_name):
            try:
                self._s3_client.make_bucket(bucket_name)
            except BaseException:
                raise s3IOException("Error: " + full_url + " cannot create bucket.")


    def write(self, bucket_name, object_name):
        """
           Write a file in the cache to the s3 storage
        """
        # full url for error reporting
        full_url = urljoin(self._url, bucket_name, object_name)
        # get the path in the cache
        s3_cache_filename = self.get_cachefile_path(bucket_name, object_name)

        # check the file exists in the cache
        if not(os.path.exists(s3_cache_filename)):
            raise s3IOException("Error: " + s3_cache_filename + " file not found in cache.")

        try:
            self._s3_client.fput_object(bucket_name, object_name, s3_cache_filename)
        except BaseException:
            raise s3IOException("Error: " + full_url + " cannot write S3 object.")


    def get_cache_location(self):
        return self._s3_user_config["cache_location"]


    def get_max_object_size(self):
        return self._s3_user_config["max_object_size"]


    def get_full_url(self, bucket_name, object_name):
        full_url = urljoin(self._url, bucket_name, object_name)
        return full_url
