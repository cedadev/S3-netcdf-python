"""
File management for S3netCDF.  Operation:
o. Files are opened from a single URL
o. The manager determines, using the ConfigManager, which fileobject to use to
   open the file
o. A file object is returned.
o. Reading / writing to a file can then be performed by operations on the file
   object.
"""

__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import asyncio
import inspect
import time
import os
from psutil import virtual_memory
from urllib.parse import urlparse
from collections import namedtuple
from hashlib import sha1
import numpy as np

from S3netCDF4.Managers._ConfigManager import Config
from S3netCDF4._Exceptions import *
from S3netCDF4.Backends import *

def generate_key(url):
    """Generate a key for the file manager to store a OpenFileRecord"""
    return sha1(url.encode("utf-8")).hexdigest()

class FileObject(object):
    """Class to return a file object, which contains a file object handle,
    returned by the FileManager class below.
    This class also allows methods to determine whether they are async or
    remote file systems."""

    def __init__(self):
        """Initialise remote system and async system to False
        Initialise file handle to parameter."""
        self._remote_system = False
        self._async_system = False
        self._fh = None
        self._mode = "r"

    # getter for remote system and async system
    @property
    def remote_system(self):
        return self._remote_system

    @property
    def async_system(self):
        return self._async_system

    @property
    def mode(self):
        return self._mode

    @property
    def file_handle(self):
        return self._fh

    async def _seek_then_read(self, seek_pos, nbytes):
        await self.file_handle.seek(seek_pos)
        data = await self.file_handle.read(nbytes)
        await self.file_handle.seek(seek_pos)
        return data

    def read_from(self, seek_pos=0, nbytes=4):
        """Read the contents of the file from the seek position, for a set
        number of bytes."""
        if self.async_system:
            # schedule a task to read the first 6 bytes of the stream and
            # wait for it to complete, then get the result
            el = self._event_loop
            read_from_task = el.create_task(
                self._seek_then_read(seek_pos, nbytes)
            )
            el.run_until_complete(read_from_task)
            data = read_from_task.result()
        else:
            self.file_handle.seek(0)
            data = self.file_handle.read(6)
            # seek back to 0 ready for any subsequent read
            self.file_handle.seek(0)
        return data

    def read(self):
        """Read the entire contents of the file."""
        if self.async_system:
            # schedule a task to read the entire file
            el = self._event_loop
            read_task = el.create_task(
                self.file_handle.read()
            )
            el.run_until_complete(read_task)
            data = read_task.result()
        else:
            data = self.file_handle.read()
        return data

    def close(self, data=None):
        """Close the Dataset."""
        # write any in-memory data into the file, if it is a remote file
        if ('w' in self._mode and self._remote_system and data is not None):
            # if it's an async system them write until completed
            if self._async_system:
                self._event_loop.run_until_complete(
                    self.file_handle.write(data)
                )
            else:
                self.file_handle.write(data)

        # close the file handle
        if self._async_system:
            self._event_loop.run_until_complete(
                self.file_handle.close()
            )
        else:
            self.file_handle.close()

class OpenFileRecord(object):
    """An object that contains a record of a file in the FileManager.
    This is different to the FileObject above, which records the file's
    representation of itself, which is external to the FileManager.
    OpenFileRecord records the FileManager representation of the file, i.e. the
    internal representation of the file within the system."""

    """Potential open states"""
    OPEN_NEW_IN_MEMORY = 0
    OPEN_EXISTS_IN_MEMORY = 1
    KNOWN_EXISTS_ON_STORAGE = 2
    OPEN_NEW_ON_DISK = 3
    OPEN_EXISTS_ON_DISK = 4
    DOES_NOT_EXIST = 100

    open_state_mapping = {
        OPEN_NEW_IN_MEMORY : "OPEN_NEW_IN_MEMORY",
        OPEN_EXISTS_IN_MEMORY : "OPEN_EXISTS_IN_MEMORY",
        KNOWN_EXISTS_ON_STORAGE : "KNOWN_EXISTS_ON_STORAGE",
        OPEN_NEW_ON_DISK : "OPEN_NEW_ON_DISK",
        OPEN_EXISTS_ON_DISK : "OPEN_EXISTS_ON_DISK",
        DOES_NOT_EXIST : "DOES_NOT_EXIST"
    }

    @property
    def data_object(self):
        return self._data_object

    @data_object.setter
    def data_object(self, val):
        self._data_object = val

    def __init__(self, url, size, file_object, last_accessed, open_state):
        """Just load all the values in from the constructor."""
        self.url = url
        self.size = size
        self.file_object = file_object
        self.last_accessed = last_accessed
        self.open_state = open_state
        self._data_object = None

    def __repr__(self):
        """String representation of the OpenFileRecord."""
        repstr = repr(type(self)) + "\n"
        repstr += "\turl = {}".format(self.url) + "\n"
        repstr += "\tsize = {}, last_accessed = {}, open_state = {}".format(
            self.size, self.last_accessed,
            OpenFileRecord.open_state_mapping[self.open_state]
        )
        return repstr

    def close(self):
        self._data_object.close()

class FileManager(object):
    """Class to return a file object handle when supplied with a URL / URI /
    filepath.  Uses an instance of the ConfigManager to read in the mapping
    between aliases and storage systems."""

    """Static member variable: ConfigManager"""
    _config = Config()

    def __init__(self):
        self._open_files = {}

    @property
    def files(self):
        return self._open_files

    def _open(self, url, mode="r"):
        """Open a file on one of the supported backends and return a
        corresponding fileobject to it."""
        # create a file object to store information about the file handle and
        # whether it is an asyncio and / or a remote filesystem, and the
        # event loop for asyncio filesystems
        _fo = FileObject()
        _fo._mode = mode

        # split the url into the scheme, netloc, etc.
        url_o = urlparse(url)
        # the alias is the scheme + "://" + netloc
        alias = url_o.scheme + "://" + url_o.netloc
        # check if alias is in the config dictionary under "hosts"
        if alias in FileManager._config["hosts"]:
            # get which backend this url pertains to
            try:
                backend = FileManager._config["hosts"][alias]["backend"]
                # get the true path
                true_path = FileManager._config["hosts"][alias]["url"] + url_o.path
                credentials = FileManager._config["hosts"][alias]["credentials"]
            except KeyError:
                raise APIException(
                    "Configuration file for {} is incomplete: {}.".format(
                        alias,
                        FileManager._config["filename"]
                    )
                )
            # create the argument string for creating the backend fileobject:
            arg_str = (backend + '("' + true_path + '"' +
                       ', mode="' + mode +
                       '", credentials=' + str(credentials) +
                       ')')
            # use eval to execute the arg_str
            _fo._fh = eval(arg_str)
        else:
            # try opening just on the file system
            try:
                # might need to create the parent directory(ies)
                if mode == "w":
                    dir_path = os.path.dirname(url)
                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path)
                _fo._fh = open(url, mode=mode)
            except FileNotFoundError as e:
                if alias != "://" and alias not in FileManager._config["hosts"]:
                    raise IOException(
                        "Host {} is not present in the user config file: {}".format(
                            alias,
                            FileManager._config["filename"]
                        )
                    )
                else:
                    raise e
        # determine whether this is a remote file system and / or an asyncio
        # file system
        # see if it's a remote dataset - if the file handle has a connect method
        try:
            if inspect.ismethod(_fo.file_handle.connect):
                _fo._remote_system = True
            else:
                _fo._remote_system = False
        except:
            _fo._remote_system = False

        # debug test - force remote_system
        #_fo._remote_system = True
        try:
            # This all looks very bizarre - but it due to us using Cython,
            # rather than CPython and asyncio coroutines.
            # In CPython each coroutine function has 128 added to the code type
            # bit mask, whereas in Cython, 128 is not present in the bit mask.
            # This means that inspect.iscoroutine() fails to acknowledge that
            # a Cython compiled coroutine function is a coroutine function!!!
            # This workaround seems quite elegant, but relies on instantiating
            # the connection before it is optimum
            connection = _fo.file_handle.connect()
            if "coroutine" in str(connection):
                _fo._async_system = True
                _fo._event_loop = asyncio.get_event_loop()
                # create a task to be scheduled to connect to the remote server
                connect_task = _fo._event_loop.create_task(
                    connection
                )
                # wait for completion
                _fo._event_loop.run_until_complete(
                    connect_task
                )
            else:
                _fo._async_system = False
        except:
            _fo._async_system = False
        return _fo

    def request_file(self, url, size=0, mode="r"):
        """Request a file, and return a file object to it.
        1. Files returned from this function are managed.
        2. They are stored in a dictionary with their file object, size, last
           time they were accessed and whether they are in memory at the moment.
        3. If a file is requested and there is not enough memory to hold the
           file then another file is removed from the memory:
           3a. If it is a read-only file it is just thrown out
           3b. If it is a write / append file it is written out to the storage.
        4. When a file is requested in write mode, it is first checked whether
           it has been accessed before.
           4a. If it has then it is read in from the file system (where it was
           previously written to).
           4b. If it hasn't then it is created (with CLOBBER - i.e. it is
           overwritten).
        5. When a file is requested in append mode it is checked whether it can
           be read in from the file system.
           4a. If it exists, it is read in.
           4b. If it doesn't exist it is created.
        """
        # generate the key from hashing the url
        key = generate_key(url)
        if key in self._open_files:
            # update the open state
            if self._open_files[key].open_state == OpenFileRecord.OPEN_NEW_IN_MEMORY:
                self._open_files[key].open_state = OpenFileRecord.OPEN_EXISTS_IN_MEMORY
            elif self._open_files[key].open_state == OpenFileRecord.OPEN_NEW_ON_DISK:
                self._open_files[key].open_state = OpenFileRecord.OPEN_EXISTS_ON_DISK

            # modify the last accessed time
            self._open_files[key].last_accessed = time.time()
        else:
            # the file is not currently in memory - check that there is enough
            # memory available
            available_memory = virtual_memory().available
            if size > available_memory:
                raise IOException("Out of memory")
            else:
                # get a file object to the (potentially) remote system
                try:
                    fo = self._open(url, mode)
                except FileNotFoundError:
                    # if we are reading and the file does not exist
                    os = OpenFileRecord.DOES_NOT_EXIST
                    self._open_files[key] = OpenFileRecord(
                        url = url,
                        size = 0,
                        file_object = None,
                        open_state = os,
                        last_accessed = 0
                    )
                else:
                    # determine if this filesystem is remote or locally attached disk
                    if fo.remote_system:
                        # it's remote so create in memory
                        os = OpenFileRecord.OPEN_NEW_IN_MEMORY
                    else:
                        # it's local so create on the disk
                        os = OpenFileRecord.OPEN_NEW_ON_DISK
                    # determine the open state on the file object
                    self._open_files[key] = OpenFileRecord(
                        url  = url,
                        size = size,
                        file_object = fo,
                        open_state = os,
                        last_accessed = time.time()
                    )
        return self._open_files[key]

    def request_array(self, index_list, dtype, base_name=""):
        """Create an array based on the info detailed in elem and dtype.
        index_list contains indices containing partitions"""
        # get the number of dimensions from the partition location
        n_dims = index_list[0].partition.location.shape[0]
        # find the max / min bounds of the target array
        target_array_min = np.zeros(n_dims, dtype=np.int32)
        target_array_max = np.zeros(n_dims, dtype=np.int32)
        for index in index_list:
            for d in range(0, n_dims):
                target_array_min[d] = np.minimum(
                    target_array_min[d],
                    index.target[d].start
                )
                target_array_max[d] = np.maximum(
                    target_array_max[d],
                    index.target[d].stop
                )
        # calculate the target shape as max - min
        target_array_shape = target_array_max - target_array_min
        # get the size and see if there is enough memory to create the array
        target_array_size = np.prod(target_array_shape)
        available_memory = virtual_memory().available
        if target_array_size > 0:#available_memory:
            # construct a name for the memory mapped array
            mmap_name = os.path.join(
                FileManager._config['cache_location'] + "/",
                os.path.basename(base_name) +\
                "_{}".format(int(np.random.uniform(0,1e8)))
            )
            target_array = np.memmap(
                mmap_name,
                shape=tuple(target_array_shape),
                dtype=dtype,
                mode="w+"
            )
        else:
            target_array = np.empty(
                target_array_shape,
                dtype=dtype
            )
        return target_array

    def free_file(self, url):
        """Free a file from the file manager after it was opened with request_file
        """
        # generate the key from hashing the url
        key = generate_key(url)
        if key in self._open_files:
            # update the open state
            pass
