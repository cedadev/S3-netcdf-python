#!python
#cython: language_level=3

__copyright__ = "(C) 2019-2021 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

"""
File management for S3netCDF.  Operation:
o. Files are opened from a single URL
o. The manager determines, using the ConfigManager, which fileobject to use to
   open the file
o. A file object is returned.
o. Reading / writing to a file can then be performed by operations on the file
   object.
"""

import asyncio
import inspect
import time
import os
from psutil import Process
from urllib.parse import urlparse
from collections import OrderedDict
from hashlib import sha1
import numpy as np
import gc

from S3netCDF4.Managers._ConfigManager import Config
from S3netCDF4._Exceptions import IOException, MemoryException, APIException
from S3netCDF4.Backends._s3aioFileObject import s3aioFileObject
from S3netCDF4.Backends._s3FileObject import s3FileObject

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
        Initialise file handle to None.  Default to read"""
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

    @file_handle.setter
    def file_handle(self, val):
        self._fh = val

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
            self.file_handle.seek(seek_pos)
            data = self.file_handle.read(nbytes)
            # seek back to 0 ready for any subsequent read
            self.file_handle.seek(seek_pos)
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
        """Close the file."""
        # write any in-memory data into the file, if it is a remote file
        if ('w' in self._mode and self._remote_system and data is not None):
            # if it's an async system them write until completed
            if self.async_system:
                self._event_loop.run_until_complete(
                    self.file_handle.write(data)
                )
            else:
                self.file_handle.write(data)

        # close the file handle
        if self.async_system:
            self._event_loop.run_until_complete(
                self.file_handle.close()
            )
        else:
            self.file_handle.close()

    def glob(self):
        """List the files at the path stored in file_handle, matching the
        pattern in pattern"""
        if self.async_system:
            # schedule a task to read the entire file
            el = self._event_loop
            glob_task = el.create_task(self.file_handle.glob())
            el.run_until_complete(glob_task)
            files = glob_task.result()
        else:
            files = self.file_handle.glob()
        return files

    def size(self):
        """Return the size of a file, either on the remote system or the disk,
        async or synchronously.  On the file system this will leave the file
        pointer at position 0, so don't use halfway through a write, unless you
        seek back to the previous position"""
        if self.remote_system:
            if self.async_system:
                el = self._event_loop
                size_task = el.create_task(self.file_handle._getsize())
                el.run_until_complete(size_task)
                size = size_task.result()
            else:
                size = self.file_handle._getsize()
        else:
            self.file_handle.seek(0, os.SEEK_END)
            size = self.file_handle.tell()
            self.file_handle.seek(0)
        return size

class OpenFileRecord(object):
    """An object that contains a record of a file in the FileManager.
    This is different to the FileObject above, which records the file's
    representation of itself, which is external to the FileManager.
    OpenFileRecord records the FileManager representation of the file, i.e. the
    internal representation of the file within the system."""

    """Potential open states"""
    OPEN_NEW_IN_MEMORY = 1
    OPEN_EXISTS_IN_MEMORY = 2
    KNOWN_EXISTS_ON_STORAGE = 3
    OPEN_NEW_ON_DISK = 11
    OPEN_EXISTS_ON_DISK = 12
    KNOWN_EXISTS_ON_DISK = 13
    DOES_NOT_EXIST = 100

    open_state_mapping = {
        OPEN_NEW_IN_MEMORY : "OPEN_NEW_IN_MEMORY",
        OPEN_EXISTS_IN_MEMORY : "OPEN_EXISTS_IN_MEMORY",
        KNOWN_EXISTS_ON_DISK : "KNOWN_EXISTS_ON_DISK",
        KNOWN_EXISTS_ON_STORAGE : "KNOWN_EXISTS_ON_STORAGE",
        OPEN_NEW_ON_DISK : "OPEN_NEW_ON_DISK",
        OPEN_EXISTS_ON_DISK : "OPEN_EXISTS_ON_DISK",
        DOES_NOT_EXIST : "DOES_NOT_EXIST"
    }

    def __init__(self, url, size, file_object,
                 last_accessed, open_state, open_mode="r", lock=False):
        """Just load all the values in from the constructor."""
        self.url = url
        self.size = size
        self.file_object = file_object
        self.data_object = None
        self.last_accessed = last_accessed
        self.open_state = open_state
        self.open_mode = open_mode
        self.lock = lock

    def __repr__(self):
        """String representation of the OpenFileRecord."""
        repstr = repr(type(self)) + "\n"
        repstr += "\turl = {}".format(self.url) + "\n"
        repstr += "\tsize = {}, last_accessed = {}, open_state = {}, locked={}".format(
            self.size, self.last_accessed,
            OpenFileRecord.open_state_mapping[self.open_state],
            self.lock
        )
        return repstr

class OpenArrayRecord(object):
    """An object that contains a record of a currently active array.
    This array may be in memory, or be a numpy memmap array (if the allocated
    memory has run out).  Details of the size, type (IN_MEMORY | MEMMAP) and
    location (neccessary for MEMMAP arrays) are recorded."""

    """Potential array types"""
    IN_MEMORY = 0
    MEMMAP = 1

    array_type_mapping = {
        IN_MEMORY : "IN_MEMORY",
        MEMMAP : "MEMMAP"
    }

    def __init__(self, size, array_type, array_location=None):
        """Load all the values in from the constructor"""
        self.size = size
        self.array_type = array_type
        self.array_location = array_location

    def __repr__(self):
        """String representation of the OpenArrayRecord."""
        repstr = repr(type(self)) + "\n"
        repstr += "\tsize = {}, array_type = {}".format(
                  self.size,
                  OpenArrayRecord.array_type_mapping[self.array_type]
                )
        if self.array_location:
            repstr += ", array_location = {}".format(self.array_location)
        return repstr

class FileManager(object):
    """Class to return a file object handle when supplied with a URL / URI /
    filepath.  Uses an instance of the ConfigManager to read in the mapping
    between aliases and storage systems."""

    """Static member variable: ConfigManager"""
    _config = Config()

    def __init__(self):
        self._open_files = OrderedDict()
        self._open_arrays = []  # only need a list for arrays as there may be
                                # no way of identifying them

    def __open(self, url, mode="r"):
        """Open a file on one of the supported backends and return a
        corresponding fileobject to it."""
        # create a file object to store information about the file handle and
        # whether it is an asyncio and / or a remote filesystem, and the
        # event loop for asyncio filesystems
        _fo = FileObject()
        _fo._mode = mode

        # check whether a glob_url or not
        is_glob_url = ('*' in url or '?' in url)

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
            _fo.file_handle = eval(arg_str)
        else:
            # try opening just on the file system
            try:
                # might need to create the parent directory(ies)
                if mode == "w":
                    dir_path = os.path.dirname(url)
                    if dir_path and not os.path.exists(dir_path):
                        os.makedirs(dir_path)
                # need to open in binary
                if not "b" in mode:
                    open_mode = mode + "b"
                # check if it's a directory or not, or contains glob wildcards
                if (os.path.isdir(url) or is_glob_url):
                    _fo.file_handle = None
                else:
                    _fo.file_handle = open(url, mode=open_mode)
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
        except AttributeError:
            _fo._remote_system = False

        # debug test - force remote_system
        #_fo._remote_system = True
        # This all looks very bizarre - but it due to us using Cython,
        # rather than CPython and asyncio coroutines.
        # In CPython each coroutine function has 128 added to the code type
        # bit mask, whereas in Cython, 128 is not present in the bit mask.
        # This means that inspect.iscoroutine() fails to acknowledge that
        # a Cython compiled coroutine function is a coroutine function!!!
        # This workaround seems quite elegant, but relies on instantiating
        # the connection before it is optimum
        try:
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
        except AttributeError:
            _fo._async_system = False
        return _fo

    def __shuffle_files_filehandle(self):
        """This function moves files out of memory by closing them, when the
        number of file handles supported by ulimit is reached.  It then marks
        them in the file manager as "KNOWN_EXISTS_ON_DISK".
        This indicates that the next time the files are opened (for writing),
        they are opened in append mode."""
        # get the subset where the status is "EXISTS_ON_DISK" and the file is
        # not locked
        exists_on_disk = OrderedDict({
            k: v for k, v in self._open_files.items() if (v.open_state == OpenFileRecord.OPEN_EXISTS_ON_DISK and not v.lock)
        })
        # sort the dictionary on the last_accessed time
        sorted_time_accessed = sorted(
            exists_on_disk.items(),
            key=lambda kvp: kvp[1].last_accessed
        )
        cp = 0
        # we need a case where all the files have been closed but there are
        # still no file handles remaining.  This occurs when cp is greater than
        # the number of files initially in the OPEN_EXISTS_ON_DISK state
        while (FileManager.__file_greater_than_filehandles()
               and cp < len(exists_on_disk)):
            # get the open file record
            key = sorted_time_accessed[cp][0]
            self.free_file(key=key, keep_reference=True)
            cp += 1

        # check if there are enough file handles
        if (FileManager.__file_greater_than_filehandles()):
            raise IOException("File handles exceed resource allocation")

    @staticmethod
    def __file_greater_than_filehandles(size=0):
        # Provide a function to measure the number of file handles against
        # that imposed by the "resource_allocation" entry in the config file
        # This will be used when checking whether files should be shuffled
        # and as a parameter in the __shuffle_files function
        # Size has to be passed in so that __shuffle_files function can use
        # this or __size_greater_than_memory - i.e. they have the same
        # parameter signature
        # get the ulimit -n number of permitted open files
        soft_limit = (
            FileManager._config["resource_allocation"]["filehandles"]
        )
        # get the current process and measure the number of open files
        proc = Process()
        n_open_files = len(proc.open_files())
        # +2 here as we want to check if we can open another file
        # next time and we need two file handles at one point
        # when we open the file and then open the netCDF
        return (n_open_files + 2 >= soft_limit)

    @staticmethod
    def __report_memory_usage():
        # Get a string of memory usage stats
        memory_limit = (
            FileManager._config["resource_allocation"]["memory"]
        )

        proc = Process()
        used_memory = proc.memory_info().rss
        available_memory = memory_limit - used_memory
        return ("Used memory: {} Available memory: {}".format(
            used_memory/1024, available_memory/1024))

    @staticmethod
    def __size_greater_than_memory(size):
        # Provide a function to measure the size of the requested file
        # against the remaining memory that has been allocated to this
        # instance of S3-netCDF python via the config file entry:
        # "resource_allocation" : {"memory" : }
        # This will be used when checking whether files already in memory
        # should be shuffled to their storage medium to make room for the
        # new file
        memory_limit = (
            FileManager._config["resource_allocation"]["memory"]
        )
        # get the amount of memory used by this process
        proc = Process()
        # available_memory = resource allocated memory - memory used by
        # process
        # rss = Resident Set Size (non swapped memory)
        used_memory = proc.memory_info().rss
        available_memory = memory_limit - used_memory
        return (size > available_memory)

    def __shuffle_files_memory(self, size):
        """This function moves files out of memory by closing them, when the
        available memory is full.  This occurs when streaming from storage to
        memory.
        It then marks them in the file manager as "KNOWN_EXISTS_ON_STORAGE".
        This indicates that the next time the files are opened (for writing),
        they are opened in append mode."""
        # get the subset where the status is "OPEN_EXISTS_IN_MEMORY"
        exists_in_mem = OrderedDict({
            k: v for k, v in self._open_files.items() if (v.open_state == OpenFileRecord.OPEN_EXISTS_IN_MEMORY and not v.lock)
        })
        # sort the dictionary on the last_accessed time
        sorted_time_accessed = sorted(
            exists_in_mem.items(),
            key=lambda kvp: kvp[1].last_accessed
        )
        cp = 0
        # we need a case where all the files have been closed but there is
        # still no memory remaining.  This occurs when cp is greater than
        # the number of files initially in the OPEN_EXISTS_IN_MEMORY state
        while (FileManager.__size_greater_than_memory(size)
               and cp < len(exists_in_mem)):
            # get the open file record
            key = sorted_time_accessed[cp][0]
            self.free_file(key=key, keep_reference=True)
            cp += 1

        # check if there is enough free memory
        # if (FileManager.__size_greater_than_memory(size)):
        #     print("Size exceeds allocation, {}".format(
        #         FileManager.__report_memory_usage())
        #     )

    def get_file_open_state(self, url=None, key=None):
        """Query the state of a file.  This allows for more clean requests,
        i.e. we can request in "a" mode, rather than always in "w" mode in
        __getitem__ and __setitem__"""
        if key == None:
            assert(url != None)
            key = generate_key(url)

        if key in self._open_files:
            return (self._open_files[key].open_state,
                    self._open_files[key].open_mode)
        else:
            return OpenFileRecord.DOES_NOT_EXIST, 'n'

    def __close_before_reopen(self, key, mode):
        """Check for mode equivalence ("a" and "w" are equivalent) and close
        a file if the requested mode is not equivalent to the file's existing
        mode."""
        if (self._open_files[key].open_state in
            [OpenFileRecord.OPEN_EXISTS_IN_MEMORY,
             OpenFileRecord.OPEN_EXISTS_ON_DISK]):
            # "a" and "w" are equivalent
            if (self._open_files[key].open_mode in ["a", "w"] and
                 mode not in ["a", "w"]):
                return True
            if (self._open_files[key].open_mode == "r" and mode != "r"):
                return True
        return False

    def request_file(self, url, size=0, mode="r", lock=False):
        """Request a file, and return a file object to it.
        1. Files returned from this function are managed.
        2. They are stored in a dictionary with their file object, size, last
           time they were accessed and whether they are in memory at the moment.
        3. If a file is requested and there is not enough memory or file handles
           to hold the file then another file is removed from the memory:
           3a. If it is a read-only file it is just thrown out
           3b. If it is a write / append file it is written out to the storage.
        4. When a file is requested in write mode, it is first checked whether
           it has been accessed before.
           4a. If it has then it is read in from the file system (where it was
           previously written to), in append mode.
           4b. If it hasn't then it is created (with CLOBBER - i.e. it is
           overwritten).
        5. When a file is requested in append mode it is checked whether it can
           be read in from the file system.
           4a. If it exists, it is read in.
           4b. If it doesn't exist it is created.
        6. Lock = True indicates that the file cannot be shuffled out of memory
           It is necessary to always have the Master Array File in memory for
           S3netCDF.
        7. Entries opened in a different mode will be closed before being opened
           in a new mode
        """
        # generate the key from hashing the url and adding the mode
        key = generate_key(url)
        # is this a url that is to be globbed (don't get the size if it is)
        is_glob_url = ('*' in url or '?' in url)
        #
        if (key in self._open_files):
            # See if the file exists in memory, and check the mode the file is
            # opened in - if it's different to the requested then close the file
            # this will upload the file if remote, or write to disk if not
            if self.__close_before_reopen(key, mode):
                self.free_file(key=key, keep_reference=True)

            # Check to see if file needs shuffling
            if (self._open_files[key].open_state ==
                  OpenFileRecord.KNOWN_EXISTS_ON_STORAGE):
                fo = self.__open(url, mode)
                # reassign the file object
                self._open_files[key].file_object = fo
                self._open_files[key].open_mode = mode
                # see the notes below about checking for file size
                if fo.remote_system:
                    if size == 0 and (mode == 'r' or mode == 'a') and not is_glob_url:
                        req_size = fo.size()
                        # if FileManager.__size_greater_than_memory(req_size):
                        #     print("Trying to stream a file that exceeds "
                        #           "memory allocation, {}".format(
                        #         FileManager.__report_memory_usage())
                        #     )
                    else:
                        req_size = size
                if FileManager.__size_greater_than_memory(req_size):
                    self.__shuffle_files_memory(req_size)

            elif (self._open_files[key].open_state ==
                  OpenFileRecord.KNOWN_EXISTS_ON_DISK):
                # check there are enough filehandles to reopen the file
                # reassign write to append
                if mode == "w":
                    open_mode = "a"
                else:
                    open_mode = mode
                if FileManager.__file_greater_than_filehandles():
                    self.__shuffle_files_filehandle()

                # reassign the file object
                fo = self.__open(url, open_mode)
                self._open_files[key].file_object = fo
                self._open_files[key].open_mode = open_mode

            # modify the last accessed time
            self._open_files[key].last_accessed = time.time()
            self._open_files[key].lock = lock
        else:
            # get a file object to the (potentially) remote system
            try:
                fo = self.__open(url, mode)
                # if size is 0 then get the actual size.  This means that the
                # entire file will be streamed into memory if there is enough
                # available memory
                if fo.remote_system:
                    # if we are writing (clobbering) a file that it doesn't
                    # matter how big it is currently - so only do this check
                    # for reading or appending, where we actually are going to
                    # stream the file into memory
                    if size == 0 and (mode == 'r' or mode == 'a') and not is_glob_url:
                        req_size = fo.size()
                        # if FileManager.__size_greater_than_memory(req_size):
                        #     print("Trying to stream a file that exceeds "
                        #           "memory allocation, {}".format(
                        #         FileManager.__report_memory_usage())
                        #     )
                    else:
                        req_size = size
                    # if a size is requested then we can shuffle the contents
                    # of memory
                    if FileManager.__size_greater_than_memory(req_size):
                        self.__shuffle_files_memory(req_size)

            except FileNotFoundError:
                # if we are reading and the file does not exist
                self._open_files[key] = OpenFileRecord(
                    url = url,
                    size = 0,
                    file_object = None,
                    open_state = OpenFileRecord.DOES_NOT_EXIST,
                    open_mode = "n",
                    last_accessed = 0,
                )
            else:
                # determine if this filesystem is remote or locally attached
                # disk
                if fo.remote_system:
                    # it's remote so create in memory
                    os = OpenFileRecord.OPEN_NEW_IN_MEMORY
                else:
                    # it's local so create on the disk
                    os = OpenFileRecord.OPEN_NEW_ON_DISK
                    # check that there are enough file handles to open
                    # another local file
                    if FileManager.__file_greater_than_filehandles():
                        self.__shuffle_files_filehandle()

                # determine the open state on the file object
                self._open_files[key] = OpenFileRecord(
                    url  = url,
                    size = size,
                    file_object = fo,
                    open_state = os,
                    open_mode = mode,
                    last_accessed = time.time(),
                    lock = lock
                )
        return self._open_files[key]

    def open_success(self, url=None, key=None):
        """This is a function to be called after request_file to indicate that
        the file has been opened successfully by the calling application.
        It manages state transistion from OPEN_NEW_ON_DISK | OPEN_NEW_IN_MEMORY
        to OPEN_EXISTS_ON_DISK | OPEN_EXISTS_IN_MEMORY."""
        # hash to get the key and make sure it's in the list of open files
        if key == None:
            assert(url != None)
            key = generate_key(url)
        assert(key in self._open_files)
        # change the states
        if self._open_files[key].open_state == OpenFileRecord.OPEN_NEW_ON_DISK:
            self._open_files[key].open_state = OpenFileRecord.OPEN_EXISTS_ON_DISK
        elif self._open_files[key].open_state == OpenFileRecord.OPEN_NEW_IN_MEMORY:
            self._open_files[key].open_state = OpenFileRecord.OPEN_EXISTS_IN_MEMORY
        elif self._open_files[key].open_state == OpenFileRecord.KNOWN_EXISTS_ON_DISK:
            self._open_files[key].open_state = OpenFileRecord.OPEN_EXISTS_ON_DISK
        elif self._open_files[key].open_state == OpenFileRecord.KNOWN_EXISTS_ON_STORAGE:
            self._open_files[key].open_state = OpenFileRecord.OPEN_EXISTS_IN_MEMORY

    def free_file(self, url=None, key=None, keep_reference=False):
        """Free a file and (crucially) free the resources of a managed file.
        The user can supply the url or the key (key will be faster as it doesn't
        need to hash the url).  keep_reference=True will retain the reference to
        the file in self._open_files
        """
        if key == None:
            assert(url != None)
            key = generate_key(url)
        assert(key in self._open_files)

        open_file = self._open_files[key]
        # close the data object and file object
        if open_file.data_object:
            nc_bytes = open_file.data_object.close()
        else:
            nc_bytes = None

        if open_file.open_state == OpenFileRecord.OPEN_EXISTS_ON_DISK:
            # mark as KNOWN to the system - open in append mode next time
            if open_file.file_object:
                open_file.file_object.close()
            open_file.open_state = OpenFileRecord.KNOWN_EXISTS_ON_DISK
        elif open_file.open_state == OpenFileRecord.OPEN_EXISTS_IN_MEMORY:
            # close the data object, get the data and write to the file object
            open_file.file_object.close(nc_bytes)
            open_file.open_state = OpenFileRecord.KNOWN_EXISTS_ON_STORAGE

        open_file.data_object = None
        open_file.file_object = None
        nc_bytes = None

        # garbage collect
        n_unreach = gc.collect()

        # delete file reference if this is a permanent close
        if not keep_reference:
            open_file.open_state = OpenFileRecord.DOES_NOT_EXIST

    def free_all_files(self):
        """Free all the files in turn in the FileManager.  Do this on close
        of your main file."""
        for file_key in self._open_files:
            self.free_file(key=file_key, keep_reference=False)

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
        target_array_size = np.prod(target_array_shape) * dtype.itemsize

        if FileManager.__size_greater_than_memory(target_array_size):
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
            array_type = OpenArrayRecord.MEMMAP
            array_location = mmap_name
        else:
            target_array = np.empty(
                target_array_shape,
                dtype=dtype
            )
            array_type = OpenArrayRecord.IN_MEMORY
            array_location = None

        # add the array to the record of arrays
        array_record = OpenArrayRecord(
                            target_array_size, array_type, array_location
                       )
        self._open_arrays.append(array_record)
        return target_array

    def free_all_arrays(self):
        """Free all the arrays created by the file manager.
        Note that this only actually deletes the memory mapped arrays on disk.
        The arrays that are in memory are deleted by the garbage collector."""
        for arry in self._open_arrays:
            if arry.array_type == OpenArrayRecord.MEMMAP:
                if os.path.exists(arry.array_location):
                    os.unlink(arry.array_location)
