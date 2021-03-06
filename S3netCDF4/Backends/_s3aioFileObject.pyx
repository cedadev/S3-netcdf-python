#!python
#cython: language_level=3

__copyright__ = "(C) 2019-2021 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

import io
from fnmatch import fnmatch
from urllib.parse import urlparse

import asyncio
import aiobotocore
from botocore.exceptions import ClientError
import botocore.config

from S3netCDF4.Managers._ConnectionPool import ConnectionPool, ConnectionObject
from S3netCDF4.Managers._ConfigManager import Config
from S3netCDF4._Exceptions import APIException, IOException

class s3aioFileObject(object):
    """Custom file object class, inheriting from Python io.Base, to read from
    an S3 object store / AWS cloud storage."""

    """Static connection pool object - i.e. shared across the file objects."""
    _connection_pool = ConnectionPool()

    # The defaults for MAXIMUM_PART_SIZE etc. are now assigned in
    # __init__ if no values are found in ~/.s3nc.json
    """Static config object for the backend options"""
    _config = Config()

    def _get_server_bucket_object(uri):
        """Get the server name from the URI"""
        # First split the uri into the network location and path, and build the
        # server
        url_p = urlparse(uri)
        # check that the uri contains a scheme and a netloc
        if url_p.scheme == '' or url_p.netloc == '':
            raise APIException(
                "URI supplied to s3aioFileObject is not well-formed: {}". format(uri)
            )
        server = url_p.scheme + "://" + url_p.netloc
        split_path = url_p.path.split("/")
        # get the bucket
        try:
            bucket = split_path[1]
        except IndexError as e:
            raise APIException(
                "URI supplied has no bucket contained within it: {}".format(uri)
            )
        # get the path
        try:
            path = "/".join(split_path[2:])
        except IndexError as e:
            raise APIException(
                "URI supplied has no path contained within it: {}".format(uri)
            )
        return server, bucket, path

    def __init__(self, uri, credentials, mode='r', create_bucket=True,
                 part_size=None, max_parts=None, multipart_upload=None,
                 multipart_download=None, connect_timeout=None,
                 read_timeout=None):
        """Initialise the file object by creating or reusing a connection in the
        connection pool."""
        # get the server, bucket and the key from the endpoint url
        self._server, self._bucket, self._path = s3aioFileObject._get_server_bucket_object(uri)
        self._closed = False        # set the file to be not closed
        self._mode = mode
        self._seek_pos = 0
        self._buffer = [io.BytesIO()]   # have a list of objects that can stream
        self._credentials = credentials
        self._create_bucket = create_bucket
        self._uri = uri

        """Either get the backend config from the parameters, or the config file
        or use defaults."""
        if "s3aioFileObject" in s3aioFileObject._config["backends"]:
            backend_config = s3aioFileObject._config["backends"]["s3aioFileObject"]
        else:
            backend_config = {}

        if part_size:
            self._part_size = int(part_size)
        elif "maximum_part_size" in backend_config:
            self._part_size = int(backend_config["maximum_part_size"])
        else:
            self._part_size = int(50 * 1024 * 1024)

        if max_parts:
            self._max_parts = int(max_parts)
        elif "maximum_parts" in backend_config:
            self._max_parts = int(backend_config["maximum_parts"])
        else:
            self._max_parts = 8

        if multipart_upload:
            self._multipart_upload = multipart_upload
        elif "multipart_upload" in backend_config:
            self._multipart_upload = backend_config["multipart_upload"]
        else:
            self._multipart_upload = True

        if multipart_download:
            self._multipart_download = multipart_download
        elif "multipart_download" in backend_config:
            self._multipart_download = backend_config["multipart_download"]
        else:
            self._multipart_download = True

        if connect_timeout:
            self._connect_timeout = connect_timeout
        elif "connect_timeout" in backend_config:
            self._connect_timeout = backend_config["connect_timeout"]
        else:
            self._connect_timeout = 30.0

        if read_timeout:
            self._read_timeout = read_timeout
        elif "read_timeout" in backend_config:
            self._read_timeout = backend_config["read_timeout"]
        else:
            self._read_timeout = 30.0

    async def __aenter__(self):
        """Async version of the enter context method."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        """Close the file on the exit of a with statement, or by the garbage
        collector removing the object."""
        await self.close()
        # check for any exceptions
        if exc_type is not None:
            return False
        return True

    async def _getsize(self):
        # Use content length in the head object to determine how the size of
        # the file / object
        # If we are writing then the size should be the buffer size
        try:
            if 'w' in self._mode:
                size = self._part_size
            else:
                response = await self._conn_obj.conn.head_object(
                    Bucket=self._bucket,
                    Key=self._path
                )
                size = response['ContentLength']
        except ClientError as e:
            raise IOException(
                "Could not get size of object {}".format(self._path)
            )
        except AttributeError as e:
            self._handle_connection_exception(e)
        return size

    async def _get_bucket_list(self):
        # get the names of the buckets in a list
        try:
            bl = await self._conn_obj.conn.list_buckets()
            bucket_list = [b['Name'] for b in bl['Buckets']]
        except AttributeError as e:
            self._handle_connection_exception(e)
        return bucket_list

    def _handle_connection_exception(self, e):
        # Check if connection made
        if ("_conn_obj" in e.args[0] or "_current_part" in e.args[0]):
            raise APIException(
                "Connection to S3 server is not established.  Use either the "
                ".connect method or a with statement."
            )
        else:
            # other AttributeError - handle that separately
            raise e

    async def connect(self):
        """Connect to the s3 server with the details passed in via the __init__
        method."""
        # if the connection returns None then either there isn't a connection to
        # the server in the pool, or there is no connection that is available
        self._conn_obj = s3aioFileObject._connection_pool.get(self._server)
        if self._conn_obj is None:
            try:
                session = aiobotocore.get_session()
                config = botocore.config.Config(
                    connect_timeout=self._connect_timeout,
                    read_timeout=self._read_timeout
                )
                s3c = session.create_client(
                          "s3",
                          endpoint_url=self._server,
                          aws_access_key_id=self._credentials["accessKey"],
                          aws_secret_access_key=self._credentials["secretKey"],
                          config=config
                      )
                # call await s3c.__aenter__ : this is needed for newer versions
                # of aiobotocore
                s3c = await s3c.__aenter__()
                # add the connection to the connection pool
                self._conn_obj = s3aioFileObject._connection_pool.add(
                     s3c, self._server
                )
            except ClientError as e:
                raise IOException(
                    "Could not connect to S3 endpoint {} {}".format(
                        self._server, e)
                )

        if ('r' in self._mode and '*' not in self._path and
            '?' not in self._path):
            # if this is a read method then check the file exists
            response = await self._conn_obj.conn.list_objects_v2(
                Bucket=self._bucket,
                Prefix=self._path
            )
            exists = False
            for obj in response.get('Contents', []):
                if obj['Key'] == self._path:
                    exists = True
            if not exists:
                raise IOException(
                    "Object does not exist: {}/{}/{}".format(
                        self._server, self._bucket, self._path
                    )
                )
        if 'w' in self._mode:
            # if this is a write method then create a bytes array
            self._current_part = 1
        if 'a' in self._mode or '+' in self._mode:
            raise APIException(
                "Appending to files is not supported {}".format(self._path)
            )
        return True

    def detach(self):
        """Separate the underlying raw stream from the buffer and return it.
        Not supported in S3."""
        raise io.UnsupportedOperation

    async def _read_partial_file(self, part_num, part_size):
        s = int(part_num*part_size)
        e = int((part_num+1)*part_size)-1
        range_fmt = 'bytes={}-{}'.format(s,e)
        s3_object = await self._conn_obj.conn.get_object(
            Bucket = self._bucket,
            Key = self._path,
            Range = range_fmt
        )
        body = s3_object['Body']
        return await body.read()

    async def read(self, size=-1):
        """Read and return up to size bytes. For the S3 implementation the size
        can be used for RangeGet.  If size==-1 then the whole object is streamed
        into memory."""
        # read the object using the bucket and path already determined in
        # __init__, and using the connection object
        try:
            # get the file size first
            file_size = await self._getsize()
            if size== -1:
                range_start = 0
                range_end   = file_size
                range_size  = file_size
            else:
                range_start = self._seek_pos
                range_end   = self._seek_pos+size-1
                if range_end > file_size:
                    range_end = file_size-1
                range_size  = range_end-range_start+1

            # if multipart download is not supported
            if not self._multipart_download:
                # get the full file
                s3_object = await self._conn_obj.conn.get_object(
                    Bucket = self._bucket,
                    Key = self._path,
                )
                body = s3_object['Body']
                data = await body.read()
            # if the file is smaller than the MAXIMUM_PART_SIZE
            elif (range_size < self._part_size):
                # the requested range is the full file, it is fastest to
                # not specify the range
                if (range_start == 0 and range_size == file_size):
                    # get the full file
                    s3_object = await self._conn_obj.conn.get_object(
                        Bucket = self._bucket,
                        Key = self._path,
                    )
                # a portion of the file is requested
                else:
                    s3_object = await self._conn_obj.conn.get_object(
                        Bucket = self._bucket,
                        Key = self._path,
                        Range = 'bytes={}-{}'.format(
                            range_start, range_end
                        )
                    )
                body = s3_object['Body']
                data = await body.read()
            # multipart download version
            else:
                """Use range get to split up a file into the MAXIMUM_PART_SIZE
                and download each part asynchronously."""
                # calculate the number of necessary parts
                n_parts = int(range_size / self._part_size + 1)
                # don't go above the maximum number downloadable
                if n_parts > self._max_parts:
                    n_parts = self._max_parts
                # (re)calculate the download size
                part_size = float(range_size) / n_parts
                # create the tasks and assign the return data buffer
                tasks = []
                data_buf = io.BytesIO()

                for p in range(0, n_parts):
                    event_loop = asyncio.get_event_loop()
                    task = event_loop.create_task(self._read_partial_file(
                        p, part_size
                    ))
                    tasks.append(task)
                # wait for all the tasks to finish
                results = await asyncio.gather(*tasks)
                # read each chunk of data and write into the global buffer
                for r in results:
                    data_buf.write(r)
                    r = None            # indicate ready for garbage collection
                data_buf.seek(0)
                data = data_buf.read()

        except ClientError as e:
            raise IOException(
                "Could not read from object {} {}".format(self._path, e)
            )
        except AttributeError as e:
            self._handle_connection_exception(e)
        return data

    async def read1(self, size=-1):
        """Just call read."""
        return await self.read(size=size)

    async def readinto(self, b):
        """Read bytes into a pre-allocated, writable bytes-like object b and
        return the number of bytes read.
        In S3 the entire file is read into the bytesbuffer.  It is important
        that the bytesbuffer is big enough to hold the entire file."""
        # get the size of the file
        size = await self._getsize()
        b[:size] = await self.read(size)
        return size

    async def readinto1(self, b):
        """Just call readinto"""
        return await self.readinto(b)

    async def _multipart_upload_from_buffer(self):
        """Do a multipart upload from the buffer.
        There are three cases:
            1.  The size is exactly the same size as the MAXIMUM_PART_SIZE
            2.  The size is greater than the MAXIMUM_PART_SIZE
            3.  The size is multiple times greater than the MAX_UPLOAD_SIZE and
                requires splitting into smaller chunks
        """
        # check to see if bucket needs to be created
        if self._create_bucket:
            # check whether the bucket exists
            bucket_list = await self._get_bucket_list()
            if not self._bucket in bucket_list:
                await self._conn_obj.conn.create_bucket(Bucket=self._bucket)

        # if the current part is 1 we have to create the multipart upload
        if self._current_part == 1:
            response = await self._conn_obj.conn.create_multipart_upload(
                Bucket = self._bucket,
                Key = self._path
            )
            self._upload_id = response['UploadId']
            # we need to keep a track of the multipart info
            self._multipart_info = {'Parts' : []}

        # upload from a buffer - do we need to split into more than one
        # multiparts?
        new_buffer = []
        for buffer_part in range(0, len(self._buffer)):
            # is the current part of the buffer larger than the maximum
            # upload size? split if it is
            data_buf = self._buffer[buffer_part]
            data_len = data_buf.tell()
            if data_len >= self._part_size:
                data_buf.seek(0)
                data_pos = 0
                # split the file up
                while data_pos < data_len:
                    new_buffer.append(io.BytesIO())
                    # copy the data - don't overstep the buffer
                    if data_pos + self._part_size >= data_len:
                        sub_data = data_buf.read(data_len-data_pos)
                    else:
                        sub_data = data_buf.read(
                            self._part_size
                        )
                    new_buffer[-1].write(sub_data)
                    # increment to next
                    data_pos += self._part_size

                # free the old memory
                self._buffer[buffer_part].close()
            else:
                # copy the old buffer into a new one
                self._buffer[buffer_part].seek(0)
                new_buffer.append(io.BytesIO(self._buffer[buffer_part].read()))

        # close other buffers first
        for b in self._buffer:
            b.close()
        self._buffer = new_buffer

        tasks = []

        for buffer_part in range(0, len(self._buffer)):
            # seek in the BytesIO buffer to get to the beginning after the
            # writing
            self._buffer[buffer_part].seek(0)
            # upload here
            # schedule the uploads
            event_loop = asyncio.get_event_loop()
            task = event_loop.create_task(self._conn_obj.conn.upload_part(
                Bucket=self._bucket,
                Key=self._path,
                UploadId=self._upload_id,
                PartNumber=self._current_part + buffer_part,
                Body=self._buffer[buffer_part]
            ))
            tasks.append(task)

        # await the completion of the uploads
        res = await asyncio.gather(*tasks)
        for buffer_part in range(0, len(self._buffer)):
            # insert into the multipart info list of dictionaries
            part = res[buffer_part]
            self._multipart_info['Parts'].append(
                {
                    'PartNumber' : self._current_part + buffer_part,
                    'ETag' : part['ETag']
                }
            )

        # add the total number of uploads to the current part
        self._current_part += len(self._buffer)

        # reset all the byte buffers and their positions
        for buffer_part in range(0, len(self._buffer)):
            self._buffer[buffer_part].close()
        self._buffer = [io.BytesIO()]
        self._seek_pos = 0

    async def write(self, b):
        """Write the given bytes-like object, b, and return the number of bytes
        written (always equal to the length of b in bytes, since if the write
        fails an OSError will be raised).
        For the S3 file object we just write the file to a temporary bytearray
        and increment the seek_pos.
        This data will be uploaded to an object when .flush is called.
        """
        if "w" not in self._mode:
            raise APIException(
                "Trying to write to a read only file, where mode != 'w'."
            )
        try:
            # add to local, temporary bytearray
            size = len(b)
            self._buffer[-1].write(b)
            self._seek_pos += size
            # test to see whether we should do a multipart upload now
            # this occurs when the number of buffers is > the maximum number of
            # parts.  self._current_part is indexed from 1
            if (self._multipart_upload and
                self._seek_pos > self._part_size):
                if len(self._buffer) == self._max_parts:
                    await self._multipart_upload_from_buffer()
                else:
                    # add another buffer to write to
                    self._buffer.append(io.BytesIO())

        except ClientError as e:
            raise IOException(
                "Could not write to object {} {}".format(self._path, e)
            )
        except AttributeError as e:
            self._handle_connection_exception(e)

        return size

    async def close(self):
        """Flush and close this stream. This method has no effect if the file is
        already closed. Once the file is closed, any operation on the file (e.g.
        reading or writing) will raise a ValueError.

        As a convenience, it is allowed to call this method more than once; only
        the first call, however, will have an effect."""
        try:
            if not self._closed:
                # self.flush will upload the bytesarray to the S3 store
                await self.flush()
                s3aioFileObject._connection_pool.release(self._conn_obj)
                self._closed = True
        except AttributeError as e:
            self._handle_connection_exception(e)
        return True

    async def seek(self, offset, whence=io.SEEK_SET):
        """Change the stream position to the given byte offset. offset is
        interpreted relative to the position indicated by whence. The default
        value for whence is SEEK_SET. Values for whence are:

        SEEK_SET or 0 – start of the stream (the default); offset should be zero
                        or positive
        SEEK_CUR or 1 – current stream position; offset may be negative
        SEEK_END or 2 – end of the stream; offset is usually negative
        Return the new absolute position.

        Note: currently cannot seek when writing a file.

        """

        if self._mode == 'w':
            raise IOException(
                "Cannot seek within a file that is being written to."
            )

        size = await self._getsize()
        error_string = "Seek {} is outside file size bounds 0->{} for file {}"
        seek_pos = self._seek_pos
        if whence == io.SEEK_SET:
            # range check
            seek_pos = offset
        elif whence == io.SEEK_CUR:
            seek_pos += offset
        elif whence == io.SEEK_END:
            seek_pos = size - offset

        # range checks
        if (seek_pos >= size):
            raise IOException(error_string.format(
                seek_pos,
                size,
                self._path)
            )
        elif (seek_pos < 0):
            raise IOException(error_string.format(
                seek_pos,
                size,
                self._path)
            )
        self._seek_pos = seek_pos
        return self._seek_pos

    def seekable(self):
        """We can seek in s3 streams using the range get and range put features.
        """
        return True

    def tell(self):
        """Return True if the stream supports random access. If False, seek(),
        tell() and truncate() will raise OSError."""
        return self._seek_pos

    def fileno(self):
        """Return the underlying file descriptor (an integer) of the stream if
        it exists. An IOError is raised if the IO object does not use a file
        descriptor."""
        raise io.UnsupportedOperation

    async def flush(self):
        """Flush the write buffers of the stream.  This will upload the contents
        of the final multipart upload of self._buffer to the S3 store."""
        try:
            if 'w' in self._mode:
                # if the size is less than the MAXIMUM UPLOAD SIZE
                # then just write the data
                size = self._buffer[0].tell()
                if ((self._current_part == 1 and
                    size < self._part_size) or
                    not self._multipart_upload
                   ):
                    if self._create_bucket:
                        # check whether the bucket exists and create if not
                        bucket_list = await self._get_bucket_list()
                        if not self._bucket in bucket_list:
                            await self._conn_obj.conn.create_bucket(
                                Bucket=self._bucket
                            )
                    # upload the whole buffer - seek back to the start first
                    self._buffer[0].seek(0)
                    await self._conn_obj.conn.put_object(
                        Bucket=self._bucket,
                        Key=self._path,
                        Body=self._buffer[0].read(size)
                    )
                else:
                    # upload as multipart
                    await self._multipart_upload_from_buffer()
                    # finalise the multipart upload
                    await self._conn_obj.conn.complete_multipart_upload(
                        Bucket=self._bucket,
                        Key=self._path,
                        UploadId=self._upload_id,
                        MultipartUpload=self._multipart_info
                    )
            # clear the buffers
            for b in self._buffer:
                b.close()

        except AttributeError as e:
            self._handle_connection_exception(e)
        return True

    def readable(self):
        """Return True if the stream can be read from. If False, read() will
        raise IOError."""
        return 'r' in self._mode or '+' in self._mode

    async def readline(self, size=-1):
        """Read and return one line from the stream.
        If size is specified, at most size bytes will be read."""
        if 'b' in self._mode:
            raise APIException(
                "readline on a binary file is not permitted: {}".format(
                    self._uri)
                )
        # only read a set number of bytes if size is passed in, otherwise
        # read upto the file size
        if size == -1:
            size = self._getsize()

        # use the BytesIO readline methods
        if self.tell() == 0:
            buffer = await self.read(size=size)
            self._buffer[-1].write(buffer)
            self._buffer[-1].seek(0)

        line = self._buffer[-1].readline().decode().strip()
        return line

    async def readlines(self, hint=-1):
        """Read and return a list of lines from the stream. hint can be
        specified to control the number of lines read: no more lines will be
        read if the total size (in bytes/characters) of all lines so far exceeds
        hint."""
        if 'b' in self._mode:
            raise APIException(
                "readline on a binary file is not permitted: {}".format(
                    self._uri)
                )
        # read the entire file in and decode it
        lines = await self.read().decode().split("\n")
        return lines

    def truncate(self, size=None):
        """Not supported"""
        raise io.UnsupportedOperation

    def writable(self):
        """Return True if the stream supports writing. If False, write() and
        truncate() will raise IOError."""
        return 'w' in self._mode

    async def writelines(self, lines):
        """Write a list of lines to the stream."""
        # first check if the file is binary or not
        if 'b' in self._mode:
            raise APIException(
                "writelines on a binary file is not permitted: {}".format(
                    self._uri)
                )
        # write all but the last line with a line break
        for l in lines:
            await self.write((l+"\n").encode('utf-8'))
        return True

    async def glob(self):
        """Emulate glob on an open bucket.  The glob has been passed in via
        self._path, created on connection to the server and bucket."""
        # get the path string up to the wildcards
        try:
            pi1 = self._path.index("*")
        except ValueError:
            pi1 = len(self._path)
        try:
            pi2 = self._path.index("?")
        except ValueError:
            pi2 = len(self._path)
        pi = min(pi1, pi2)
        # using the prefix will cut down on the search space
        prefix = self._path[:pi]
        # get the wildcard
        wildcard = self._path[pi:]
        # set up the paginator
        paginator = self._conn_obj.conn.get_paginator("list_objects_v2")
        parameters = {
            'Bucket': self._bucket,
            'Prefix': prefix
        }
        page_iterator = paginator.paginate(**parameters)
        files = []
        async for page in page_iterator:
            for item in page.get('Contents', []):
                fname = item['Key']
                # check that it matches against wildcard
                if fnmatch(fname, wildcard):
                    files.append(item['Key'])
        return files
