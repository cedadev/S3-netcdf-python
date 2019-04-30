__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import io
from urllib.parse import urlparse, urljoin, urlsplit

import boto3
from botocore.exceptions import ClientError

from S3netCDF4.Managers._ConnectionPool import ConnectionPool
from S3netCDF4._Exceptions import APIException, IOException

class s3FileObject(io.BufferedIOBase):
    """Custom file object class, inheriting from Python io.Base, to read from
    an S3 object store / AWS cloud storage."""

    """Maximum upload size for the object.  We can split large objects into
    multipart upload.  No parallelism, beyond what the boto3 library implements,
    at the moment."""
    MAXIMUM_UPLOAD_SIZE = 10 * 1024 * 1024   # set at 50MB, minimum multipart
                                             # upload is 5MB

    """Static connection pool object - i.e. shared across the file objects."""
    _connection_pool = ConnectionPool()

    def _get_server_bucket_object(uri):
        """Get the server name from the URI"""
        # First split the uri into the network location and path, and build the
        # server
        url_p = urlparse(uri)
        # check that the uri contains a scheme and a netloc
        if url_p.scheme == '' or url_p.netloc == '':
            raise APIException(
                "URI supplied to s3FileObject is not well-formed: {}". format(uri)
            )
        server = url_p.scheme + "://" + url_p.netloc
        split_path = url_p.path.split("/")
        bucket = split_path[1]
        path = "/".join(split_path[2:])
        return server, bucket, path

    def __init__(self, uri, credentials, mode='r', create_bucket=True,
                 buffer_size=MAXIMUM_UPLOAD_SIZE):
        """Initialise the file object by creating or reusing a connection in the
        connection pool."""
        # get the server, bucket and the key from the endpoint url
        self._server, self._bucket, self._path = s3FileObject._get_server_bucket_object(uri)
        self._closed = False        # set the file to be not closed
        self._mode = mode
        self._seek_pos = 0
        self._buffer_size = buffer_size
        self._buffer_pos  = 0
        self._buffer = bytearray()
        self._credentials = credentials
        self._create_bucket = create_bucket

    def __enter__(self):
        """Create the connection on an enter."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Close the file on the exit of a with statement, or by the garbage
        collector removing the object."""
        self.close()
        # check for any exceptions
        if exc_type is not None:
            return False
        return True

    def _getsize(self):
        # Use content length in the head object to determine how the size of
        # the file / object
        try:
            response = self._conn_obj.conn.head_object(
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

    def _get_bucket_list(self):
        # get the names of the buckets in a list
        try:
            bl = self._conn_obj.conn.list_buckets()['Buckets'] # this returns a dict
            bucket_list = [b['Name'] for b in bl]
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

    def connect(self):
        """Connect to the s3 server with the details passed in via the __init__
        method."""
        # if the connection returns None then either there isn't a connection to
        # the server in the pool, or there is no connection that is available
        self._conn_obj = s3FileObject._connection_pool.get(self._server)
        if self._conn_obj is None:
            try:
                s3c = boto3.client(
                          "s3",
                          endpoint_url=self._server,
                          aws_access_key_id=self._credentials["accessKey"],
                          aws_secret_access_key=self._credentials["secretKey"]
                      )
                # add the connection to the connection pool
                self._conn_obj = s3FileObject._connection_pool.add(
                    s3c, self._server
                )
            except ClientError as e:
                raise IOException(
                    "Could not connect to S3 endpoint {} {}".format(
                        self._server, e)
                )
        # if this is a write method then create a bytes array
        if self._mode == 'w':
            self._current_part = 1
        elif self._mode == 'a' or self._mode == '+':
            raise APIException(
                "Appending to files is not supported {}".format(self._path)
            )

    def detach(self):
        """Separate the underlying raw stream from the buffer and return it.
        Not supported in S3."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read and return up to size bytes. For the S3 implementation the size
        can be used for RangeGet.  If size==-1 then the whole object is streamed
        into memory."""
        # read the object using the bucket and path already determined in
        # __init__, and using the connection object
        try:
            if size== -1:
                s3_object = self._conn_obj.conn.get_object(
                    Bucket = self._bucket,
                    Key = self._path
                )
                body = s3_object['Body']
            else:
                # do the partial / range get version, and increment the seek
                # pointer
                range_end = self._seek_pos+size-1
                file_size = self._getsize()
                if range_end >= file_size:
                    range_end = file_size-1
                s3_object = self._conn_obj.conn.get_object(
                    Bucket = self._bucket,
                    Key = self._path,
                    Range = 'bytes={}-{}'.format(
                        self._seek_pos, range_end
                    )
                )
                self._seek_pos += size
                body = s3_object['Body']
        except ClientError as e:
            raise IOException(
                "Could not read from object {} {}".format(self._path, e)
            )
        except AttributeError as e:
            self._handle_connection_exception(e)
        return body.read()

    def read1(self, size=-1):
        """Just call read."""
        return self.read(size=size)

    def readinto(self, b):
        """Read bytes into a pre-allocated, writable bytes-like object b and
        return the number of bytes read.
        In S3 the entire file is read into the bytesbuffer.  It is important
        that the bytesbuffer is big enough to hold the entire file."""
        # get the size of the file
        size = self._getsize()
        b[:size] = self.read(size)
        return size

    def readinto1(self, b):
        """Just call readinto"""
        return self.readinto(b)

    def write(self, b):
        """Write the given bytes-like object, b, and return the number of bytes
        written (always equal to the length of b in bytes, since if the write
        fails an OSError will be raised).
        For the S3 file object we just write the file to a temporary bytearray
        and increment the seek_pos.
        This data will be uploaded to an object when .flush is called.
        """
        if self._mode != "w":
            raise APIException(
                "Trying to write to a read only file, where mode != 'w'."
            )
        try:
            # add to local, temporary bytearray
            size = len(b)
            self._buffer[self._seek_pos:self._seek_pos+size] = b
            self._seek_pos += size
            self._buffer_pos += size
            # test to see whether we should do a multipart upload now
            if self._buffer_pos > self._buffer_size:
                # check to see if bucket needs to be created
                if self._create_bucket:
                    # check whether the bucket exists
                    bucket_list = self._get_bucket_list()
                    if not self._bucket in bucket_list:
                        self._conn_obj.conn.create_bucket(Bucket=self._bucket)
                # if the current part is 0 we have to create the multipart upload
                if self._current_part == 1:
                    response= self._conn_obj.conn.create_multipart_upload(
                        Bucket = self._bucket,
                        Key = self._path
                    )
                    self._upload_id = response['UploadId']
                    # we need to keep a track of the multipart info
                    self._multipart_info = {'Parts' : []}

                # upload here
                part = self._conn_obj.conn.upload_part(
                    Bucket=self._bucket,
                    Key=self._path,
                    UploadId=self._upload_id,
                    PartNumber=self._current_part,
                    Body=self._buffer
                )
                # insert into the multipart info list of dictionaries
                self._multipart_info['Parts'].append(
                    {
                        'PartNumber' : self._current_part,
                        'ETag' : part['ETag']
                    }
                )
                # reset bytearray and position in it
                self._buffer_pos = 0
                self._buffer = bytearray()
                self._current_part += 1
        except ClientError as e:
            raise IOException(
                "Could not write to object {} {}".format(self._path, e)
            )
        except AttributeError as e:
            self._handle_connection_exception(e)

        return size

    def close(self):
        """Flush and close this stream. This method has no effect if the file is
        already closed. Once the file is closed, any operation on the file (e.g.
        reading or writing) will raise a ValueError.

        As a convenience, it is allowed to call this method more than once; only
        the first call, however, will have an effect."""
        try:
            if not self._closed:
                # self.flush will upload the bytesarray to the S3 store
                self.flush()
                s3FileObject._connection_pool.release(self._conn_obj)
                self._closed = True
        except AttributeError as e:
            self._handle_connection_exception(e)

    def seek(self, offset, whence=io.SEEK_SET):
        """Change the stream position to the given byte offset. offset is
        interpreted relative to the position indicated by whence. The default
        value for whence is SEEK_SET. Values for whence are:

        SEEK_SET or 0 – start of the stream (the default); offset should be zero
                        or positive
        SEEK_CUR or 1 – current stream position; offset may be negative
        SEEK_END or 2 – end of the stream; offset is usually negative
        Return the new absolute position.
        """
        size = self._getsize()
        error_string = "Seek is outside file size bounds {}"
        if whence == io.SEEK_SET:
            # range check
            if (offset >= size):
                raise IOException(error_string.format(self._path))
            self._seek_pos = offset
        elif whence == io.SEEK_CUR:
            if (self._seek_pos + offset >= size):
                raise IOException(error_string.format(self._path))
            self._seek_pos += offset
        elif whence == io.SEEK_END:
            seek_pos = size - offset
            if seek_pos < 0:
                raise IOException(error_string.format(self._path))
            else:
                self._seek_pos = seek_pos

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

    def flush(self):
        """Flush the write buffers of the stream.  This will upload the contents
        of the final multipart upload of self._buffer to the S3 store."""
        try:
            if self._mode == 'w':
                # we have to check whether we are in a multipart upload or not
                if self._current_part == 1:
                    if self._create_bucket:
                        # check whether the bucket exists
                        bucket_list = self._get_bucket_list()
                        if not self._bucket in bucket_list:
                            self._conn_obj.conn.create_bucket(Bucket=self._bucket)

                    # just a regular upload
                    self._conn_obj.conn.put_object(
                        Bucket=self._bucket,
                        Key=self._path,
                        Body=self._buffer
                    )
                else:
                    # multipart upload
                    # upload the last part of the multipart upload here
                    part = self._conn_obj.conn.upload_part(
                        Bucket=self._bucket,
                        Key=self._path,
                        UploadId=self._upload_id,
                        PartNumber=self._current_part,
                        Body=self._buffer
                    )
                    # insert into the multipart info list of dictionaries
                    self._multipart_info['Parts'].append(
                        {
                            'PartNumber' : self._current_part,
                            'ETag' : part['ETag']
                        }
                    )
                    # reset bytearray and position in it - just incase the stream is
                    # used again - it will overwrite this time, though!
                    self._seek_pos = 0
                    self._buffer = bytearray()
                    # finalise the multipart upload
                    self._conn_obj.conn.complete_multipart_upload(
                        Bucket=self._bucket,
                        Key=self._path,
                        UploadId=self._upload_id,
                        MultipartUpload=self._multipart_info
                    )
        except AttributeError as e:
            self._handle_connection_exception(e)

    def readable(self):
        """Return True if the stream can be read from. If False, read() will
        raise IOError."""
        return 'r' in self._mode or '+' in self._mode

    def readline(self, size=-1):
        """Read and return one line from the stream.
        If size is specified, at most size bytes will be read."""
        if 'b' in self._mode:
            raise APIException(
                "readline on a binary file is not permitted: {}".format(
                    self._uri)
                )
        # only read a set number of bytes if size is passed in, otherwise
        # read upto the buffer size
        if size == -1:
            size = self._buffer_size

        # read the file into the buffer until a new line is found
        keep_reading = True
        while keep_reading:
            if self._buffer_pos > self._getsize() - size:
                return ""
            buffer_ascii = self._buffer.decode()
            if "\n" in buffer_ascii:
                line = buffer_ascii.split("\n")[0]
                self._buffer = self._buffer[len(line)+1:]
                self._buffer_pos += len(line)
                keep_reading = False
            else:
                # add to the buffer
                buffer = self.read(size=size)
                self._buffer += buffer
        return line

    def readlines(self, hint=-1):
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
        lines = self.read().decode().split("\n")
        return lines

    def truncate(self, size=None):
        """Not supported"""
        raise io.UnsupportedOperation

    def writable(self):
        """Return True if the stream supports writing. If False, write() and
        truncate() will raise IOError."""
        return True

    def writelines(self, lines):
        """Write a list of lines to the stream."""
        # first check if the file is binary or not
        if 'b' in self._mode:
            raise APIException(
                "writelines on a binary file is not permitted: {}".format(
                    self._uri)
                )
        # write all but the last line with a line break
        for l in lines:
            self.write((l+"\n").encode('utf-8'))
