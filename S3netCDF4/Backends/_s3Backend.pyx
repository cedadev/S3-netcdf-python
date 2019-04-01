__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import boto3.client
from botocore.client import Config
from botocore.exceptions import ClientError

from S3netCDF4._s3Exceptions import s3IOException, s3APIException
from _backend import Backend

class s3Backend(Backend):
    """Class for the S3 backend for S3-netCDF-python.
       This class uses boto3 to connect to a S3 object store / AWS.
    """

    def __init__(self):
        pass

    def connect(self, url, credentials):
        """Create connection to object store / AWS, using the supplied
        credentials."""
        #config = Config(connect_timeout=10, retries={'max_attempts': 0})
        try:
            s3c = boto3.client("s3", endpoint_url=url,
                               aws_access_key_id=credentials['access_key'],
                               aws_secret_access_key=credentials['secret_key'])
        except Exception as e:
            raise s3IOException("Could not connect to S3 endpoint {} {}",
                                url, e)
        return s3c

    def close(self):
        """Static function to close a connection passed in."""
        # boto3 doesn't have a close connection method!
        pass

    def get_id(self):
        """Return an unique id for this backend"""
        return ("s3Backend")

    def upload(self, conn, local_filename, remote_filename):
        """Uploads file to backend
        Args:
            conn: the connection to the backend
            local_filename: the filename of the file to be uploaded
            remote_filename: the location on the remote storage to upload the
                file to.
        Returns:
            None
        """
        conn.upload_file(cloc, bucket, fname)

    def download(self, conn, remote_filename, local_filename):
        """Downloads file from remote storage to the local filesystem
        Args:
            conn: the connection to the backend
            remote_filename: the location on the remote storage of the file to
                download.
            local_filename: the target location, on the local filesystem, to
                download the file to.
        """
        try:
            conn.download_file(bucket, key, cacheloc)
        except ClientError:
            raise s3IOException('Cannot download object: File not found')

    def remove(self, conn, remote_filename):
        """Deletes a file from the backend.
        Args:
            conn: the connection to the backend
            remote_filename: the location on the remote storage of the file to
                be deleted.
        Returns:
            None
        """

        try:
            conn.delete_object(Bucket=bucket, Key=key)
        except ClientError:
            raise s3IOException(
                "Cannot remove object from backend: File not found"
            )

    def create_root(self, conn, path):
        """Creates the root path to where the remote files should be stored.
        Args:
            conn: the connection to the backend
            path: the location on the remote storage to create the root path
        Returns:
            None
        """
        conn.create_bucket(Bucket=bucket)

    def list_roots(self, conn, path=None):
        """List all the root paths on the remote filesystem, that reside under
        the path.
        Args:
            conn: the connection to the backend
            path: the location on the remote storage to list root paths below
        Returns:
            None
        """

        return conn.list_buckets()['Buckets']

    def get_size(self, conn, remote_filename):
        """Get the size of a file on the remote filesystem
        Args:
            conn: the connection to the backend
            remote_filename: the location on the remote storage of the file to
                get the size of.
        Returns:
            None
        """

        response = conn.head_object(Bucket=bucket,Key=fname)
        return response['ContentLength']

    def exists(self, conn, remote_filename):
        """Determine whether a file or root path exists on the remote storage
        Args:
            conn: the connection to the backend
            remote_filename: the location to check whether either a filename or
                a root_path exists.
        Returns:
            None
        """
        try:
            conn.head_object(Bucket=bucket, Key=fname)
            return True
        except:
            return False

    def get_partial(self, conn, remote_filename, start, stop):
        """
        Returns a partial file defined by bytes
        :param start: start byte
        :param stop: stop byte
        :return:
        """
        s3_object  = conn.get_object(Bucket=bucket,Key=fid, Range='bytes={}-{}'.format(start,stop))
        body = s3_object['Body']
        return body.read().decode('utf8','replace').strip()
