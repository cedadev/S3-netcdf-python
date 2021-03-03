#!python
#cython: language_level=3

__copyright__ = "(C) 2019-2021 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

"""
A very simple connection pool for S3netCDF.  This allows connections to be
maintained to (for example) a AWS or object store.  The pool allows for the
following behaviour:
o. The backend File Object makes a request for a connection.  The pool either
   returns a connection or None, if no connections are available or if all
   available connections are locked
o. If None is returned, the backend is expected to make a connection and add it
   to the pool
o. When connections are added they are locked and they can later be released so
   that they can be reused without having to re-establish the connection.
o. When a connection is closed it is removed from the pool.
"""
from S3netCDF4._Exceptions import APIException

class ConnectionObject(object):
    """A small class to hold connection information."""
    def __init__(self, conn=None, uri="", available=False):
        self.conn = conn
        self.uri = uri
        self.conn_refs = 0

    def __str__(self):
        return "{} : ({})".format(self.uri, self.conn_refs)

class ConnectionPool(object):
    """Connection pool for S3 netCDF.  Stores connections to external storage in
    a pool, and keeps track of how many connections have been made to them.
    This maintains connections to servers to enhance performance by not
    incurring the time penalty of establishing a connection
    """

    def __init__(self):
        self._connection_pool = {}

    def add(self, conn, conn_uri):
        """Add a connection to the ConnectionPool.
        Args:
            conn    : the connection, e.g. a botocore client
            conn_uri: the uri of the connection, e.g. URL address
        Returns:
            None
        """
        # Use the conn_uri as the key to the dictionary
        # If the conn_uri already exists in the connection pool then increase
        # the reference count
        # If it doesn't then create the connection with a reference count of
        # zero
        if conn_uri in self._connection_pool:
            conn_obj = self._connection_pool[conn_uri]
            conn_obj.conn_refs += 1
        else:
            conn_obj = ConnectionObject(conn, conn_uri)
            conn_obj.conn_refs = 1
            self._connection_pool[conn_uri] = conn_obj
        return conn_obj

    def get(self, conn_uri):
        """Get a connection from the ConnectionPool.
        Args:
            conn_uri: the uri of the connection, e.g. URL address
        Returns:
            ConnectionObject | None
        """
        # Use the conn_uri to the dictionary to try to find a free connection
        if conn_uri in self._connection_pool:
            conn_obj = self._connection_pool[conn_uri]
            conn_obj.conn_refs += 1
            return conn_obj

        return None

    def release(self, conn_obj):
        """Release the connection for the connection uri.
        Args:
            conn : the ConnectionObject created in add"""
        if not conn_obj.uri in self._connection_pool:
            raise APIException(
                "Connection is not in the connection pool {}".format(
                    conn_obj.uri
                )
            )
        conn_obj.conn_refs -= 1
