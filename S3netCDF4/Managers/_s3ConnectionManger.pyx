"""
Connection management for S3netCDF.  Operation:
o. Connections are made via the backends and kept in a "Connection Pool".
o. When connections are made they are locked and then can be released so that
   they can be reused without having to re-establish the connection.
o. When a connection is closed it is removed from the pool.
"""

__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

from _s3Exceptions import s3IOException, s3APIException
from S3netCDF4 import Backends

class s3Connection(object):
    """Object to store in the connection pool."""
    def __init__(self, alias, backend_name, conn):
        """Initialise, store the connection object.  Available is false."""
        self._alias = alias
        self._backend_name = backend_name
        self._conn = conn
        self._available = False

    def alias(self):
        """Get the alias for the connection."""
        return self._alias

    def backend(self):
        """Connection type, i.e. which backend does the connection use?"""
        return self._backend

    def release(self):
        """Release the connection object to allow it to be reused."""
        self._available = True

    def lock(self):
        """Lock the connection object to prevent it from being reused."""
        self._available = False

    def close(self):
        """Close the connection pointed at by the _conn object."""
        # get the backend - this should always return as checking is performed
        # when the slConnection object is created
        backend = Backends.get_backend_from_id(self._backend_name)
        backend.close(self._conn)
        # mark the connection as unavailable
        self._available = False

    def get(self):
        """Return the underlying connection object, e.g. FTP lib object,
        boto3 S3 client, etc."""
        return self._conn

    def available(self):
        """Is the connection available? (unlocked)"""
        return self._available

    def locked(self):
        """Is the connection not available? (locked)"""
        return not self._available


class s3ConnectionManager(object):
    """Connection manager for Sem-SL.  Stores connections in a connection pool
    and persists connections to remove the overhead of establishing the
    connection for every fragment when writing / reading."""

    def __init__(self, sl_config):
        """Initialise by passing in the config dictionary."""
        self._sl_config = sl_config
        self._connection_pool = {}

    def open(self, endpoint):
        """Open or retrieve a connection to the connection_uri.  Operation:
        o. Identify what type of connection to make and read the details from
        the config.
        o. Determine whether there is a spare matching connection in the
        connection pool and return if there is.
        o. Create a new connection if there isn't and add to the connection pool
        setting available to false.
        """
        # set the host name to not-found
        host_name = None
        try:
            hosts = self._sl_config["hosts"]
            for h in hosts:
                if endpoint in hosts[h]['alias']:
                    host_name = h
        except Exception as e:
            raise s3IOException("Error in config file {} {}".format(
                                self._sl_config["filename"],
                                e))

        # check whether the url was found in the config file
        if host_name == None:
            raise s3IOException(("Error {} was not found as an alias in the"
                                 " user config file {} ").format(
                                   endpoint,
                                   self._sl_config["filename"])
                                )

        # Now we have the host_name, get the backend, url and required
        # credentials from the config dictionary
        try:
            host_config = self._sl_config["hosts"][host_name]
            url_name = host_config['url']
            backend_name = host_config['backend']
            credentials = host_config['required_credentials']
        except:
            raise s3IOException("Error in config file {}".format(
                                   self._s3_user_config["filename"])
                               )
        # Check that the desired backend is in the list
        if backend_name not in Backends.get_backend_ids():
            raise s3IOException("Error backend {} not found or not supported".format(
                                   backend_name)
                               )
        # Otherwise, try to create the backend
        try:
            backend = Backends.get_backend_from_id(backend_name)()
        except Exception as e:
            raise s3IOException("Error creating backend {} {}".format(
                                backend_name, e)
                               )

        # try to find a free connection to this endpoint
        create_connection = True
        if endpoint in self._connection_pool:
            # loop over to see if there are any available connections
            for sl_conn in self._connection_pool[endpoint]:
                if sl_conn.available():
                    # a connection is available so lock it
                    sl_conn.lock()
                    # we don't need to create a connection, we have this one
                    create_connection = False
                    # sl_conn will be returned
                    break

        if create_connection:
            # now try to connect to the backend
            try:
                conn = backend.connect(url_name, credentials)
            except Exception as e:
                raise s3IOException("Error connecting to backend {} {} {}".format(
                                    backend_name, url_name, e)
                                   )

            # add the connection to the connection pool
            sl_conn = slConnection(host_name, backend_name, conn)
            # indicate we are using the connection
            sl_conn.lock()
            # add to the pool
            if endpoint in self._connection_pool:
                self._connection_pool[endpoint].append(sl_conn)
            else:
                self._connection_pool[endpoint] = [sl_conn]
        return sl_conn

    def total_connections(self, endpoint):
        """Get the number of connections to a particular host / alias."""
        if endpoint in self._connection_pool:
            return len(self._connection_pool[endpoint])
        else:
            return 0

    def open_connections(self, endpoint):
        """Get the number of connections that are in use to a particular
        host / alias."""
        if endpoint in self._connection_pool:
            total = 0
            for sl_conn in self._connection_pool[endpoint]:
                if sl_conn.locked():
                    total += 1
            return total
        else:
            return 0
