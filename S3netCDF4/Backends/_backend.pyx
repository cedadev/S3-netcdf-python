__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

class Backend(object):
    """Base class for the S3-netcdf-python backends.
       This class should not be used, only classes that inherit from this class.
    """
    def __init__(self):
        raise NotImplementedError

    def connect(self, url, credentials):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def get_id(self):
        return ("Backend")

    def upload(self, conn, filename):
        raise NotImplementedError

    def download(self, conn, filename):
        raise NotImplementedError

    def remove(self, conn, filename):
        raise NotImplementedError

    def open(self):
        raise NotImplementedError

    def list_root(self):
        raise NotImplementedError

    def get_size(self):
        raise NotImplementedError

    def exists(self, fid):
        raise NotImplementedError

    def get_partial(self, fid, start, stop):
        raise NotImplementedError
