#!python
#cython: language_level=3

__copyright__ = "(C) 2020 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

# Exception classes to indicate they come from the s3 component of the library
class IOException(BaseException):
    pass

class MemoryException(BaseException):
    pass

class APIException(BaseException):
    pass
