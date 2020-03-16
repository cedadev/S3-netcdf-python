#!python
#cython: language_level=3

# Exception classes to indicate they come from the s3 component of the library
class IOException(BaseException):
    pass


class APIException(BaseException):
    pass
