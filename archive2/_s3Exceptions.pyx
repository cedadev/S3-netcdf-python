# Exception classes to indicate they come from the s3 component of the library
class s3IOException(BaseException):
    pass


class s3APIException(BaseException):
    pass