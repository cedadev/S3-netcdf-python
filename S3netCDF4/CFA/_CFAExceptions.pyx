#!python
#cython: language_level=3

"""Exceptions for the _CFAClasses"""

class CFAError(BaseException):
    pass

class CFAGroupError(CFAError):
    pass

class CFADimensionError(CFAError):
    pass

class CFAVariableError(CFAError):
    pass

class CFAVariableIndexError(CFAError, IndexError):
    pass

class CFAPartitionError(CFAError):
    pass

class CFAPartitionIndexError(CFAError, IndexError):
    pass

class CFASubArrayError(CFAError):
    pass
