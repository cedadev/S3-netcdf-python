#!python
#cython: language_level=3

__copyright__ = "(C) 2020 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

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

class CFAParserError(CFAError):
    pass
