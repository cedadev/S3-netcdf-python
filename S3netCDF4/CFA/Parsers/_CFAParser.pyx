#!python
#cython: language_level=3

__copyright__ = "(C) 2020 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

"""
   Collection of functions that parse files with embedded CFA metadata and
   return a hierarchy of objects instantiated from the _CFAClasses.
   See the class definitions and documentation in _CFAClasses.pyx for this
   hierarchy.

   See:
     http://www.met.reading.ac.uk/~david/cfa/0.4/index.html
   for the specification of the CFA conventions.

   s3netCDF-python uses an updated version (v0.5) of the CFA conventions which,
   rather than writing the partition information to a netCDF attribute as a
   string, writes the partition information to variables inside a group.
"""

class CFA_Parser(object):
    """Base class for CFA Parser - pure abstract so raise an exception."""
    def __init__(self):
        raise NotImplementedError

    def read(self, input_object):
        raise NotImplementedError

    def write(self, cfa_dataset, output_object):
        raise NotImplementedError

    def is_file(self, input_object):
        raise NotImplementedError
