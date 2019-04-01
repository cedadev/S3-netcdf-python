"""
File management for S3netCDF.  Operation:
o. Files are opened from a single URL
o. The manager determines, using the ConfigManager and the ConnectionManager,
   which backend to use to open the file
o. A file object is returned, containing the connection to the file, and the
   backend the file is located on
o. Reading / writing to a file can then be performed by operations on the file
   object, which then calls the method on the backend
"""

__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
