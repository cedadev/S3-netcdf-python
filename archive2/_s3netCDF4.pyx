"""
S3 enabled version of netCDF4.
Allows reading and writing of netCDF files to object stores via AWS S3.

Requirements: minio, psutil, netCDF4, Cython

Author: Neil Massey
Date:   10/07/2017
"""

# This module inherits from the standard netCDF4 implementation
# import as UniData netCDF4 to avoid confusion with the S3 module
import netCDF4._netCDF4 as netCDF4
from _s3netCDFIO import get_netCDF_file_details
from _s3Exceptions import *
from _CFAClasses import *

import os

# these are class attributes that only exist at the python level (not in the netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 '_file_details'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member functions to allow the
       read and write of netCDF file to an object store accessed via an AWS S3 HTTP API.
    """

    def __init__(self, filename, mode='r', clobber=True, format='DEFAULT',
                 diskless=False, persist=False, keepweakref=False, memory=None,
                 **kwargs):
        """
        **`__init__(self, filename, mode="r", clobber=True, diskless=False,
           persist=False, keepweakref=False, format='NETCDF4')`**

        `S3netCDF4.Dataset` constructor
        See `netCDF4.Dataset` for full details of all the keywords
        """

        # we've passed all the details of detecting whether this is an S3 or POSIX file to the function
        # get_netCDFFilename(filename).  Diskless == always_stream

        # get the file details
        self._file_details = get_netCDF_file_details(filename, mode, diskless)

        # switch on the read / write / append mode
        if mode == 'r' or mode == 'a' or mode == 'r+':             # read
            # we have different defaults for the read and write - as the read automatically handles CFA files
            # from the netCDF attribute
            if format == 'DEFAULT':
                format = 'NETCDF4'

            # check whether the memory has been set from get_netCDF_file_details (i.e. the file is streamed to memory)
            if self._file_details.memory != None or diskless:
                # we have to first create the dummy file (name held in file_details.memory) - check it exists before creating it
                if not os.path.exists(self._file_details.filename):
                    temp_file = netCDF4.Dataset(self._file_details.filename, 'w', format=self._file_details.format).close()
                # create the netCDF4 dataset from the data, using the temp_file
                netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=True, persist=False,
                                         keepweakref=keepweakref, memory=self._file_details.memory, **kwargs)
            else:
                # not in memory but has been streamed to disk
                netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=False, persist=persist,
                                         keepweakref=keepweakref, memory=None, **kwargs)

            # check if file is a CFA file, for standard netCDF files
            try:
                cfa = "CFA" in self.getncattr("Conventions")
            except:
                cfa = False

            if cfa:
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.Parse(self)
                # recreate the variables as s3Variables and attach the cfa data
                for v in self.variables:
                    if v in self._file_details.cfa_file.variables:
                        self.variables[v] = s3Variable(self.variables[v], self._file_details.cfa_file.variables[v])

            else:
                self._file_details.cfa_file = None

        elif mode == 'w':           # write
            # check the format for writing - allow CFA4 in arguments and default to it as well
            # we have different defaults for read and write - write defaults to distributed files across objects
            if format == 'DEFAULT':
                format = 'CFA4'

            if format == 'CFA4':
                format = 'NETCDF4'
                self._file_details.cfa_file = True
            elif format == 'CFA3':
                format = 'NETCDF3'
                self._file_details.cfa_file = True
            else:
                self._file_details.cfa_file = False

            # for writing a file, all we have to do is check that the containing folder in the cache exists
            if self._file_details.filename != "":   # first check that it is not a diskless file
                cache_dir = os.path.dirname(self._file_details.filename)
                # create all the sub folders as well
                if not os.path.isdir(cache_dir):
                    os.makedirs(cache_dir)

            netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber, format=format,
                                     diskless=diskless, persist=persist, keepweakref=keepweakref, memory=None,
                                     **kwargs)
        else:
            # no other modes are supported
            raise s3APIException("Mode " + mode + " not supported.")


    def __enter__(self):
        """Allows objects to be used with a `with` statement."""
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """Allows objects to be used with a `with` statement."""
        self.close()


class s3Variable(object):
    """
      Inherit the UniData netCDF4 Variable class and override some key methods so as to enable CFA and S3 functionality
    """

    def __init__(self, nc_var, cfa_var):
        """Keep a reference to the nc_var and cfa_var"""
        self.nc_var = nc_var
        self.cfa_var = cfa_var


    """There now follows a long list of functions, matching the netCDF4.Variable interface.
       The only functions we need to override are __getitem__ and __setitem__, so as to
       use the CFA information in cfa_var."""

    def __repr__(self):
        return unicode(self.nc_var).encode('utf-8')

    def __array__(self):
        return self.nc_var.__array__()

    def __unicode__(self):
        return self.nc_var.__unicode__()

    @property
    def name(self):
        return self.nc_var.name
    @name.setter
    def name(self, value):
        raise AttributeError("name cannot be altered")

    @property
    def datatype(self):
        return self.nc_var.datatype

    @property
    def shape(self):
        return self.nc_var.shape
    @shape.setter
    def shape(self, value):
        raise AttributeError("shape cannot be altered")

    @property
    def size(self):
        return self.nc_var.size

    @property
    def dimensions(self):
        return self.nc_var.dimensions
    @dimensions.setter
    def dimensions(self, value):
        raise AttributeError("dimensions cannot be altered")

    def group(self):
        return self.nc_var.group()

    def ncattrs(self):
        return self.nc_var.ncattrs()

    def setncattr(self, name, value):
        self.nc_var.setncattr(name, value)

    def setncattr_string(self, name, value):
        self.nc_var.setncattr(name, value)

    def setncatts(self, attdict):
        self.nc_var.setncatts(attdict)

    def getncattr(self, name, encoding='utf-8'):
        return self.nc_var.getncattr(name, encoding)

    def delncattr(self, name):
        self.nc_var.delncattr(name)

    def filters(self):
        return self.nc_var.filters()

    def endian(self):
        return self.nc_var.endian()

    def chunking(self):
        return self.nc_var.chunking()

    def get_var_chunk_cache(self):
        return self.nc_var.get_var_chunk_cache()

    def set_var_chunk_cache(self, size=None, nelems=None, preemption=None):
        self.nc_var.set_var_chunk_cache(size, nelems, preemption)

    def __delattr__(self, name):
        self.nc_var.__delattr__(name)

    def __setattr__(self, name, value):
        self.nc_var.__setattr(name, value)

    def __getattr__(self, name):
        print "!"
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name.startswith('__') and name.endswith('__'):
            # if __dict__ requested, return a dict with netCDF attributes.
            if name == '__dict__':
                names = self.nc_var.ncattrs()
                values = []
                for name in names:
                    values.append(_get_att(self.nc_var.group(), self.nc_var._varid, name))
                return OrderedDict(zip(names, values))
            else:
                raise AttributeError
        elif name in netCDF4._private_atts:
            return self.nc_var.__dict__[name]
        else:
            return self.nc_var.getncattr(name)

    def renameAttribute(self, oldname, newname):
        self.nc_var.renameAttribute(oldname, newname)

    def __len__(self):
        return self.nc_var.__len__()

    def assignValue(self, val):
        self.nc_var.assignValue(val)

    def getValue(self):
        return self.nc_var.getValue()

    def set_auto_chartostring(self, chartostring):
        self.nc_var.set_auto_chartostring(chartostring)

    def set_auto_maskandscale(self, maskandscale):
        self.nc_var.set_auto_maskandscale(maskandscale)

    def set_auto_scale(self, scale):
        self.nc_var.set_auto_scale(scale)

    def set_auto_mask(self, mask):
        self.nc_var.set_auto_mask(mask)

    def __reduce__(self):
        return self.nc_var.__reduce__()

    def __getitem__(self, elem):
        raise NotImplementedError

    def __setitem__(self, elem, data):
        raise NotImplementedError