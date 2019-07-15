"""
S3 enabled version of netCDF4.
Allows reading and writing of netCDF files to object stores via AWS S3.

Requirements: botocore / aiobotocore, psutil, netCDF4, Cython

Author:  Neil Massey
Date:    10/07/2017
Updated: 13/05/2019
"""

import numpy as np

# This module inherits from the standard UniData netCDF4 implementation
# import as netCDF4 to avoid confusion with the S3netCDF4 module
import netCDF4._netCDF4 as netCDF4

from _Exceptions import *
from S3netCDF4.CFA._CFAClasses import *
from S3netCDF4.CFA._CFAFunctions import *
from S3netCDF4.CFA.Parsers._CFAnetCDFParser import CFA_netCDFParser
from S3netCDF4.Managers._FileManager import FileManager

# these are class attributes that only exist at the python level (not in the
# netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 '_file_manager', '_file_object', 'file_object', '_mode',
 '_interpret_netCDF_filetype',
 '_cfa_dataset', '_cfa_group', '_cfa_var', '_cfa_dim'
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dimension(netCDF4.Dimension):
    """
       Inherit the UniData netCDF4 Dimension class and override some key member
       functions to allow the adding dimensions to netCDF files and CFA netCDF
       files.
    """
    def __init__(self, parent, name, size=None,
                 axis_type="N", metadata={}, **kwargs):
        """Initialise the dimension.  This adds the CFADimension structure to
        the dimension as well as initialising the superclass."""
        super().__init__(parent, name, size, **kwargs)
        # Has this been called from a group?
        if hasattr(parent, "_cfa_group") and parent._cfa_group:
            self._cfa_dim = parent._cfa_group.createDimension(name, size)
        # or has it been called from a dataset?
        elif hasattr(parent, "_cfa_dataset") and parent._cfa_dataset:
            if "root" in parent._cfa_dataset.getGroups():
                cfa_root_group = parent._cfa_dataset.getGroup("root")
            else:
                cfa_root_group = parent._cfa_dataset.createGroup("root")
            self._cfa_dim = cfa_root_group.createDimension(name, size)
        else:
            self._cfa_dim = None
        # Axis type metadata and metadata dictionary will be added to the
        # variable when the dimensions variable for this dimension is created


class s3Variable(netCDF4.Variable):
    """
       Inherit the UniData netCDF4 Variable class and override some key member
       functions to allow the adding variables to netCDF files and CFA netCDF
       files.
    """
    def __init__(self, parent, name, datatype, dimensions=(), zlib=False,
            complevel=4, shuffle=True, fletcher32=False, contiguous=False,
            chunksizes=None, endian='native', least_significant_digit=None,
            fill_value=None, chunk_cache=None, subarray_shape=np.array([]),
            max_subarray_size=0, **kwargs):
        """Initialise the variable.  This adds the CFAVariable structure to
        the variable as well as initialising the superclass.
        The CFA conventions dictate that the variables are created in two
        different ways:
        1. Dimension variables are created as regular netCDF variables, with
           the same dimension attached to them
        2. Field variables are created as scalar variables, i.e. they have no
           dimensions associated with them. Instead, the dimensions go in the
           netCDF attribute:
              :cfa_dimensions = "dim1 dim2 dim3"
        """
        # first check if this is a group, and create within the group if it
        # is
        if type(dimensions) is not tuple:
            raise APIException("Dimensions has to be of type tuple")

        if hasattr(parent, "_cfa_group") and parent._cfa_group:
            # check if this is a dimension variable and, if it is, assign the
            # netCDF dimension.  If it is a field variable then don't assign.
            if name in parent._cfa_group.getDimensions():
                nc_dimensions = (name,)
            else:
                nc_dimensions = list([])
                # only create the cfa variable for field variables
                self._cfa_var = parent._cfa_group.createVariable(
                    var_name=name,
                    nc_dtype=np.dtype(datatype),
                    dim_names=list(dimensions),
                    subarray_shape=subarray_shape,
                    max_subarray_size=max_subarray_size
                )
        # second check if this is a dataset, and create or get a "root" CFAgroup
        # if it is and add the CFAVariable to that group
        elif hasattr(parent, "_cfa_dataset") and parent._cfa_dataset:
            # same logic as above for whether to create a variable or if it is
            # a dimension variable
            # create root group if it doesn't exist
            if "root" in parent._cfa_dataset.getGroups():
                cfa_root_group = parent._cfa_dataset.getGroup("root")
            else:
                cfa_root_group = parent._cfa_dataset.createGroup("root")

            # get the dimensions - the name of the variable if it is a dimension
            # variable, or empty if a field variable
            if name in cfa_root_group.getDimensions():
                nc_dimensions = (name,)
            else:
                nc_dimensions = list([])
                # create the CFA var for field variables only
                self._cfa_var = cfa_root_group.createVariable(
                    var_name=name,
                    nc_dtype=np.dtype(datatype),
                    dim_names=list(dimensions),
                    subarray_shape=subarray_shape,
                    max_subarray_size=max_subarray_size
                )
        else:
            self._cfa_var = None
            nc_dimensions = dimensions

        # Initialise the base class
        super().__init__(
            parent,
            name,
            datatype,
            dimensions=nc_dimensions,
            zlib=zlib,
            complevel=complevel,
            shuffle=shuffle,
            fletcher32=fletcher32,
            contiguous=contiguous,
            chunksizes=chunksizes,
            endian=endian,
            least_significant_digit=least_significant_digit,
            fill_value=fill_value,
            chunk_cache=chunk_cache
        )

    def __setattr__(self, name, value):
        """Override the __setattr__ for the variable and store the attribute in
        the cfa_metadata.  This ensures that all of the metadata is passed down
        to the variable in the subarray files, and that any editting of the
        attributes is done before the subarray files are written."""
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if hasattr(self, "_cfa_var") and self._cfa_var:
            if name not in netCDF4._private_atts:
                # we will rely on error checking in the super class __setattr__
                # which we will call when the file is written
                self._cfa_var.metadata[name] = value
            elif not name.endswith('__'):
                if hasattr(self, name):
                    raise AttributeError((
                    "'%s' is one of the reserved attributes %s, cannot rebind. "
                    "Use setncattr instead." % (name, tuple(_private_atts))
                ))
                else:
                    self.__dict__[name]=value
        else:
            super().__setattr__(name, value)

    def __getattr__(self, name):
        """Override the __getattr__ for the variable and return the corresponding
        corresponding attribute from the cfa_metadata."""
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name.startswith('__') and name.endswith('__'):
            # if __dict__ requested, return a dict with netCDF attributes.
            if name == '__dict__':
                return super().__getattr(name)
            else:
                raise AttributeError
        elif name in netCDF4._private_atts:
            return self.__dict__[name]
        else:
            if hasattr(self, "_cfa_var") and self._cfa_var:
                return self._cfa_var.metadata[name]
            else:
                return super().__getattr(name)

    def delncattr(self, name):
        """Override delncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            try:
                self._cfa_var.metadata.pop(name)
            except KeyError:
                raise APIException(
                    "Attribute {} not found in variable {}".format(
                    name, self.name
                ))
        else:
            super().delncattr(name, value)

    def getncattr(self, name):
        """Override getncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            try:
                return self._cfa_var.metadata[name]
            except KeyError:
                raise APIException(
                    "Attribute {} not found in variable {}".format(
                    name, self.name
                ))
        else:
            return super().getncattr(name)

    def ncattrs(self):
        """Override ncattrs function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            return self._cfa_var.metadata.keys()
        else:
            return super().ncattrs()

    def setncattr(self, name, value):
        """Override setncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            self._cfa_var.metadata[name] = value
        else:
            super().setncattr(name, value)

    def setncattr_string(self, name, value):
        """Override setncattr_string function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            self._cfa_var.metadata[name] = value
        else:
            super().setncattr_string(name, value)

    def setncatts(self, attdict):
        """Override setncattrs function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            for k in attdict:
                self._cfa_var.metadata[k] = attdict[k]
        else:
            super().setncatts(attdict)

class s3Group(netCDF4.Group):
    """
       Inherit the UniData netCDF4 Group class and override some key member
       functions to allow the adding groups to netCDF files and CFA netCDF
       files.
    """
    def __init__(self, parent, name, **kwargs):
        """Initialise the group.  This adds the CFAGroup structure to the
        group as well as initialising the superclass."""
        super().__init__(parent, name, **kwargs)
        # check that this is a CFA format file
        if hasattr(parent, "_cfa_dataset") and parent._cfa_dataset:
            self._cfa_group = parent._cfa_dataset.createGroup(name)
        else:
            self._cfa_group = None

    def createDimension(self, dimname, size=None,
                        axis_type=None, metadata={}):
        """Create a dimension in the group.  Add the CFADimension structure to
        the group by calling createDimension on self._cfa_group."""
        self.dimensions[dimname] = s3Dimension(
                                       self, dimname,
                                       size=size,
                                       axis_type=axis_type,
                                       metadata=metadata
                                   )
        dimension = self.dimensions[dimname]
        return dimension

    def renameDimension(self, oldname, newname):
        """Rename the dimension by overloading the base method."""
        if self._cfa_group:
            self._cfa_group.renameDimension(oldname, newname)
        super().renameDimension(oldname, newname)

    def createVariable(self, varname, datatype, dimensions=(), zlib=False,
                       complevel=4, shuffle=True, fletcher32=False,
                       contiguous=False, chunksizes=None, endian='native',
                       least_significant_digit=None, fill_value=None,
                       chunk_cache=None, subarray_shape=np.array([]),
                       max_subarray_size=0):
        """Create a variable in the group.  Add the CFAVariable structure to
        the group by calling createVariable on self._cfa_group.
        """
        self.variables[varname] = s3Variable(
                                      self, varname, datatype,
                                      dimensions=dimensions,
                                      zlib=zlib,
                                      complevel=complevel,
                                      shuffle=shuffle,
                                      fletcher32=fletcher32,
                                      contiguous=contiguous,
                                      chunksizes=chunksizes,
                                      endian=endian,
                                      east_significant_digit=least_significant_digit,
                                      fill_value=fill_value,
                                      chunk_cache=chunk_cache,
                                      subarray_shape=subarray_shape,
                                      max_subarray_size=max_subarray_size
                                   )
        variable = self.variables[varname]
        return variable

    def renameVariable(self, oldname, newname):
        """Rename the variable by overloading the base method."""
        if self._cfa_group:
            self._cfa_group.renameVariable(oldname, newname)
        super().renameVariable(oldname, newname)

    def delncattr(self, name):
        """Override delncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_group") and self._cfa_group:
            try:
                self._cfa_group.metadata.pop(name)
            except KeyError:
                raise APIException(
                    "Attribute {} not found in variable {}".format(
                    name, self.name
                ))
        else:
            super().delncattr(name, value)

    def getncattr(self, name):
        """Override getncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_group") and self._cfa_group:
            try:
                return self._cfa_group.metadata[name]
            except KeyError:
                raise APIException(
                    "Attribute {} not found in variable {}".format(
                    name, self.name
                ))
        else:
            return super().getncattr(name)

    def ncattrs(self):
        """Override ncattrs function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_group") and self._cfa_group:
            return self._cfa_group.metadata.keys()
        else:
            return super().ncattrs()

    def setncattr(self, name, value):
        """Override setncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_group") and self._cfa_group:
            self._cfa_group.metadata[name] = value
        else:
            super().setncattr(name, value)

    def setncattr_string(self, name, value):
        """Override setncattr_string function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_group") and self._cfa_group:
            self._cfa_group.metadata[name] = value
        else:
            super().setncattr_string(name, value)

    def setncatts(self, attdict):
        """Override setncattrs function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_group") and self._cfa_group:
            for k in attdict:
                self._cfa_group.metadata[k] = attdict[k]
        else:
            super().setncatts(attdict)

class s3Dataset(netCDF4.Dataset):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member
       functions to allow the read and write of netCDF files and CFA formatted
       netCDF files to an object store accessed via an AWS S3 HTTP API.
    """

    @property
    def file_object(self):
        return self._file_object

    def __init__(self, filename, mode='r', clobber=True, format='DEFAULT',
               diskless=False, persist=False, keepweakref=False, memory=None,
               **kwargs):
        """The __init__ method can now be used with asyncio as all of the async
        functionally has been moved to the FileManager.
        Python reserved methods cannot be declared as `async`.
        """
        # Create a file manager object and keep it
        self._file_manager = FileManager()
        self._mode = mode

        # open the file and record the file handle - make sure we open it in
        # binary mode
        if 'b' not in mode:
            fh_mode = mode + 'b'

        # create the file object, this controls access to the various
        # file backends that are supported
        self._file_object = self._file_manager.open(filename, mode=fh_mode)

        # set the file up for write mode
        if mode == 'w':
            # check the format for writing - allow CFA4 in arguments and default
            # to CFA4 for writing so as to distribute files across subarrays
            if format == 'CFA4' or format == 'DEFAULT':
                file_type = 'NETCDF4'
                self._cfa_dataset = CFADataset(
                    name=filename,
                    format='NETCDF4'
                )
            elif format == 'CFA3':
                file_type = 'NETCDF3_CLASSIC'
                self._cfa_dataset = CFADataset(
                    name=filename,
                    format='NETCDF3_CLASSIC'
                )
            else:
                file_type = format
                self._cfa_dataset = None

            if self.file_object.remote_system:
                # call the base constructor
                super().__init__(
                    "inmemory.nc", mode=mode, clobber=clobber,
                    format=file_type, diskless=True, persist=persist,
                    keepweakref=keepweakref, memory=1, **kwargs
                )
            else:
                super().__init__(
                    filename, mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, **kwargs
                )

        # handle read-only mode
        elif mode == 'r':
            # get the header data
            data = self.file_object.read_from(0, 6)
            file_type, file_version = s3Dataset._interpret_netCDF_filetype(data)
            # check what the file type is a netCDF file or not
            if file_type == 'NOT_NETCDF':
                raise IOError("File: {} is not a netCDF file".format(filename))
                # read the file in, or create it
            if self.file_object.remote_system:
                # stream into memory
                nc_bytes = self.file_object.read()
                # call the base constructor
                super().__init__(
                    "inmemory.nc", mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, memory=nc_bytes, **kwargs
                )
            else:
                super().__init__(
                    filename, mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, **kwargs
                )
        else:
            # no other modes are supported
            raise APIException("Mode " + mode + " not supported.")

    def close(self):
        """Close the Dataset."""
        # write the metadata to (all) the file(s)
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            parser = CFA_netCDFParser()
            parser.write(self._cfa_dataset, self)
        # call the base class close method
        nc_bytes = super().close()
        self.file_object.close(nc_bytes)

    def createDimension(self, dimname, size=None,
                        axis_type=None, metadata={}):
        """Create a dimension in the Dataset.  Add the CFADimension structure to
        the Dataset by calling createDimension on self._cfa_dataset."""
        if dimname in self.dimensions:
            raise APIException(
                "Dimension name: {} already exists.".format(dimname)
            )
        self.dimensions[dimname] = s3Dimension(
                                       self, dimname,
                                       size=size,
                                       axis_type=axis_type,
                                       metadata=metadata
                                   )
        dimension = self.dimensions[dimname]
        return dimension

    def renameDimension(self, oldname, newname):
        """Rename the dimension by overloading the base method."""
        if not oldname in self.dimensions:
            raise APIException(
                "Dimension name: {} does not exist.".format(oldname)
            )
        # get the cfa root group
        if self._cfa_dataset:
            cfa_root_group = self._cfa_dataset.getGroup("root")
            cfa_root_group.renameDimension(oldname, newname)
        super().renameDimension(oldname, newname)

    def createVariable(self, varname, datatype, dimensions=(), zlib=False,
                       complevel=4, shuffle=True, fletcher32=False,
                       contiguous=False, chunksizes=None, endian='native',
                       least_significant_digit=None, fill_value=None,
                       chunk_cache=None, subarray_shape=np.array([]),
                       max_subarray_size=0):
        """Create a variable in the Dataset.  Add the CFAVariable structure to
        the Dataset by calling createVariable on self._cfa_dataset.
        """
        # all variables belong to a group - if a group has not been specified
        # then they belong to the "/root" group
        self.variables[varname] = s3Variable(
                                      self, varname, datatype,
                                      dimensions=dimensions,
                                      zlib=zlib,
                                      complevel=complevel,
                                      shuffle=shuffle,
                                      fletcher32=fletcher32,
                                      contiguous=contiguous,
                                      chunksizes=chunksizes,
                                      endian=endian,
                                      east_significant_digit=least_significant_digit,
                                      fill_value=fill_value,
                                      chunk_cache=chunk_cache,
                                      subarray_shape=subarray_shape,
                                      max_subarray_size=max_subarray_size
                                   )
        variable = self.variables[varname]
        return variable

    def renameVariable(self, oldname, newname):
        """Rename the variable by overloading the base method."""
        # get the cfa root group
        if self._cfa_dataset:
            cfa_root_group = self._cfa_dataset.getGroup("root")
            cfa_root_group.renameVariable(oldname, newname)
        super().renameVariable(oldname, newname)

    def createGroup(self, groupname):
        """Create a group.  If this file is a CFA file then create the CFAGroup
        as well."""
        self.groups[groupname] = s3Group(self, groupname)
        group = self.groups[groupname]
        return group

    def renameGroup(self, oldname, newname):
        """Rename a group.  If this file is a CFA file then rename the CFAGroup
        as well."""
        if self._cfa_dataset:
            self._cfa_dataset.renameGroup(oldname, newname)
        super().renameGroup(oldname, newname)

    def _getVariables(self):
        return super().__getattribute__("variables")

    def __getattribute__(self, attrname, *args, **kwargs):
        """Here we override several netCDF4.Dataset functionalities, in order
        to intercept and return CFA functionality instead."""
        if attrname == "variables":
            return self._getVariables()
        return super().__getattribute__(attrname)

    def _interpret_netCDF_filetype(data):
        """
           Pass in the first four bytes from the stream and interpret the magic number.
           See NC_interpret_magic_number in netcdf-c/libdispatch/dfile.c

           Check that it is a netCDF file before fetching any data and
           determine what type of netCDF file it is so the temporary empty file can
           be created with the same type.

           The possible types are:
           `NETCDF3_CLASSIC`, `NETCDF4`,`NETCDF4_CLASSIC`, `NETCDF3_64BIT_OFFSET` or `NETCDF3_64BIT_DATA`
           or
           `NOT_NETCDF` if it is not a netCDF file - raise an exception on that

           :return: string filetype
        """
        # start with NOT_NETCDF as the file_type
        file_version = 0
        file_type = 'NOT_NETCDF'

        # check whether it's a netCDF file (how can we tell if it's a
        # NETCDF4_CLASSIC file?
        if data[1:4] == b'HDF':
            # netCDF4 (HD5 version)
            file_type = 'NETCDF4'
            file_version = 5
        elif (data[0] == b'\016' and data[1] == b'\003' and \
              data[2] == b'\023' and data[3] == b'\001'):
            file_type = 'NETCDF4'
            file_version = 4
        elif data[0:3] == b'CDF':
            file_version = data[3]
            if file_version == 1:
                file_type = 'NETCDF3_CLASSIC'
            elif file_version == '2':
                file_type = 'NETCDF3_64BIT_OFFSET'
            elif file_version == '5':
                file_type = 'NETCDF3_64BIT_DATA'
            else:
                file_version = 1 # default to one if no version
        else:
            file_type = 'NOT_NETCDF'
            file_version = 0
        return file_type, file_version

    def delncattr(self, name):
        """Override delncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            try:
                self._cfa_dataset.metadata.pop(name)
            except KeyError:
                raise APIException(
                    "Attribute {} not found in dataset".format(name))
        else:
            super().delncattr(name, value)

    def getncattr(self, name):
        """Override getncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            try:
                return self._cfa_dataset.metadata[name]
            except KeyError:
                raise APIException(
                    "Attribute {} not found in dataset".format(name))
        else:
            return super().getncattr(name)

    def ncattrs(self):
        """Override ncattrs function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            return self._cfa_dataset.metadata.keys()
        else:
            return super().ncattrs()

    def setncattr(self, name, value):
        """Override setncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            self._cfa_dataset.metadata[name] = value
        else:
            super().setncattr(name, value)

    def setncattr_string(self, name, value):
        """Override setncattr_string function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            self._cfa_dataset.metadata[name] = value
        else:
            super().setncattr_string(name, value)

    def setncatts(self, attdict):
        """Override setncattrs function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            for k in attdict:
                self._cfa_dataset.metadata[k] = attdict[k]
        else:
            super().setncatts(attdict)
