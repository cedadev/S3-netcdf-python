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
from S3netCDF4.CFA.Parsers._CFAnetCDFParser import CFA_netCDFParser
from S3netCDF4.Managers._FileManager import FileManager
import time

class s3Dimension(object):
    """
       Inherit the UniData netCDF4 Dimension class and override some key member
       functions to allow the adding dimensions to netCDF files and CFA netCDF
       files.
    """
    _private_atts = ["_cfa_dim", "_nc_dim"]
    def __init__(self, cfa_dim=None, nc_dim=None):
        """Just initialise the dimension.  The variables will be loaded in by
        either the createDimension method(s) or the load function called from
        the parser."""
        self._cfa_dim = cfa_dim
        self._nc_dim = nc_dim

    def create(self, parent, name, size=None,
               axis_type="U", metadata={}, **kwargs):
        """Initialise the dimension.  This adds the CFADimension structure to
        the dimension as well as initialising the superclass."""
        # Has this been called from a group?
        if hasattr(parent, "_cfa_grp") and parent._cfa_grp:
            self._cfa_dim = parent._cfa_grp.createDimension(
                                name, size, axis_type=axis_type
                            )
            nc_object = parent._nc_grp
        # or has it been called from a dataset?
        elif hasattr(parent, "_cfa_dataset") and parent._cfa_dataset:
            if "root" in parent._cfa_dataset.getGroups():
                cfa_root_group = parent._cfa_dataset.getGroup("root")
            else:
                cfa_root_group = parent._cfa_dataset.createGroup("root")
            self._cfa_dim = cfa_root_group.createDimension(
                                name, size, axis_type=axis_type
                            )
            nc_object = parent
        else:
            self._cfa_dim = None
            nc_object = parent._nc_grp
        # Axis type metadata and metadata dictionary will be added to the
        # variable when the dimensions variable for this dimension is created
        self._nc_dim = netCDF4.Dimension(nc_object, name, size, **kwargs)

    def load(self, cfa_dim, nc_dim):
        """Load the variables in"""
        self._cfa_dim = cfa_dim
        self._nc_dim = nc_dim

    def __getattr__(self, name):
        """Override the __getattr__ for the dimension and return the
            corresponding attribute from the _nc_dim object."""
        if name in s3Dimension._private_atts:
            return self.__dict__[name]
        else:
            # use eval to return _nc_dim function
            return eval("self._nc_dim.{}".format(name))

    def __setattr__(self, name, value):
        """Override the __getattr__ for the dimension and return the
            corresponding attribute from the _nc_dim object."""
        if name in s3Dimension._private_atts:
            self.__dict__[name] = value
        elif name == "_dimid":
            self._nc_dim._dimid = value
        elif name == "_grpid":
            self._nc_dim._grpid = value
        elif name == "_data_model":
            self._nc_dim._data_model = value
        elif name == "_name":
            self._nc_dim._name = value
        elif name == "_grp":
            self._nc_dim._grp = value

    def __len__(self):
        return self._nc_dim.__len__()

    @property
    def size(self):
        return self._nc_dim.__len__()

class s3Variable(object):
    """
       Inherit the UniData netCDF4 Variable class and override some key member
       functions to allow the adding variables to netCDF files and CFA netCDF
       files.
    """
    # private attributes for just the s3Variable
    _private_atts = [
        "_cfa_var", "_cfa_dim", "_nc_var"
    ]

    def __init__(self, cfa_var=None, cfa_dim=None, nc_var=None):
        """Just initialise the class, any loading of the variables will be done
        by the parser, or the CreateVariable member of s3Group."""
        self._cfa_var = cfa_var
        self._cfa_dim = cfa_dim
        self._nc_var = nc_var

    def create(self, parent, name, datatype, dimensions=(), zlib=False,
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
        # first check if this is a group, and create within the group if it is
        if type(dimensions) is not tuple:
            raise APIException("Dimensions has to be of type tuple")

        if hasattr(parent, "_cfa_grp") and parent._cfa_grp:
            # check if this is a dimension variable and, if it is, assign the
            # netCDF dimension.  If it is a field variable then don't assign.
            if name in parent._cfa_grp.getDimensions():
                nc_dimensions = (name,)
                # get a reference to the already created cfa_dim
                self._cfa_dim = parent._cfa_grp.getDimension(name)
            else:
                nc_dimensions = list([])
                # only create the cfa variable for field variables
                self._cfa_var = parent._cfa_grp.createVariable(
                    var_name=name,
                    nc_dtype=np.dtype(datatype),
                    dim_names=list(dimensions),
                    subarray_shape=subarray_shape,
                    max_subarray_size=max_subarray_size
                )
            # get the netcdf dataset
            ncd = parent.parent
            # we need the actual instance of the netCDF parent - i.e an original
            # netCDF4 python class
            nc_parent = parent._nc_grp

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
                # get a reference to the already created cfa_dim
                self._cfa_dim = cfa_root_group.getDimension(name)
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
                # later we will need to write the partition information into
                # the partition object
            # get the netcdf dataset
            ncd = parent
            nc_parent = ncd
        else:
            self._cfa_var = None
            nc_dimensions = dimensions
            if hasattr(parent, "_nc_grp"):
                ncd = parent._nc_grp
            else:
                ncd = parent
            nc_parent = ncd

        if (hasattr(self, "_cfa_var") and self._cfa_var and
            len(self._cfa_var.getDimensions()) != 0):
            # get the partition matrix dimensions from the created variable
            pm_dimensions = self._cfa_var.getPartitionMatrixDimensions()
            pm_shape = self._cfa_var.getPartitionMatrixShape()
            assert(len(pm_dimensions) == len(pm_shape))

            # get the version of the cfa dataset
            cfa_version = ncd._cfa_dataset.getCFAVersion()

            if cfa_version == "0.5":
                # create the custom datatypes in the netCDF file, if not already
                # created
                if not "Subarray" in ncd.cmptypes:
                    subarray_type = ncd.createCompoundType(
                        Subarray_type, "Subarray"
                    )
                else:
                    subarray_type = ncd.cmptypes["Subarray"]
                if not "Partition" in ncd.cmptypes:
                    partition_type = ncd.createCompoundType(
                        Partition_type, "Partition"
                    )
                else:
                    partition_type = ncd.cmptypes["Partition"]

                # write out the cfa data as a group, with the same name as the
                # variable, prefixed with "cfa_"
                cfa_metagroup_name = "cfa_" + name
                # create this "metagroup"
                cfa_metagroup = nc_parent.createGroup(cfa_metagroup_name)

                # create the Partition dimensions
                for d in range(0, len(pm_dimensions)):
                    part_dim = cfa_metagroup.createDimension(
                                   pm_dimensions[d], pm_shape[d]
                               )
                # create the partition variable
                partition_var = cfa_metagroup.createVariable(
                    name, partition_type, tuple(pm_dimensions)
                )
                # write the partition information directly into the partitions
                # variable in the netCDF file
                self._cfa_var.writePartitions(partition_var)
            elif (cfa_version == "0.4"):
                # create a numpy array of the complex datatype
                partitions = np.empty(pm_shape, dtype=Partition_type)
                # write the parition information into this numpy array
                self._cfa_var.writePartitions(partitions)
            else:
                raise CFAError("Unsupported CFA version {}.".format(cfa_version))

        # Initialise the base class
        self._nc_var = netCDF4.Variable(
            nc_parent,
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

    def load(self, cfa_var, cfa_dim, nc_var, parent):
        """Just initialise the class, any loading of the variables will be done
        by the parser, or the CreateVariable member of s3Group."""
        self._cfa_var = cfa_var
        self._cfa_dim = cfa_dim
        self._nc_var = nc_var
        self.parent = parent

    def _setatt(self, cfa_object, name, value):
        if not (name in netCDF4._private_atts or name in s3Variable._private_atts):
            # we will rely on error checking in the super class __setattr__
            # which we will call when the file is written
            cfa_object.metadata[name] = value
        elif not name.endswith('__'):
            if hasattr(self, name):
                raise AttributeError((
                "'%s' is one of the reserved attributes %s, cannot rebind. "
                "Use setncattr instead." % (name, tuple(
                    netCDF4._private_atts, s3Variable._private_atts
                ))
            ))
            else:
                self.__dict__[name]=value

    def __setattr__(self, name, value):
        """Override the __setattr__ for the variable and store the attribute in
        the cfa_metadata.  This ensures that all of the metadata is passed down
        to the variable in the subarray files, and that any editting of the
        attributes is done before the subarray files are written."""
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name in s3Variable._private_atts:
            self.__dict__[name] = value
        elif hasattr(self, "_cfa_var") and self._cfa_var:
            self._setatt(self._cfa_var, name, value)
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            self._setatt(self._cfa_dim, name, value)
        else:
            self._nc_var.__setattr__(name, value)

    def __getattr__(self, name):
        """Override the __getattr__ for the variable and return the
        corresponding attribute from the cfa_metadata."""
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name.startswith('__') and name.endswith('__'):
            # if __dict__ requested, return a dict with netCDF attributes.
            if name == '__dict__':
                return self._nc_var.__getattr__(name)
            else:
                raise AttributeError
        else:
            if name in s3Variable._private_atts:
                return self.__dict__[name]
            elif hasattr(self, "_cfa_var") and self._cfa_var:
                try:
                    return self._cfa_var.metadata[name]
                except KeyError:
                    return eval("self._nc_var.{}".format(name))
            elif hasattr(self, "_cfa_dim") and self._cfa_dim:
                try:
                    return self._cfa_dim.metadata[name]
                except KeyError:
                    return eval("self._nc_var.{}".format(name))
            else:
                # use eval to return _nc_var function
                return eval("self._nc_var.{}".format(name))

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
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            try:
                self._cfa_dim.metadata.pop(name)
            except KeyError:
                raise APIException(
                    "Attribute {} not found in variable {}".format(
                    name, self.name
                ))
        else:
            self._nc_var.delncattr(name, value)

    def getncattr(self, name):
        """Override getncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            try:
                return self._cfa_var.metadata[name]
            except KeyError:
                return self._nc_var.getncattr(name)
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            try:
                return self._cfa_dim.metadata[name]
            except KeyError:
                return self._nc_var.getncattr(name)
        else:
            return self._nc_var.getncattr(name)

    def ncattrs(self):
        """Override ncattrs function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            return self._cfa_var.metadata.keys()
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            return self._cfa_dim.metadata.keys()
        else:
            return self._nc_var.ncattrs()

    def setncattr(self, name, value):
        """Override setncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            self._cfa_var.metadata[name] = value
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            self._cfa_dim.metadata[name] = value
        else:
            self._nc_var.setncattr(name, value)

    def setncattr_string(self, name, value):
        """Override setncattr_string function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            self._cfa_var.metadata[name] = value
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            self._cfa_dim.metadata[name] = value
        else:
            self._nc_var.setncattr_string(name, value)

    def setncatts(self, attdict):
        """Override setncattrs function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_var") and self._cfa_var:
            for k in attdict:
                if not(k in netCDF4._private_atts or k in s3Variable._private_atts):
                    self._cfa_var.metadata[k] = attdict[k]
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            for k in attdict:
                if not(k in netCDF4._private_atts or k in s3Variable._private_atts):
                    self._cfa_dim.metadata[k] = attdict[k]
        else:
            self._nc_var.setncatts(attdict)

    def __setitem__(self, elem, data):
        """Override the netCDF4.Variable __setitem__ method to assign data to
        the correct subarray file.
        The CFAVariable class has a method which will determine:
            1. The path of the subarray file(s)
            2. The slice in the subarray(s) to write into
            3. The slice from the input data to read from, when transferring
            data from the input data to the subarray
        """
        if hasattr(self, "_cfa_var") and self._cfa_var:
            # get the above details from the CFA variable, details are returned as:
            # (filename, varname, source_slice, target_slice)
            index_list = self._cfa_var.__getitem__(elem)
        else:
            self._nc_var.__setitem__(elem, data)

    def __getitem__(self, elem):
        """Override the netCDF4.Variable __getitem__ method to assign data to
        the correct subarray file.
        The CFAVariable class has a method which will determine:
            1. The path of the subarray file(s)
            2. The slice in the subarray(s) to read from
            3. The slice in the output array to write to
        """
        if hasattr(self, "_cfa_var") and self._cfa_var:
            # get the above details from the CFA variable, details are returned as:
            # (filename, varname, source_slice, target_slice)
            index_list = self._cfa_var.__getitem__(elem)
        else:
            self._nc_var.__getitem__(elem, data)


class s3Group(object):
    """
       Inherit the UniData netCDF4 Group class and override some key member
       functions to allow the adding groups to netCDF files and CFA netCDF
       files.
    """
    _private_atts = ["_nc_grp", "_cfa_grp", "parent",
                     "_s3_variables", "_s3_dimensions"]

    def __init__(self, nc_grp=None, cfa_grp=None, parent=None):
        """Just initialise the class, any loading of the variables will be done
        by the parser, or the CreateGroup member of s3Dataset."""
        self._nc_grp = nc_grp
        self._cfa_grp = cfa_grp
        self.parent = parent

        self._s3_dimensions = None
        self._s3_variables = None

    def create(self, parent, name, **kwargs):
        """Initialise the group.  This adds the CFAGroup structure to the
        group as well as initialising the superclass."""
        self._nc_grp = netCDF4.Group(parent, name, **kwargs)
        # check that this is a CFA format file
        if hasattr(parent, "_cfa_dataset") and parent._cfa_dataset:
            self._cfa_grp = parent._cfa_dataset.createGroup(name)
        else:
            self._cfa_grp = None
        self.parent = parent

    def load(self, nc_grp=None, cfa_grp=None, parent=None):
        """Assign the variables in the parameters to the member variables."""
        self._nc_grp = nc_grp
        self._cfa_grp = cfa_grp
        self.parent = parent

    def createDimension(self, dimname, size=None,
                        axis_type="U", metadata={}):
        """Create a dimension in the group.  Add the CFADimension structure to
        the group by calling createDimension on self._cfa_grp."""
        self._nc_grp.dimensions[dimname] = s3Dimension()
        self._nc_grp.dimensions[dimname].create(
                                       self, dimname,
                                       size=size,
                                       axis_type=axis_type,
                                       metadata=metadata
                                   )
        return self._nc_grp.dimensions[dimname]

    def renameDimension(self, oldname, newname):
        """Rename the dimension by overloading the base method."""
        if self._cfa_grp:
            self._cfa_grp.renameDimension(oldname, newname)
        self._nc_grp.renameDimension(oldname, newname)

    def createVariable(self, varname, datatype, dimensions=(), zlib=False,
                       complevel=4, shuffle=True, fletcher32=False,
                       contiguous=False, chunksizes=None, endian='native',
                       least_significant_digit=None, fill_value=None,
                       chunk_cache=None, subarray_shape=np.array([]),
                       max_subarray_size=0):
        """Create a variable in the group.  Add the CFAVariable structure to
        the group by calling createVariable on self._cfa_grp.
        """
        self.variables[varname] = s3Variable()
        self.variables[varname].create(
                                      self, varname, datatype,
                                      dimensions=dimensions,
                                      zlib=zlib,
                                      complevel=complevel,
                                      shuffle=shuffle,
                                      fletcher32=fletcher32,
                                      contiguous=contiguous,
                                      chunksizes=chunksizes,
                                      endian=endian,
                                      least_significant_digit=least_significant_digit,
                                      fill_value=fill_value,
                                      chunk_cache=chunk_cache,
                                      subarray_shape=subarray_shape,
                                      max_subarray_size=max_subarray_size
                                   )
        return self.variables[varname]

    def renameVariable(self, oldname, newname):
        """Rename the variable by overloading the base method."""
        if self._cfa_grp:
            self._cfa_grp.renameVariable(oldname, newname)
        self._nc_grp.renameVariable(oldname, newname)

    def delncattr(self, name):
        """Override delncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_grp") and self._cfa_grp:
            try:
                self._cfa_grp.metadata.pop(name)
            except KeyError:
                raise APIException(
                    "Attribute {} not found in variable {}".format(
                    name, self.name
                ))
        else:
            self._nc_grp.delncattr(name, value)

    def getncattr(self, name):
        """Override getncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_grp") and self._cfa_grp:
            try:
                return self._cfa_grp.metadata[name]
            except KeyError:
                return self._nc_grp.getncattr(name)
        else:
            return self._nc_grp.getncattr(name)

    def ncattrs(self):
        """Override ncattrs function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_grp") and self._cfa_grp:
            return self._cfa_grp.metadata.keys()
        else:
            return self._nc_grp.ncattrs()

    def setncattr(self, name, value):
        """Override setncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_grp") and self._cfa_grp:
            self._cfa_grp.metadata[name] = value
        else:
            self._nc_grp.setncattr(name, value)

    def setncattr_string(self, name, value):
        """Override setncattr_string function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_grp") and self._cfa_grp:
            self._cfa_grp.metadata[name] = value
        else:
            self._nc_grp.setncattr_string(name, value)

    def setncatts(self, attdict):
        """Override setncattrs function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_grp") and self._cfa_grp:
            for k in attdict:
                self._cfa_grp.metadata[k] = attdict[k]
        else:
            self._nc_grp.setncatts(attdict)

    @property
    def variables(self):
        # if we are requesting the variables, and this is a cfa dataset then
        # build and return the dictionary of s3_variables
        if self._s3_variables == None:
            self._s3_variables = {}
            for var in self._nc_grp.variables:
                nc_var = self._nc_grp.variables[var]
                if var in self._cfa_grp.getVariables():
                    cfa_var = self._cfa_grp.getVariable(var)
                    # create the s3group with links to the cfa group, and nc_grp
                    self._s3_variables[var] = s3Variable(
                                                   nc_var=nc_var,
                                                   cfa_var=cfa_var
                                                )
                else:
                    self._s3_variables[var] = nc_var

        return self._s3_variables

    @property
    def dimensions(self):
        # if we are requesting the dimensions, and this is a cfa dataset then
        # build and return the dictionary of s3_dimensions
        if self._s3_dimensions == None:
            self._s3_dimensions = {}
            for dim in self._nc_grp.dimensions:
                print(dim)
                nc_dim = self._nc_grp.dimensions[dim]
                if dim in self._cfa_grp.getDimensions():
                    cfa_dim = self._cfa_grp.getDimension(dim)
                    # create the s3group with links to the cfa dimension,
                    # and nc_dim
                    self._s3_dimensions[dim] = s3Dimension(
                                                   cfa_dim=cfa_dim,
                                                   nc_dim=nc_dim
                                                )
                else:
                    self._s3_dimensions[dim] = nc_dim

        return self._s3_dimensions

    def __getattr__(self, name):
        """Override the __getattr__ for the Group so as to return its
        private variables."""
        if name in s3Group._private_atts:
            return self.__dict__[name]
        elif name == "dimensions":
            return self.dimensions
        elif name == "variables":
            return self.dimensions
        else:
            return eval("self._nc_grp.{}".format(name))

    def __setattr__(self, name, value):
        """Override the __setattr__ for the Group so as to assign its
        private variables."""
        if name in s3Group._private_atts:
            self.__dict__[name] = value
        else:
            self._nc_grp.__setattr__(name, value)

class s3Dataset(netCDF4.Dataset):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member
       functions to allow the read and write of netCDF files and CFA formatted
       netCDF files to an object store accessed via an AWS S3 HTTP API.
    """

    _private_atts = ['file_object', '_file_object', '_file_manager', '_mode',
                     '_cfa_grp', '_cfa_dataset',
                    ]

    @property
    def file_object(self):
        return self._file_object

    def __init__(self, filename, mode='r', clobber=True, format='DEFAULT',
               diskless=False, persist=False, keepweakref=False, memory=None,
               cfa_version="0.4", **kwargs):
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
                    format='NETCDF4',
                    cfa_version=cfa_version
                )
            elif format == 'CFA3':
                file_type = 'NETCDF3_CLASSIC'
                self._cfa_dataset = CFADataset(
                    name=filename,
                    format='NETCDF3_CLASSIC',
                    cfa_version=cfa_version
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
            # parse the CFA file if it is one
            parser = CFA_netCDFParser()
            if parser.is_file(self):
                self._cfa_dataset = parser.read(self)
                # set these to be null, they will be built and cached when a call
                # is made to the .groups, .variables or .dimensions functions
                self._s3_groups = None
                self._s3_dimensions = None
                self._s3_variables = None
        else:
            # no other modes are supported
            raise APIException("Mode " + mode + " not supported.")

    def close(self):
        """Close the Dataset."""
        # write the metadata to (all) the file(s)
        if (self._mode == 'w' and
            hasattr(self, "_cfa_dataset") and
            self._cfa_dataset):
            parser = CFA_netCDFParser()
            parser.write(self._cfa_dataset, self)
        # call the base class close method
        nc_bytes = super().close()
        self.file_object.close(nc_bytes)

    def createDimension(self, dimname, size=None,
                        axis_type="U", metadata={}):
        """Create a dimension in the Dataset.  Add the CFADimension structure to
        the Dataset by calling createDimension on self._cfa_dataset."""
        if dimname in self.dimensions:
            raise APIException(
                "Dimension name: {} already exists.".format(dimname)
            )
        self.dimensions[dimname] = s3Dimension()
        self.dimensions[dimname].load(
                                       self, dimname,
                                       size=size,
                                       axis_type=axis_type,
                                       metadata=metadata
                                   )
        return self.dimensions[dimname]

    def renameDimension(self, oldname, newname):
        """Rename the dimension by overloading the base method."""
        if not oldname in self.dimensions:
            raise APIException(
                "Dimension name: {} does not exist.".format(oldname)
            )
        # get the cfa root group
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
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
        self.variables[varname] = s3Variable()
        self.variables[varname].create(
                                      self, varname, datatype,
                                      dimensions=dimensions,
                                      zlib=zlib,
                                      complevel=complevel,
                                      shuffle=shuffle,
                                      fletcher32=fletcher32,
                                      contiguous=contiguous,
                                      chunksizes=chunksizes,
                                      endian=endian,
                                      least_significant_digit=least_significant_digit,
                                      fill_value=fill_value,
                                      chunk_cache=chunk_cache,
                                      subarray_shape=subarray_shape,
                                      max_subarray_size=max_subarray_size
                                   )
        return self.variables[varname]

    def renameVariable(self, oldname, newname):
        """Rename the variable by overloading the base method."""
        # get the cfa root group
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            cfa_root_group = self._cfa_dataset.getGroup("root")
            cfa_root_group.renameVariable(oldname, newname)
        super().renameVariable(oldname, newname)

    def createGroup(self, groupname):
        """Create a group.  If this file is a CFA file then create the CFAGroup
        as well."""
        self.groups[groupname] = s3Group()
        self.groups[groupname].create(self, groupname)
        return self.groups[groupname]

    def renameGroup(self, oldname, newname):
        """Rename a group.  If this file is a CFA file then rename the CFAGroup
        as well."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            self._cfa_dataset.renameGroup(oldname, newname)
        super().renameGroup(oldname, newname)

    def __getattr__(self, name):
        """Override the __getattr__ for the Dataset so as to return its
        private variables."""
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name in s3Dataset._private_atts:
            return self.__dict__[name]
        else:
            return super().__getattr__(name)

    def __setattr__(self, name, value):
        """Override the __setattr__ for the Dataset so as to assign its
        private variables."""
        if name in s3Dataset._private_atts:
            self.__dict__[name] = value
        else:
            super().__setattr__(name, value)

    @property
    def groups(self):
        # if we are requesting the groups, and this is a cfa dataset then build
        # and return the dictionary of s3_groups
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            if self._s3_groups == None:
                self._s3_groups = {}
                for grp in super().groups:
                    nc_grp = super().groups[grp]
                    if grp in self._cfa_dataset.getGroups():
                        cfa_grp = self._cfa_dataset.getGroup(grp)
                        # create the s3group with links to the cfa group, and nc_grp
                        self._s3_groups[grp] = s3Group(
                                                       nc_grp=nc_grp,
                                                       cfa_grp=cfa_grp,
                                                       parent=self
                                                    )
                    else:
                        self._s3_groups[grp] = nc_grp

            return self._s3_groups
        else:
            return super().groups

    @property
    def variables(self):
        # if we are requesting the variables, and this is a cfa dataset then
        # build and return the dictionary of s3_variables
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            if self._s3_variables == None:
                # get the root group
                cfa_group = self._cfa_dataset.getGroup("root")
                self._s3_variables = {}
                for var in super().variables:
                    nc_var = super().variables[var]
                    if var in cfa_group.getVariables():
                        cfa_var = cfa_group.getVariable(var)
                        # create the s3group with links to the cfa group, and nc_grp
                        self._s3_variables[var] = s3Variable(
                                                       nc_var=nc_var,
                                                       cfa_var=cfa_var
                                                    )
                    else:
                        self._s3_variables[var] = nc_var

            return self._s3_variables
        else:
            return super().variables

    @property
    def dimensions(self):
        # if we are requesting the dimensions, and this is a cfa dataset then
        # build and return the dictionary of s3_dimensions
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            if self._s3_dimensions == None:
                # get the root group
                cfa_group = self._cfa_dataset.getGroup("root")
                self._s3_dimensions = {}
                for dim in super().dimensions:
                    nc_dim = super().dimensions[dim]
                    if dim in cfa_group.getDimensions():
                        cfa_dim = cfa_group.getDimension(dim)
                        # create the s3group with links to the cfa dimension,
                        # and nc_dim
                        self._s3_dimensions[dim] = s3Dimension(
                                                       cfa_dim=cfa_dim,
                                                       nc_dim=nc_dim
                                                    )
                    else:
                        self._s3_dimensions[dim] = nc_dim

            return self._s3_dimensions
        else:
            return super().dimensions

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
                return super().getncattr(name)
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
