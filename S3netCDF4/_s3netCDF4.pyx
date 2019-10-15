"""
S3 enabled version of netCDF4.
Allows reading and writing of netCDF files to object stores via AWS S3.

Requirements: botocore / aiobotocore, psutil, netCDF4, Cython

Author:  Neil Massey
Date:    10/07/2017
Updated: 13/05/2019
"""

import numpy as np

# This module Duplicates classes and functions from the standard UniData netCDF4
# implementation and overrides their functionality so as it enable S3 and CFA
# functionality
# import as netCDF4 to avoid confusion with the S3netCDF4 module
import netCDF4._netCDF4 as netCDF4

from _Exceptions import *
from S3netCDF4.CFA._CFAClasses import *
from S3netCDF4.CFA.Parsers._CFAnetCDFParser import CFA_netCDFParser
from S3netCDF4.Managers._FileManager import FileManager, OpenFileRecord

class s3Dimension(object):
    """
       Duplicate the UniData netCDF4 Dimension class and override some key member
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
            cfa_root_group = parent._cfa_dataset.getGroup("root")
            self._cfa_dim = cfa_root_group.createDimension(
                                name, size, axis_type=axis_type
                            )
            nc_object = parent._nc_dataset
        else:
            self._cfa_dim = None
            # get the group or dataset (for none-CFA)
            if hasattr(parent, "_nc_grp"):
                nc_object = parent._nc_grp
            elif hasattr(parent, "_nc_dataset"):
                nc_object = parent._nc_dataset
            else:
                raise APIException("Cannot find group or dataset in parent")
        # Axis type metadata and metadata dictionary will be added to the
        # variable when the dimensions variable for this dimension is created
        self._nc_dim = nc_object.createDimension(name, size)

    def __getattr__(self, name):
        """Override the __getattr__ for the dimension and return the
            corresponding attribute from the _nc_dim object."""
        if name in s3Dimension._private_atts:
            return self.__dict__[name]
        else:
            return self._nc_dim.__getattr__(name)

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
       Duplicate the UniData netCDF4 Variable class and override some key member
       functions to allow the adding variables to netCDF files and CFA netCDF
       files.
    """
    # private attributes for just the s3Variable
    _private_atts = [
        "_cfa_var", "_cfa_dim", "_nc_var", "_file_manager", "parent"
    ]

    def __init__(self, cfa_var=None, cfa_dim=None, nc_var=None):
        """Just initialise the class, any loading of the variables will be done
        by the parser, or the CreateVariable member of s3Group."""
        self._cfa_var = cfa_var
        self._cfa_dim = cfa_dim
        self._nc_var = nc_var
        # need a file manager for the sub array files
        self._file_manager = FileManager()
        self.parent = None

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

        # keep track of the parent that created the variable
        self.parent = parent

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
            # get the s3_dataset
            s3_dataset = parent.parent
            # get the netCDF parent (group)
            nc_parent = parent._nc_grp
            # get the netCDF dataset
            ncd = s3_dataset._nc_dataset

        # second check if this is a dataset, and create or get a "root" CFAgroup
        # if it is and add the CFAVariable to that group
        elif hasattr(parent, "_cfa_dataset") and parent._cfa_dataset:
            # same logic as above for whether to create a variable or if it is
            # a dimension variable
            cfa_root_group = parent._cfa_dataset.getGroup("root")

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
            # get the s3_dataset
            s3_dataset = parent
            # get the netCDF parent (dataset)
            nc_parent = s3_dataset._nc_dataset
            # get the netCDF dataset
            ncd = s3_dataset._nc_dataset
        else:
            self._cfa_var = None
            nc_dimensions = dimensions
            # get the group or dataset (for none-CFA)
            if hasattr(parent, "_nc_grp"):
                nc_parent = parent._nc_grp
            elif hasattr(parent, "_nc_dataset"):
                nc_parent = parent._nc_dataset
            else:
                raise APIException("Cannot find group or dataset in parent")

        if (hasattr(self, "_cfa_var") and self._cfa_var and
            len(self._cfa_var.getDimensions()) != 0):
            # get the partition matrix dimensions from the created variable
            pm_dimensions = self._cfa_var.getPartitionMatrixDimensions()
            pm_shape = self._cfa_var.getPartitionMatrixShape()
            assert(len(pm_dimensions) == len(pm_shape))

            # get the version of the cfa dataset
            cfa_version = s3_dataset._cfa_dataset.getCFAVersion()

            if cfa_version == "0.5" or cfa_version == "0.4":
                self._cfa_var.writePartitions(cfa_version, nc_parent)
            else:
                raise CFAError("Unsupported CFA version {}.".format(cfa_version))

        # Initialise the base class
        self._nc_var = nc_parent.createVariable(
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

    def __getattr__(self, name):
        """Override the __getattr__ for the Group so as to return its
        private variables."""
        if name in s3Variable._private_atts:
            return self.__dict__[name]
        elif name in netCDF4._private_atts:
            return self._nc_var.__getattr__(name)
        elif hasattr(self, "_cfa_var") and self._cfa_var:
            return self._cfa_var.metadata[name]
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            return self._cfa_dim.metadata[name]
        else:
            return self._nc_var.__getattr__(name)

    def __setattr__(self, name, value):
        """Override the __setattr__ for the Group so as to assign its
        private variables."""
        if name in s3Variable._private_atts:
            self.__dict__[name] = value
        elif name in netCDF4._private_atts:
            self._nc_var.__setattr__(name, value)
        elif hasattr(self, "_cfa_var") and self._cfa_var:
            self._cfa_var.metadata[name] = value
        elif hasattr(self, "_cfa_dim") and self._cfa_dim:
            self._cfa_dim.metadata[name] = value
        else:
            self._nc_var.__setattr__(name, value)

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

    def __create_subarray_ncfile(self, index, in_memory=True, mode="w"):
        """Create the subarray file, either in memory or on disk."""
        file_path = index.file.decode("utf-8")
        # get the parent dataset to get the information used in its creation,
        # so that it can be mirrored in the creation of the sub-array files
        if hasattr(self.parent, "_nc_grp"):
            ncd = self.parent.parent._nc_dataset
        else:
            ncd = self.parent._nc_dataset
        nc_dataset = netCDF4.Dataset(file_path, mode=mode, format=ncd.data_model)

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
            for index in index_list:
                size = np.prod(index.shape)
                # check with the filemanager what state the file is in, and
                # whether we should open it
                request_object = self._file_manager.request_file(
                                    index.file, size, mode="w"
                                )
                if request_object.open_state == OpenFileRecord.OPEN_NEW_IN_MEMORY:
                    # create a netCDF file in memory if it has not been created
                    # previously
                    self.__create_subarray_ncfile(
                        index, in_memory = True, mode="w"
                    )
                elif request_object.open_state == OpenFileRecord.OPEN_NEW_ON_DISK:
                    self.__create_subarray_ncfile(
                        index, in_memory = False, mode="w"
                    )
                # # if it has been created before then use the previously created
                # # file
                # elif request_object.open_state == OpenFileRecord.OPEN_EXISTS_IN_MEMORY:
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
            for i in index_list:
                # when the file is streamed into memory, the whole file is read
                # in - i.e. it is opened in memory by netCDF.
                # We need to reserve the full amount of space
                size = np.prod(i.shape)
                request_object = self._file_manager.request_file(
                                    i.file, size, mode="r"
                                )
        else:
            return self._nc_var.__getitem__(elem)

class s3Group(object):
    """
       Duplicate the UniData netCDF4 Group class and override some key member
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
        self._s3_variables = {}
        self._s3_dimensions = {}

    def create(self, parent, name, **kwargs):
        """Initialise the group.  This adds the CFAGroup structure to the
        group as well as initialising the composed class."""
        # Has this been called from a group?
        if hasattr(parent, "_cfa_dataset") and parent._cfa_dataset:
            self._cfa_grp = parent._cfa_dataset.createGroup(name)
        else:
            self._cfa_grp = None
        # create netCDF group
        self._nc_grp  = parent._nc_dataset.createGroup(name)
        # record the parent
        self.parent = parent

    def createDimension(self, dimname, size=None,
                        axis_type="U", metadata={}):
        """Create a dimension in the group.  Add the CFADimension structure to
        the group by calling createDimension on self._cfa_grp."""
        if dimname in self._nc_grp.dimensions:
            raise APIException(
                "Dimension name: {} already exists.".format(dimname)
            )
        self._s3_dimensions[dimname] = s3Dimension()
        self._s3_dimensions[dimname].create(self, dimname, size=size,
                                         axis_type=axis_type, metadata=metadata)
        return self._s3_dimensions[dimname]

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
        """Create a variable in the Group.  Add the CFAVariable structure to
        the group by calling createVariable on self._cfa_grp.
        """
        self._s3_variables[varname] = s3Variable()
        self._s3_variables[varname].create(
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
        return self._s3_variables[varname]

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
        return self._s3_variables

    @property
    def dimensions(self):
        return self._s3_dimensions

    def __getattr__(self, name):
        """Override the __getattr__ for the Group so as to return its
        private variables."""
        if name in s3Group._private_atts:
            return self.__dict__[name]
        elif name in netCDF4._private_atts:
            return self._nc_grp.__getattr__(name)
        elif hasattr(self, "_cfa_grp") and self._cfa_grp:
            return self._cfa_grp.metadata[name]
        else:
            return self._nc_grp.__getattr__(name)


    def __setattr__(self, name, value):
        """Override the __setattr__ for the Group so as to assign its
        private variables."""
        if name in s3Group._private_atts:
            self.__dict__[name] = value
        elif name in netCDF4._private_atts:
            self._nc_grp.__setattr__(name, value)
        elif hasattr(self, "_cfa_grp") and self._cfa_grp:
            self._cfa_grp.metadata[name] = value
        else:
            self._nc_grp.__setattr__(name, value)


class s3Dataset(object):
    """
       Duplicate the UniData netCDF4 Dataset class and override some key member
       functions to allow the read and write of netCDF files and CFA formatted
       netCDF files to an object store accessed via an AWS S3 HTTP API.
    """

    _private_atts = ['file_object', '_file_object', '_file_manager', '_mode',
                     '_cfa_dataset', '_nc_dataset',
                     '_s3_groups', '_s3_dimensions', '_s3_variables'
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
        # check CFA compatibility 0.5 can only be used with NETCDF4 or CFA4
        # (strictly CFA4, but we'll allow the user some leeway)
        if cfa_version == "0.5":
            if format == "CFA3" or format == "NETCDF3_CLASSIC":
                raise APIException(
                    "CFA 0.5 is not compatible with NETCDF3 file formats."
                )

        # Create a file manager object and keep it
        self._file_manager = FileManager()
        self._mode = mode

        # open the file and record the file handle - make sure we open it in
        # binary mode
        if 'b' not in mode:
            fh_mode = mode + 'b'

        # set the group to be an empty dictionary.  There will always be one
        # group - the root group, this will be created later
        self._s3_groups = {}
        self._s3_dimensions = {}
        self._s3_variables = {}

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
                # create the root group in the cfa file
                self._cfa_dataset.createGroup("root")
            elif format == 'CFA3':
                file_type = 'NETCDF3_CLASSIC'
                self._cfa_dataset = CFADataset(
                    name=filename,
                    format='NETCDF3_CLASSIC',
                    cfa_version=cfa_version
                )
                # create the root group in the cfa file
                self._cfa_dataset.createGroup("root")
            else:
                file_type = format
                self._cfa_dataset = None

            if self._file_object.remote_system:
                # call the constructor of the netCDF4.Dataset class
                self._nc_dataset = netCDF4.Dataset(
                    "inmemory.nc", mode=mode, clobber=clobber,
                    format=file_type, diskless=True, persist=False,
                    keepweakref=keepweakref, memory=0, **kwargs
                )
            else:
                # this is a non remote file, i.e. just on the disk
                self._nc_dataset = netCDF4.Dataset(
                    filename, mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, **kwargs
                )
                # close the existing file hangle and set the file_object to be
                # the netcdf dataset
                self._file_object.close()
                self._file_object._fh = self._nc_dataset
        # handle read-only mode
        elif mode == 'r':
            # get the header data
            data = self._file_object.read_from(0, 6)
            file_type, file_version = s3Dataset._interpret_netCDF_filetype(data)
            # check what the file type is a netCDF file or not
            if file_type == 'NOT_NETCDF':
                raise IOError("File: {} is not a netCDF file".format(filename))
                # read the file in, or create it
            if self._file_object.remote_system:
                # stream into memory
                nc_bytes = self.file_object.read()
                # call the base constructor
                self._nc_dataset = netCDF4.Dataset(
                    "inmemory.nc", mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, memory=nc_bytes, **kwargs
                )
            else:
                # close the open file object
                self._file_object.close()
                self._nc_dataset = netCDF4.Dataset(
                    filename, mode=mode, clobber=clobber,
                    format=file_type, diskless=diskless, persist=persist,
                    keepweakref=keepweakref, **kwargs
                )
                # reassign the file handle
                self._file_object._fh = self._nc_dataset

            # parse the CFA file if it is one
            parser = CFA_netCDFParser()
            if parser.is_file(self._nc_dataset):
                parser.read(self)
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
        if self._file_object.remote_system:
            nc_bytes = self._nc_dataset.close()
            self._file_object.close(nc_bytes)
        else:
            self._file_object.close()

    def createDimension(self, dimname, size=None,
                        axis_type="U", metadata={}):
        """Create a dimension in the Dataset.  This just calls createDimension
        on the root group"""
        self._s3_dimensions[dimname] = s3Dimension()
        self._s3_dimensions[dimname].create(self, dimname, size=size,
                                         axis_type=axis_type, metadata=metadata)
        return self._s3_dimensions[dimname]

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
        self._nc_dataset.renameDimension(oldname, newname)

    def createVariable(self, varname, datatype, dimensions=(), zlib=False,
                       complevel=4, shuffle=True, fletcher32=False,
                       contiguous=False, chunksizes=None, endian='native',
                       least_significant_digit=None, fill_value=None,
                       chunk_cache=None, subarray_shape=np.array([]),
                       max_subarray_size=0):
        """Create a variable in the Dataset.  Add the CFAVariable structure to
        the Dataset by calling createVariable on self._cfa_dataset.
        """
        self._s3_variables[varname] = s3Variable()
        self._s3_variables[varname].create(
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
        return self._s3_variables[varname]

    def renameVariable(self, oldname, newname):
        """Rename the variable by overloading the base method."""
        # get the cfa root group
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            cfa_root_group = self._cfa_dataset.getGroup("root")
            cfa_root_group.renameVariable(oldname, newname)
        self._nc_dataset.renameVariable(oldname, newname)

    def createGroup(self, groupname):
        """Create a group.  If this file is a CFA file then create the CFAGroup
        as well."""
        self._s3_groups[groupname] = s3Group()
        self._s3_groups[groupname].create(self, groupname)
        return self._s3_groups[groupname]

    def renameGroup(self, oldname, newname):
        """Rename a group.  If this file is a CFA file then rename the CFAGroup
        as well."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            self._cfa_dataset.renameGroup(oldname, newname)
        self._nc_dataset.renameGroup(oldname, newname)

    def __getattr__(self, name):
        """Override the __getattr__ for the Dataset so as to return its
        private variables."""
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name in s3Dataset._private_atts:
            return self.__dict__[name]
        elif name == "groups":
            return self.groups
        elif name in netCDF4._private_atts:
            return self._nc_dataset.__getattr__(name)
        elif hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            return self._cfa_dataset.metadata[name]
        else:
            return self._nc_dataset.__getattr__(name)

    def __setattr__(self, name, value):
        """Override the __setattr__ for the Dataset so as to assign its
        private variables."""
        if name in s3Dataset._private_atts:
            self.__dict__[name] = value
        elif name in netCDF4._private_atts:
            self._nc_dataset.__setattr__(name, value)
        elif hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            self._cfa_dataset.metadata[name] = value
        else:
            self._nc_dataset.__setattr__(name, value)

    @property
    def groups(self):
        # if we are requesting the groups, and this is a cfa dataset then build
        # and return the dictionary of s3_groups
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            return self._s3_groups
        else:
            return self._nc_dataset.groups

    @property
    def variables(self):
        # if we are requesting the variables, and this is a cfa dataset then
        # build and return the dictionary of s3_variables
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            return self._s3_variables
        else:
            return self._nc_dataset.variables

    @property
    def dimensions(self):
        # if we are requesting the dimensions, and this is a cfa dataset then
        # build and return the dictionary of s3_dimensions
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            return self._s3_dimensions
        else:
            return self._nc_dataset.dimensions

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
            self._nc_dataset.delncattr(name, value)

    def getncattr(self, name):
        """Override getncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            try:
                return self._cfa_dataset.metadata[name]
            except KeyError:
                return self._nc_dataset.getncattr(name)
        else:
            return self._nc_dataset.getncattr(name)

    def ncattrs(self):
        """Override ncattrs function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            return self._cfa_dataset.metadata.keys()
        else:
            return self._nc_dataset.ncattrs()

    def setncattr(self, name, value):
        """Override setncattr function to manipulate the metadata dictionary,
        rather than the netCDF file.  The attributes are copied from the
        metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            self._cfa_dataset.metadata[name] = value
        else:
            self._nc_dataset.setncattr(name, value)

    def setncattr_string(self, name, value):
        """Override setncattr_string function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            self._cfa_dataset.metadata[name] = value
        else:
            self._nc_dataset.setncattr_string(name, value)

    def setncatts(self, attdict):
        """Override setncattrs function to manipulate the metadata
        dictionary, rather than the netCDF file.  The attributes are copied from
        the metadata dictionary on write."""
        if hasattr(self, "_cfa_dataset") and self._cfa_dataset:
            for k in attdict:
                self._cfa_dataset.metadata[k] = attdict[k]
        else:
            self._nc_dataset.setncatts(attdict)
