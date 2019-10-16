
"""See class diagrams and interactions in __init__.py"""

__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey and Matthew Jones"

import numpy as np
cimport numpy as np
from copy import copy
import json
import os.path
from collections import namedtuple

import netCDF4._netCDF4 as netCDF4

from S3netCDF4.CFA._CFAExceptions import *
from S3netCDF4.CFA._CFASplitter import CFASplitter

cdef class CFADataset:
    """
       Class containing details of a CFADataset (master array)
       +------------------------------------------------+
       | CFADataset                                     |
       +------------------------------------------------+
       | name               string                      |
       | format             string                      |
       | cfa_version        string                      |
       | cfa_groups         dict<CFAGroups>             |
       | metadata           dict<mixed>                 |
       +------------------------------------------------+
       | CFAGroup           createGroup(string grp_name,|
       |                                dict metadata)  |
       | CFAGroup           getGroup(string grp_name)   |
       | bool               renameGroup(string old_name,|
       |                                string new_name)|
       | list<basestring>   getGroups()                 |
       | string             getName()                   |
       | string             getFormat()                 |
       | basestring         getCFAVersion()             |
       | dict<mixed>        getMetadata()               |
       +------------------------------------------------+
    """

    cdef basestring name
    cdef basestring format
    cdef dict cfa_groups
    cdef dict metadata
    cdef basestring cfa_version

    def __init__(CFADataset self,
                 basestring name,
                 dict cfa_groups=dict(),
                 basestring format="NETCDF4",
                 basestring cfa_version="0.4",
                 dict metadata=dict()
                ):
        """
            Initialise a CFADataset object
            Args:
                name: the name of the dataset
                cfa_groups (dict): A dictionary containing the groups in the
                    Dataset.  There will always be at least one group.  If the
                    input data does not have groups, then a 'root' group should
                    be created.
                format (string): the format (datamodel) of the Dataset
                metadata (string): The metadata for the Dataset
            Returns:
                None
        """
        # have to initialise all member variables like this (as copies) to avoid
        # Python passing the variable by reference and then passing it back to
        # be altered by the calling function
        self.name = basestring(name)
        self.cfa_groups = dict(cfa_groups)
        self.format = basestring(format)
        self.metadata = dict(metadata)
        # set the CFA version - we need to ensure that, if the version is 0.5,
        # then a compatible NETCDF version has been chosen - i.e. NETCDF4
        if cfa_version[0:3] == "0.5" and format != "NETCDF4":
            raise CFAError(
                "Incompatible netCDF version ({}) for cfa_version ({})".format(
                    format, cfa_version
                )
            )
        self.cfa_version = basestring(cfa_version)

    def __repr__(CFADataset self):
        """String representation of the Dataset."""
        repstr = repr(type(self)) + " : format = {} : groups = {}\n".format(
            self.format,
            str([grp for grp in self.cfa_groups])
        )
        for md in self.metadata:
            repstr += "\t{} : {}\n".format(md, self.metadata[md])
        return repstr[:-1]

    def __getitem__(CFADataset self, basestring grp_name):
        """Overload the getitem to return a group"""
        return self.getGroup(grp_name)

    def __getattr__(CFADataset self, basestring name):
        """Overload the getattribute to return a group"""
        if name == "metadata":
            return self.metadata
        else:
            return self.getGroup(name)

    def __setattr__(CFADataset self, basestring grp_name, value):
        """Overload the setattribute to return an error"""
        raise CFAError("Not permitted.")

    cpdef CFAGroup createGroup(CFADataset self, basestring grp_name,
                               dict metadata=dict()):
        """Create a group with the name grp_name.

        Args:
            grp_name (string): the name of the group.
            metadata (dict)  : the metadata for the group

        Returns:
            CFAGroup: The instance of the group if created successfully.

        Exceptions:
            CFAError: if the group already exists
        """

        # Check that the group hasn't already been added
        if grp_name in self.cfa_groups:
            raise CFAGroupError((
                "Could not createGroup {}, group already exists"
            ).format(grp_name))
        # create the group and add it to the dictionary of groups
        self.cfa_groups[grp_name] = CFAGroup(
                                        grp_name,
                                        dataset=self,
                                        metadata=metadata
                                    )
        # return the group
        return self.cfa_groups[grp_name]

    cpdef CFAGroup getGroup(CFADataset self, basestring grp_name):
        """Get a group with the name grp_name.

        Args:
            grp_name (string): the name of the group.

        Returns:
            CFAGroup: The group identified by grp_name

        """

        try:
            return self.cfa_groups[grp_name]
        except KeyError as e:
            raise CFAGroupError((
                "Could not getGroup {}, group does not exist."
            ).format(grp_name))

    cpdef bint renameGroup(CFADataset self,
                           basestring old_name,
                           basestring new_name
                          ):
        """Rename a group from old_name to new_name.

        Args:
            old_name(string): the current name of the group.
            new_name(string): the new name of the group.

        Returns:
            bool: True if group renamed successfully, False otherwise.
        """

        try:
            # reasssign the key, then rename in CFAGroup datastructure
            self.cfa_groups[new_name] = self.cfa_groups.pop(old_name)
            self.cfa_groups[new_name].grp_name = new_name
        except KeyError as e:
            raise CFAGroupError((
                "Could not renameGroup {}, group does not exist."
            ).format(old_name))
        return True

    cpdef basestring getName(CFADataset self):
        """Get the name of the dataset.

        Returns:
            string: the name of the dataset
        """
        return self.name

    cpdef list getGroups(CFADataset self):
        """Get the name of all the groups for this CFADataset.

        Returns:
            list<string>: A list of all the names of the groups in this CFADataset
        """
        return [k for k in self.cfa_groups.keys()]

    cpdef dict getMetadata(CFADataset self):
        """Return the metadata for the CFADataset.

        Returns:
            dict<mixed>: The dictionary of metadata for the Dataset."""
        return self.metadata

    cpdef basestring getCFAVersion(CFADataset self):
        """Return the CFA version for the CFADataset.

        Returns:
            basestring: the CFA version of the Dataset."""
        return self.cfa_version

    cpdef basestring getFormat(CFADataset self):
        """Return the file format for the CFADataset.

        Returns:
            string: the file format of the CFADataset"""
        return self.format


cdef class CFAGroup:
    """
        Class containing details of a CFAGroup (master array grouping)
        +------------------------------------------------+
        | CFAGroup                                       |
        +------------------------------------------------+
        | CFADataset      dataset                        |
        | cfa_dims        dict<CFADim>                   |
        | grp_name        string                         |
        | metadata        dict<mixed>                    |
        | cfa_vars        dict<CFAVariable>              |
        +------------------------------------------------+
        | CFAVariable createVariable(string var_name,    |
        |         np.dtype nc_dtype,                     |
        |         list<string> dim_names=[],             |
        |         np.ndarray subarray_shape=np.array([]),|
        |         int max_subarray_size=0,               |
        |         dict<mixed> metadata={})               |
        | CFAVariable getVariable(string var_name)       |
        | list<basestring>    getVariables()             |
        | bool        renameVariable(string old_name,    |
        |                            string new_name)    |
        |                                                |
        | CFADim      createDimension(string dim_name,   |
        |                           int dim_len,         |
        |                           dict<mixed>metadata, |
        |                           <type> type)         |
        | CFADim       getDimension(string dim_name)     |
        | iterator<CFADim> getDimensions()               |
        | bool        renameDimension(string old_name,   |
        |                             string new_name)   |
        |                                                |
        | string      getName()                          |
        | dict<mixed> getMetadata()                      |
        | CFADataset  getDataset()                       |
        +------------------------------------------------+
    """

    cdef basestring grp_name
    cdef CFADataset dataset
    cdef dict metadata
    cdef dict cfa_vars
    cdef dict cfa_dims

    def __init__(CFAGroup self,
                 basestring group_name,
                 dataset=None,
                 dict cfa_dims=dict(),
                 dict cfa_vars=dict(),
                 dict metadata=dict()
                ):
        """Initialise a CFAGroups object"""
        # have to initialise like this to avoid storing a reference to the
        # variable that is passed in
        self.grp_name = basestring(group_name)
        self.dataset = dataset      # it's okay to store a reference to this!
        self.metadata = dict(metadata)
        self.cfa_dims = dict(cfa_dims)
        self.cfa_vars = dict(cfa_vars)

    def __repr__(CFAGroup self):
        """String representation of the CFAGroup."""
        repstr = repr(type(self)) + " : name = {} : dimensions = {} : variables = {}\n".format(
            self.grp_name,
            str([dim for dim in self.cfa_dims]),
            str([var for var in self.cfa_vars])
        )
        for md in self.metadata:
            repstr += "\t{} : {}\n".format(md, self.metadata[md])
        return repstr[:-1]

    def __getitem__(CFAGroup self, basestring name):
        """Overload getitem to behave as getattr"""
        if name == "metadata":
            return self.metadata
        else:
            return self.__getattr__(name)

    def __getattr__(CFAGroup self, basestring name):
        """Return the variable or dimension with the name of varname"""
        if name == "shape":
            return self.shape
        elif name == "metadata":
            return self.metadata
        elif name in self.cfa_vars:
            return self.getVariable(name)
        elif name in self.cfa_dims:
            return self.getDimension(name)
        else:
            raise CFAGroupError("{} is neither a variable nor a dimension"
                                "".format(name))

    cpdef _ensure_axis_has_type(CFAGroup self, basestring var_name):
        """Before calling the splitting algorithm (which may be called by
        createVariable), we need to assign the axis types to the dimensions,
        if the axis type is undefined (=="U").
         These axis types can be:
           T - time - (axis="T", name="t??" or name contains "time")
           Z - level - (axis="Z", name="z??" or name contains "level")
           Y - y axis / latitude (axis="Y", name="lat*" or name="y??")
           X - x axis / longitude (axis="X", name="lon*" or name="x??")
           N - not defined."""
        # loop over the cfa_dims
        dimensions = self.getDimensions()
        for d in dimensions:
            # get the dimension and the dimension variable
            dim = self.getDimension(d)
            axis_type = dim.getAxisType()
            if axis_type != "U":
                continue
            dim_md = dim.getMetadata()
            if "axis" in dim_md and dim_md["axis"] in ["T", "Z", "Y", "X", "N"]:
                dim.axis_type = dim_md["axis"]
            # no variable, or no axis attribute so we're going to guess from the
            # name of the dimension
            elif (len(d) < 3 and d[0] == "t") or "time" in d:
                dim.axis_type = "T"
            elif (len(d) < 3 and d[0] == "z") or "level" in d:
                dim.axis_type = "Z"
            elif (len(d) < 3 and d[0] == "y") or d[0:3] == "lat":
                dim.axis_type = "Y"
            elif (len(d) < 3 and d[0] == "x") or d[0:3] == "lon":
                dim.axis_type = "X"
            else:
                dim.axis_type = "N"

    cpdef CFAVariable createVariable(CFAGroup self,
                         basestring var_name,
                         np.dtype nc_dtype,
                         list dim_names=list(),
                         np.ndarray subarray_shape=np.array([]),
                         int max_subarray_size=0,
                         dict metadata=dict()
                        ):
        """Create a CFA variable and add it to the group.

        Args:
            var_name (string): the name of the variable
            nc_dtype (np.dtype): the type of the variable, as a numpy array
              data type.
            dim_names (list): a list of the dimension names that are present
              in the cfa_group. If this is not a zero length list and if
              subarray_shape is a zero length array then the array-splitter
              will be called to determine the shape of the sub arrays.
            subarray_shape (np.array): the shape of the subarrays.  If this is
              not a zero length array and if shape is not a zero length array
              then the subarrays will be created with this shape.
            max_subarray_size (int): the maximum size of each of the subarrays,
              size is the number of elements in the array.
            metadata (dict): the dictionary of non-CFA metadata, e.g. netCDF
              attributes.

        Returns:
            CFAVariable: the new CFA variable."""

        # Check that the variable hasn't already been added
        if var_name in self.cfa_vars:
            raise CFAVariableError((
                "Could not createVariable {}, variable already exists"
            ).format(var_name))

        # ensure that the axes have a type associated with them before running
        # the splitting algorithm
        self._ensure_axis_has_type(var_name)

        # There are three cases for creating the CFA subarray:
        # 1. No dimensions are given - the array is just created and returned.
        #   Parse may be called later on the array to create the subarrays from
        #   a file.
        # 2. Dimensions are given but subarray_shape is not given - the array
        #   splitter is called to determine the shape of the subarrays and the
        #   subarrays are created.
        # 3. Dimensions are given and subarray_shape is also given - the
        #   subarrays are created with this shape.  The array splitter is *not*
        #   called.

        # check that the dimensions exist in the cfa_dims dict
        for dim in dim_names:
            if dim not in self.cfa_dims:
                raise CFADimensionError(
                    "Dimension: {} does not exist in CFA group: {}. Please "
                    "create the dimension using CFAGroup::createDimension".format(
                        dim, self.grp_name
                    )
                )

        # check that if subarray_shape is specified then shape is also specified
        if subarray_shape.size != 0:
            if len(dim_names) == 0:
                raise CFASubArrayError(
                    "Dimnensions of CFAVariable are not specified but the"
                    " subarray_shape is specified. This is not possible."
                    " Please specify the dimensions of the CFAVariable."
                )
            else:
                # check the size of the specified subarray
                if len(subarray_shape) != len(dim_names):
                    raise CFASubArrayError(
                        "Number of dimensions in subarray_shape does not match"
                        " those in shape."
                    )
                # check each dimension is not longer than that in array
                for i in range(0, len(subarray_shape)):
                    if subarray_shape[i] > self.cfa_dims[self.cfa_dims.keys()[i]].getLen():
                        raise CFASubArrayError(
                            "Dimension in desired sub_array is larger than"
                            " dimension in array for dimension: {}".format(
                                self.cfa_dims.keys()[i]
                            )
                        )

        if len(dim_names) == 0:
            # create the variable and add it to the dictionary of variables
            self.cfa_vars[var_name] = CFAVariable(
                        var_name=var_name,
                        nc_dtype=nc_dtype,
                        group=self,
                        metadata=metadata
                      )
        else:
            # get the shape and axis types from the dimensions
            shape = []
            axis_types = []
            # assign axis types
            for dim_name in dim_names:
                dim = self.cfa_dims[dim_name]
                shape.append(dim.getLen())
                axis_type = dim.getAxisType()
                axis_types.append(axis_type)

            # create the splitter, even if the subarray shape is known
            cfa_splitter = CFASplitter(
                                shape=np.array(shape),
                                max_subarray_size=max_subarray_size,
                                axis_types=axis_types
                            )
            if subarray_shape.size == 0:
                # run the splitter
                subarray_shape = cfa_splitter.calculateSubarrayShape()
            else:
                cfa_splitter.setSubarrayShape(subarray_shape)

            # calculate the partition matrix shape
            pmshape = (shape / subarray_shape).astype(np.int32)

            # create the new variable
            self.cfa_vars[var_name] = CFAVariable(
                var_name=var_name,
                nc_dtype=nc_dtype,
                group=self,
                metadata=metadata,
                cf_role="cfa_variable",
                cfa_dimensions=dim_names,
                pmdimensions=dim_names,
                pmshape=pmshape,
            )

        return self.cfa_vars[var_name]

    cpdef CFAVariable getVariable(CFAGroup self, basestring var_name):
        """Get a CFA variable by name."""
        try:
            return self.cfa_vars[var_name]
        except KeyError as e:
            raise CFAVariableError(
                "Could not getVariable {}, variable does not exist.".format(
                    var_name)
            )

    cpdef list getVariables(CFAGroup self):
        """Get the name of all the variables for this Group."""
        return [k for k in self.cfa_vars.keys()]

    cpdef bint renameVariable(CFAGroup self,
                              basestring old_name,
                              basestring new_name
                             ):
        """Rename a variable from old_name to new_name.

        Args:
            old_name(string): the current name of the variable.
            new_name(string): the new name of the variable.

        Returns:
            bool: True if variable renamed successfully, False otherwise.
        """

        try:
            # reasssign the key, then rename in CFAGroup datastructure
            self.cfa_vars[new_name] = self.cfa_vars.pop(old_name)
            self.cfa_vars[new_name].var_name = new_name
        except KeyError as e:
            raise CFAVariableError((
                "Could not renameVariable {}, variable does not exist."
            ).format(old_name))
        return True

    cpdef CFADimension createDimension(CFAGroup self,
                          basestring dim_name="",
                          int dim_len=-1,
                          basestring axis_type="U",
                          dict metadata=dict()
                         ):
        """Create a CFA dimension and add it to the group"""

        # Check that the group hasn't already been added
        if dim_name in self.cfa_dims:
            raise CFADimensionError((
                "Could not createDimension {}, dimension already exists"
            ).format(dim_name))
        # create the group and add it to the dictionary of groups
        self.cfa_dims[dim_name] = CFADimension(
                    dim_name=dim_name,
                    dim_len=dim_len,
                    axis_type=axis_type,
                    metadata=metadata
                  )

        return self.cfa_dims[dim_name]

    cpdef CFADimension getDimension(CFAGroup self, basestring dim_name):
        """Get a CFA diimension by name."""
        try:
            return self.cfa_dims[dim_name]
        except KeyError as e:
            raise CFADimensionError((
                "Could not getDimension {}, dimension does not exist."
            ).format(dim_name))

    cpdef list getDimensions(CFAGroup self):
        """Get the name of all the dimensions for this Group."""
        return [k for k in self.cfa_dims.keys()]

    cpdef bint renameDimension(CFAGroup self,
                               basestring old_name,
                               basestring new_name
                              ):
        """Rename a dimension from old_name to new_name.

        Args:
            old_name(string): the current name of the dimension.
            new_name(string): the new name of the dimension.

        Returns:
            bool: True if dimension renamed successfully, False otherwise.
        """

        try:
            # reasssign the key, then rename in CFAGroup datastructure
            self.cfa_dims[new_name] = self.cfa_dims.pop(old_name)
            self.cfa_dims[new_name].dim_name = new_name
        except KeyError as e:
            raise CFADimensionError((
                "Could not renameDimension {}, dimension does not exist."
            ).format(old_name))
        return True

    cpdef dict getMetadata(CFAGroup self):
        """Return the metadata for the Group."""
        return self.metadata

    cpdef basestring getName(CFAGroup self):
        """Get the name of the group."""
        return self.grp_name

    cpdef CFADataset getDataset(CFAGroup self):
        """Get the CFADataset that the group belongs to."""
        return self.dataset

# construct the named tuple for the CFAPartition - this replaces a class in an
# earlier version
CFAPartition = namedtuple("CFAPartition",
                          "index location ncvar file format shape")

cdef class CFAVariable:
    """
        Class containing definition of a CFA Variable, containing CFASubarrays.
        +------------------------------------------------+
        | CFAVariable                                    |
        +------------------------------------------------+
        | CFAGroup       group                           |
        | var_name       string                          |
        | nc_dtype       np.dtype                        |
        | metadata       dict<mixed>                     |
        | cf_role        string                          |
        | cfa_dimensions list<string>                    |
        | pmdimensions   list<string>                    |
        | pmshape        array<int>                      |
        | base           string                          |
        | partitions     object                          |
        +------------------------------------------------+
        | string         getName()                       |
        | np.dtype       getType()                       |
        | dict<mixed>    getMetadata()                   |
        | list<string>   getDimensions()                 |
        | string         getRole()                       |
        | np.ndarray     shape()                         |
        | bool           parse(dict cfa_metadata)        |
        | dict<mixed>    dump()                          |
        | CFAPartition   getPartition(array<int> index)  |
        | array<CFAParti getPartitions(CFAVariable self) |
        | None           setPartitions(object)           |
        +------------------------------------------------+
    """

    cdef public basestring var_name
    cdef CFAGroup group
    cdef np.dtype nc_dtype
    cdef public dict metadata
    cdef basestring cf_role
    cdef list cfa_dimensions
    cdef list pmdimensions
    cdef np.ndarray pmshape
    cdef basestring base
    cdef np.ndarray _shape
    cdef object nc_partition_group
    cdef np.ndarray subarray_shape

    cfa_variable_metadata_keys = ["cf_role", "cfa_dimensions", "cfa_array"]

    def __init__(CFAVariable self,
                 basestring var_name,
                 np.dtype nc_dtype,
                 group=None,
                 basestring cf_role="",
                 list cfa_dimensions=list(),
                 list pmdimensions=list(),
                 np.ndarray pmshape=np.array([]),
                 basestring base="",
                 dict metadata=dict(),
                 object nc_partition_group=None,
                ):
        """Initialise a CFAVariable object"""
        # Initialise like this to avoid storing a reference to the variable
        # passed in
        self.var_name = basestring(var_name)
        self.nc_dtype = np.dtype(nc_dtype)
        self.group = group
        self.cf_role = basestring(cf_role)
        self.cfa_dimensions = list(cfa_dimensions)
        self.pmdimensions = list(pmdimensions)
        self.pmshape = np.array(pmshape)
        self.base = basestring(base)
        # set the nc_partition_group to be None in default
        self.nc_partition_group = nc_partition_group
        # we have to process the metadata to exclude the cfa directives
        self.metadata = dict()
        for k in metadata:
            if not k in CFAVariable.cfa_variable_metadata_keys:
                self.metadata[k] = metadata[k]

        self._shape = np.array([])

    def __repr__(CFAVariable self):
        """Return a string representing the variable"""
        repstr = repr(type(self)) + " : name = {} : dimensions = {} : shape = {}\n".format(
            self.var_name,
            str([dim for dim in self.cfa_dimensions]),
            str([s for s in self.shape()])
        )
        for md in self.metadata:
            repstr += "\t{} : {}\n".format(md, self.metadata[md])
        return repstr[:-1]

    def __getitem__(CFAVariable self, in_key):
        """Return all the subarrays required to take a slice out of the master
           array, the slice to take within the subarray and the position of that
           slice in the master array.

           Args:
               key: an index into the CFA master array

           Returns:
               list<tuple> A list of tuples containing the sub array info needed
                  The tuple consists of (file, var, slice, position) where:
                  file: string: the name of the file containing the subarray
                  var: string: the name of the variable in the file
                  slice: array: the index into the subarray file
                  position: array: the position in the master array file
        """
        cdef np.ndarray source_slice          # slice in the source partition
        cdef np.ndarray target_slice          # slice in the target master array
        cdef list return_list
        cdef np.ndarray slices                # don't use slices, use nx3
        cdef list slice_range                 # dimesional numpy arrays
        cdef list index_list                  # for speed!
        cdef list new_index_list
        cdef int s, x, n                      # iterator variables

        # try to get the length or convert to list
        try:
            key_l = len(in_key)
            key = in_key
        except:
            key_l = 1
            key = [in_key]

        # fill the slices from the 0 index upwards
        shape = self.shape()
        slices = np.empty([len(shape), 3], np.int32)
        for s in range(0, key_l):
            key_ts = type(key[s])
            if (key_ts in [int, np.int32, np.int64, np.int16, np.int8]):
                slices[s,:] = [key[s], key[s], 1]
            elif key_ts is slice:
                key_list = key[s].indices(shape[s])
                slices[s,:] = [key_list[0], key_list[1]-1, key_list[2]]
            else:
                raise CFAVariableIndexError(
                    "Cannot index CFA array with type: {} from {}".format(
                        key_ts, in_key
                    )
                )
        # fill in any other part that is not specified
        for s in range(key_l, len(shape)):
            slices[s] = [0, shape[s]-1, 1]
        # reset key_l as we have now filled the slices
        key_l = len(slices)
        # check the ranges of the slices
        for s in range(0, key_l):
            if (slices[s,0] < 0 or slices[s,0] >= shape[s] or
                slices[s,1] < 0 or slices[s,1] >= shape[s]):
                raise CFAVariableIndexError(
                    "Index into CFA array is out of range: {}".format(
                        in_key)
                    )

        # now we have the slices we can determine the partitions, using the
        # partition shape
        i_per_part = shape / self.pmshape
        slice_range = []
        for s in range(0, key_l):
            # append the start / stop of the partition indices for each axis
            slice_range.append(np.arange(int(slices[s,0] / i_per_part[s]),
                                         int(slices[s,1] / i_per_part[s])+1))
        """Generate all possible combinations of the indices.
        indices should contain an iterator (list, array, etc) of iterators,
        where each of the sub-iterators contains the indices required for that
        dimension.
        e.g.: indices = [np.arange(1,4),  # t axis for cf-netcdf file
                         np.arange(0,1),  # z axis
                         np.arange(6,9),  # y axis
                         np.arange(2,3)]  # x axis
        """

        index_list = []
        # build the first dimension
        for x in range(0, len(slice_range[0])):
            index_list.append([slice_range[0][x]])
        # loop over all of the other dimensions
        for n in range(1, len(slice_range)):
            new_index_list = []
            for x in range(0, len(index_list)):
                for y in range(0, len(slice_range[n])):
                    # make a copy of the index list
                    z = copy(index_list[x])
                    # append the value
                    z.append(slice_range[n][y])
                    # add to the new list
                    new_index_list.append(z)
            index_list = copy(new_index_list)

        # now get the partition filenames, with the variable and the source
        # and target slices necessary to copy a slice from a subarray to a
        # master array
        return_list = []
        return_type = namedtuple(
                        'return_type',
                        'partition source target'
                      )
        for i in index_list:
            partition = self.getPartition(i)
            # create the default source slice, this is the shape of the subarray
            ndims = len(partition.shape)
            source_slice = np.empty([ndims,3], np.int32)
            target_slice = np.empty([ndims,3], np.int32)
            # loop over all the slice dimensions - these should be equal between
            # the source_slice and target_slice
            for d in range(0, ndims):
                # create the target slice, this is the location of the partition
                # in the master array - we will modify both of these
                target_slice[d] = [partition.location[d,0],
                                   partition.location[d,1],
                                   1]
                source_slice[d] = [0,
                                   partition.shape[d],
                                   1]
                # rejig the target and source slices based on the input slices
                if slices[d,1] < target_slice[d,1]:
                    source_slice[d,1] -= target_slice[d,1] - slices[d,1]
                    target_slice[d,1] = slices[d,1]
                # adjust the target start and end for the sub slice
                target_slice[d,0] -= slices[d,0]
                target_slice[d,1] -= slices[d,0]
                # check if the slice started in the location
                if target_slice[d,0] < 0:
                    # source slice start is absolute value of target slice
                    source_slice[d,0] = -1 * target_slice[d,0]
                    # target start is 0
                    target_slice[d,0] = 0

            # append in order: filename, varname, source_slice, target_slice
            return_list.append(return_type(partition = partition,
                                           source = source_slice,
                                           target = target_slice)
                              )
        return return_list

    cpdef CFAGroup getGroup(CFAVariable self):
        """Return the parent CFAGroup of the CFAVariable."""
        return self.group

    cpdef basestring getName(CFAVariable self):
        """Return the name of the variable."""
        return self.var_name

    cpdef np.dtype getType(CFAVariable self):
        """Return the type of the variable."""
        return self.nc_dtype

    cpdef dict getMetadata(CFAVariable self):
        """Return the variable's metadata."""
        return self.metadata

    cpdef list getDimensions(CFAVariable self):
        """Return the variable's dimensions."""
        return self.pmdimensions

    cpdef basestring getRole(CFAVariable self):
        """Return the variables cfa role."""
        return self.cf_role

    cpdef np.ndarray shape(CFAVariable self):
        """Derive (from the dimensions) and return the shape of the variable."""
        # only do this if no cached version
        if self._shape.size == 0:
            self._shape = np.zeros(len(self.pmdimensions), np.int32)
            self._shape[:] = [self.getGroup().getDimension(d).getLen()
                              for d in self.getDimensions()]
        return self._shape

    cpdef getBaseFilename(CFAVariable self):
        """Create the file name path that is common between all subarray files"""
        # create the base filename
        file_path = self.getGroup().getDataset().getName()
        # trim the ".nc" off the end
        file_path = file_path.replace(".nc", "")
        # get just the filename, stripped of ".nc"
        file_name = os.path.basename(file_path).replace(".nc", "")
        # construct the base path of the subarray filenames
        base_filename = file_path + "/" + file_name
        # add the group if it is not root
        if self.getGroup().getName() != 'root':
            base_filename += "." + self.getGroup().getName()
        # add the varname
        base_filename += "." + self.var_name + "."

        return base_filename

    cpdef partitionIsDefined(CFAVariable self, index):
        """Return whether a partition is defined or not - i.e. has it been
        written to yet."""
        filename = self.nc_partition_group.variables["file"][index]
        return filename != ""

    cpdef np.ndarray getPartitionMatrixShape(CFAVariable self):
        """Get the partition matrix shape."""
        return self.pmshape

    cpdef list getPartitionMatrixDimensions(CFAVariable self):
        """Get the names of the dimensions in the partition matrix"""
        return self.pmdimensions

    cpdef object getPartition(CFAVariable self, index):
        """Get the partition at the index.  This is now autogenerated to reduce
        memory overhead and remove the need for parsing."""
        # get the list index from the partition look up table
        try:
            # autogen partition info - no need to call the writePartition method
            # until the file is going to be written
            index = tuple(index) # use a copy
            subarray_shape = self.shape() / self.pmshape
            location = np.empty((len(self.pmshape), 2), 'i')
            # calculate the start and end locations
            location[:,0] = (0.5+index[:] *
                            subarray_shape).astype('i')
            # -1 here as CFA expects indices to be *inclusive*
            location[:,1] = (0.5+index[:] *
                            subarray_shape +
                            subarray_shape -1).astype('i')
            # get the integer subarray shape for this actual partition
            # (add the 1 back on that was subtracted above)
            actual_subarray_shape = (location[:,1] - location[:,0]+1)

            # create the name of the subarray file with the numbering system
            # have to check if self.base_filename is None for an undefinied
            # partition
            filename = (self.getBaseFilename() +
                        ".".join([str(i) for i in index]) +
                        ".nc"
                       )
            part = CFAPartition(
                index = index,
                location = location,
                ncvar = self.var_name,
                file = filename,
                format = self.getGroup().getDataset().getFormat(),
                shape = actual_subarray_shape
            )

        except IndexError as ie:
            raise CFAPartitionIndexError(
                "No partition with index: {} exists for the variable: {}".format(
                    index, self.var_name
                )
            )
        return part

    cpdef writePartition(CFAVariable self, object partition):
        """write the partition to the nc_partition group."""
        # shortcut to the partition group
        ncp = self.nc_partition_group
        # write the partition info into the netCDF group representation of
        # a partition
        index = partition.index
        ncp.variables["index"][index] = partition.index
        ncp.variables["location"][index] =  partition.location
        ncp.variables["ncvar"][index] =  partition.ncvar
        ncp.variables["file"][index] = partition.file
        ncp.variables["format"][index] = partition.format
        ncp.variables["shape"][index] = partition.shape

    cpdef writeInitialPartitionInfo(CFAVariable self, basestring cfa_version,
                                    object nc_parent):
        """Set the partitions - this is a reference to a group contained in the
        netCDF master file (for CFA-0.5) or a dataset contained within memory
        (for CFA-0.4)"""
        # create the partition definition either in the file or in memory
        self.createNCPartition(cfa_version, nc_parent)

        # shortcut to the partition group
        ncp = self.nc_partition_group

        # set the pmshape
        ncp.variables["pmshape"][:] = self.pmshape
        # set the pmdimensions
        ncp.variables["pmdimensions"][0] = ", ".join(self.pmdimensions)

        # partitions are dynamically created now, so no need to call
        # writePartition repeatedly

    cpdef createNCPartition(CFAVariable self, cfa_version="0.4", nc_parent=None):
        """Create the datastructure in a netCDF file for the netCDF partition
        used internally and in CFA-0.5 files."""
        # get the dimension definitions from the CFAVar
        if cfa_version == "0.4":
            # create Dataset in memory
            self.nc_partition_group = netCDF4.Dataset(
                "inmemory.nc", mode="w", diskless=True, persist=False,
                memory=0
            )
        elif cfa_version == "0.5":
            # create group in file
            cfa_metagroup_name = "cfa_" + self.var_name
            self.nc_partition_group = nc_parent.createGroup(cfa_metagroup_name)
        else:
            raise CFAError("Unsupported CFA version {}.".format(cfa_version))

        # shortcut to partition group
        ncp = self.nc_partition_group
        pm_dimensions = self.getPartitionMatrixDimensions()
        pm_shape = self.getPartitionMatrixShape()
        # need to create the dimensions
        for d in range(0, len(pm_dimensions)):
            ncp.createDimension(pm_dimensions[d], pm_shape[d])
        n_pm_dims = len(pm_shape)
        # create additional dimensions for location, shape etc.
        # in the partition matrix
        ncp.createDimension("X", n_pm_dims)
        ncp.createDimension("Y", 2)
        # create the cfa variables the partitions
        ### pm_shape
        pm_shape_var = ncp.createVariable(
            "pmshape", np.int32, dimensions = ("X",)
        )
        # pm_dimensions - just write as string
        pm_dimensions_var = ncp.createVariable(
            "pmdimensions", str
        )
        ### index
        index_dim_names = list(pm_dimensions)
        index_dim_names.append("X")
        index_var = ncp.createVariable(
            "index", np.int32, dimensions=index_dim_names
        )
        ### location
        location_dim_names = list(pm_dimensions)
        location_dim_names.extend(["X", "Y"])
        location_var = ncp.createVariable(
            "location", np.int32, dimensions=location_dim_names
        )
        ### ncvar (name of)
        ncvar_var = ncp.createVariable(
            "ncvar", str, dimensions=pm_dimensions
        )
        ### filename
        file_var = ncp.createVariable(
            "file", str, dimensions=pm_dimensions
        )
        ### format
        format_var = ncp.createVariable(
            "format", str, dimensions=pm_dimensions
        )
        ### shape
        shape_dim_names = index_dim_names
        shape_var = ncp.createVariable(
            "shape", np.int32, dimensions=shape_dim_names
        )

    cpdef parse(CFAVariable self, dict cfa_metadata):
        """
        Parse the metadata and create the required member data and subarrays.
        This is for CFA version 0.4, which parses metadata held in the netCDF
        attribute - which is returned from the netCDF4 library as a dictionary.

        Args:
            cfa_metadata (dict): the dictionary of the metadata to parse into
            the CFA structures.

        Returns:
            bool: True if parsed successfully, False if it is not a cfa file.
        """
        # The dictionary contains three keys (and requires them):
        # check first if this is a cf_file - don't throw an exception if it's
        # not
        if not "cf_role" in cfa_metadata:
            self.cf_role = ""
            return False
        else:
            self.cf_role = cfa_metadata["cf_role"]

        # check that "cf_role", "cfa_dimensions" and "cfa_array" defined in
        # metadata
        if not ("cfa_dimensions" in cfa_metadata):
            raise CFADimensionError(
                      "cfa_dimensions or cf_dimensions not defined in {} "
                      "metadata".format(self.var_name)
            )

        if not "cfa_array" in cfa_metadata:
            raise CFAVariableError(
                      "cfa_array not defined in {} metadata".format(
                          self.var_name)
            )

        for md_key in cfa_metadata:
            # interpret the "cfa_metadata" key metadata
            if md_key == "cfa_array":
                # this is the main bulk of the cfa defintion -> we are going to
                # create CFA_Arrays from the partition information contained in this
                # chunk of metadata
                cfa_json = json.loads(cfa_metadata[md_key])
                # check that the partitions are defined in the JSON
                if not "Partitions" in cfa_json:
                    raise CFAPartitionError(
                              "Partitions not defined in {} metadata".format(
                                  self.var_name)
                    )
                # load all the data for this class - if it exists
                if "base" in cfa_json:
                    self.base = cfa_json["base"]
                if "pmshape" in cfa_json:
                    self.pmshape = np.array(cfa_json["pmshape"], dtype='i')
                if "pmdimensions" in cfa_json:
                    self.pmdimensions = cfa_json["pmdimensions"]

                # create a netCDF dataset in memory to store the variables
                # containing the partition information.  This is to ensure the
                # same code can be used for accessing partitions in CFA-0.4
                # format and CFA-0.5 format
                self.createNCPartition(cfa_version="0.4")
                # get a shortcut to the partition group
                ncp = self.nc_partition_group
                for p in cfa_json["Partitions"]:
                    # we require the index and location.  Allow KeyError
                    # exceptions to be thrown if they are not found
                    index = np.array(p["index"], 'i')
                    it = tuple(index)
                    ncp.variables["index"][it] = index
                    ncp.variables["location"][it] = p["location"]
                    ncp.variables["ncvar"][it] = p["subarray"]["ncvar"]
                    ncp.variables["file"][it] = p["subarray"]["file"]
                    ncp.variables["format"][it] = p["subarray"]["format"]
                    ncp.variables["shape"][it] = np.array(p["subarray"]["shape"])

            elif md_key == "cfa_dimensions" or md_key == "cf_dimensions":
                self.cfa_dimensions = cfa_metadata[md_key].split()
            elif md_key == "cf_role":
                self.cf_role = cfa_metadata[md_key]
            else:
                self.metadata[md_key] = cfa_metadata[md_key]

    cpdef dict dump(CFAVariable self):
        """Return the a dictionary representation of the CFAVariable so it can be
           added to the metadata for the variable later.
           Returns:
               dict: the JSON representation of the CFAVariable
        """
        # add the other (non-cfa) metadata
        output_dump = {k:self.metadata[k] for k in self.metadata}
        # only output partitions data if the variable has "cfa_variable" as its
        # value for the "cf_role" attribute
        if self.cf_role != "cfa_variable":
            return output_dump

        # below here is for cfa variables
        cfa_array_dict = {}
        if self.base != "":
            cfa_array_dict["base"] = self.base
        if self.pmshape.any():
            cfa_array_dict["pmshape"] = self.pmshape.tolist()
        if self.pmdimensions != []:
            cfa_array_dict["pmdimensions"] = self.pmdimensions

        # write out the partitions from the CFAPartition type
        partitions_list = []
        # shortcut to partition group
        ncp = self.nc_partition_group
        # iterate over the indices
        it = np.nditer(ncp.variables["ncvar"],
                       flags=["multi_index", "refs_ok"])
        #
        for i in it:
            ncvar  = ncp.variables["ncvar"][it.multi_index]
            # don't write out any undefinied partitions
            if ncvar == "":
                continue
            shape  = ncp.variables["shape"][it.multi_index]
            file   = ncp.variables["file"][it.multi_index]
            format = ncp.variables["format"][it.multi_index]
            location = ncp.variables["location"][it.multi_index]
            index = ncp.variables["index"][it.multi_index]
            # build the partition JSON with the subarray
            partition_dict = {
                              "subarray" : {
                                  "ncvar"  : ncvar,
                                  "file"   : file,
                                  "format" : format,
                                  "shape"  : shape.tolist()
                             },
                             "index" : index.tolist(),
                             "location" : location.tolist()
                            }
            # add to list of partitions
            partitions_list.append(partition_dict)
        cfa_array_dict["Partitions"] = partitions_list

        output_dump["cf_role"]       = self.cf_role,
        output_dump["cf_dimensions"] = " ".join(self.cfa_dimensions),
        output_dump["cfa_array"]     = cfa_array_dict
        return output_dump

    cpdef load(CFAVariable self, dict cfa_metadata, object nc_object):
        """Load the CFA Variable definition from a CFA 0.5 definition of the
        Partitions and Subarrays.  These are contained in a group named
        "cfa_<var_name>", in a variable called "<var_name>" with pmdimensions
        defined by the dimensions of the <var_name> variable and a pmshape
        derived from the size of these dimensions.
        nc_object is either a netCDF4.Dataset or netCDF4.Group"""
        # load the partition matrix variables from the cfa_metagroup in the
        # CFA-netCDF 0.5 file
        # The dictionary contains three keys (and requires them):
        # check first if this is a cf_file - don't throw an exception if it's
        # not
        if not "cf_role" in cfa_metadata:
            self.cf_role = ""
            return False
        else:
            self.cf_role = cfa_metadata["cf_role"]

        # check that "cf_role", "cfa_dimensions" and "cfa_array" defined in
        # metadata
        if not ("cfa_dimensions" in cfa_metadata):
            raise CFADimensionError(
                      "cfa_dimensions or cf_dimensions not defined in {} "
                      "metadata".format(self.var_name)
            )

        if not "cfa_group" in cfa_metadata:
            raise CFAVariableError(
                      "cfa_group not defined in {} metadata".format(
                          self.var_name)
            )
        else:
            # get the group containing the partition information
            self.nc_partition_group = nc_object.groups[cfa_metadata["cfa_group"]]

        # read the variables from the group
        if "base" in self.nc_partition_group.variables:
            self.base = self.nc_partition_group.variables["base"][:]

        if "pmshape" in self.nc_partition_group.variables:
            self.pmshape = self.nc_partition_group.variables["pmshape"][:]

        if "pmdimensions" in self.nc_partition_group.variables:
            pm_dimensions = self.nc_partition_group.variables["pmdimensions"][:]
            self.pmdimensions = pm_dimensions.replace(" ", "").split(",")

cdef class CFADimension:
    """
        Class containing definition of a CFA Dimension.
        +------------------------------------------------+
        | CFADimension                                   |
        +------------------------------------------------+
        | dim_name         string                        |
        | dim_len          int                           |
        | metadata         dict<mixed>                   |
        | type             np.dtype                      |
        | axis_type        string                        |
        +------------------------------------------------+
        | dict<mixed>      dump()                        |
        | string           getName()                     |
        | dict<mixed>      getMetadata()                 |
        | int              getLen()                      |
        | string           getAxisType()                 |
        +------------------------------------------------+
    """

    cdef public basestring dim_name
    cdef int dim_len
    cdef public dict metadata
    cdef public basestring axis_type

    def __init__(CFADimension self,
                 basestring dim_name="",
                 int dim_len=-1,
                 basestring axis_type="U",
                 dict metadata=dict()
                ):
        """Initialise the CFADim object"""
        self.dim_name = basestring(dim_name)
        self.dim_len = int(dim_len)
        self.metadata = dict(metadata)
        self.axis_type = basestring(axis_type)

    def __repr__(CFADimension self):
        """String representation of the CFADimension."""
        repstr = repr(type(self)) + (" : name = {} : length = {} : "
                      "axis_type = {}\n").format(
                          self.dim_name,
                          self.dim_len,
                          self.axis_type
                      )
        for md in self.metadata:
            repstr += "\t{} : {}\n".format(md, self.metadata[md])
        return repstr[:-1]

    cpdef dict dump(CFADimension self):
        """Return a dictionary representation of the CFADim
           Returns:
               dict: the JSON representation of the CFADimension
        """
        output_dump = {"dim_name" : self.dim_name,
                       "dim_len"  : self.dim_len}
        # add the other metadata
        for k in self.metadata:
            output_dump[k] = self.metadata[k]
        return output_dump

    cpdef basestring getName(CFADimension self):
        """Return the name of the dimension."""
        return self.dim_name

    cpdef dict getMetadata(CFADimension self):
        """Return a dictionary of the metadata for the dimension"""
        return self.metadata

    cpdef int getLen(CFADimension self):
        """Return the length of the dimension"""
        return self.dim_len

    cpdef basestring getAxisType(CFADimension self):
        """Return the axis type, this should be one of:
            X - X axis (e.g. longitude)
            Y - Y axis (e.g. latitude)
            Z - Z axis (e.g. height above sea-level)
            T - Time axis
            N - None of the above axis (e.g. ensemble member)
        """
        return self.axis_type
