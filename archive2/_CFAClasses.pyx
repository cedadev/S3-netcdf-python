"""
   Classes containing the structure of CFA-netCDF files (master array) and the CF-netcdf subarray files.
   See:
     http://www.met.reading.ac.uk/~david/cfa/0.4/index.html
   for the specification of the CFA conventions.

   Only a subset of the CFA-netCDF specification is implemented - just what we use to fragment the files
   to store as multiple objects on the object storage.

   The classes here are organised to reflect the implied hierarchy in the CFA conventions :
   (NC = netCDF)

   +-------------------------------+                +-------------------------------+
   | CFAFile                       |        +------>| CFADim                        |
   +-------------------------------+        |       +-------------------------------+
   | file_name       string        |        |       | dim_name         string       |
   | nc_dims         [CFADim]      |--------+       | metadata         {}           |
   | global_metadata {}            |                | values           numpy[float] |
   | cfa_metadata    {}            |                +-------------------------------+
   | variables       [CFAVariable] |--------+
   +-------------------------------+        |
                                            |       +-------------------------------+
                                            +------>| CFAVariable                   |
                                                    +-------------------------------+
                                                    | var_name       string         |
                                                    | metadata       {}             |
                                                    | pmdimensions   [string]       |
                                                    | pmshape        [int]          |
                                                    | base           string         |
                                            +-------| partitions     [CFAPartition] |
                                            |       +-------------------------------+
   +-------------------------------+        |
   | CFAPartition                  |<-------+
   +-------------------------------+
   | index           [int]         |
   | location        [int]         |
   | subarray        CFASubArray   |--------+
   +-------------------------------+        |
                                            |       +-------------------------------+
                                            +------>| CFASubarray                   |
                                                    +-------------------------------+
                                                    | ncvar          string         |
                                                    | file           string         |
                                                    | format         string         |
                                                    | shape          [int]          |
                                                    +-------------------------------+
"""

import numpy as np
cimport numpy as np

import json
from collections import OrderedDict

class CFAException(BaseException):
    pass


cdef class CFAFile:
    """
       Class containing details of a CFAFile (master array)
    """

    cdef public nc_dims
    cdef public global_metadata
    cdef public cfa_metadata
    cdef public variables

    def __init__(self, nc_dims = [], global_metadata = {},
                       cfa_metadata = {}, variables = OrderedDict()):
        """Initialise the CFAFile class"""
        self.nc_dims = nc_dims
        self.global_metadata = global_metadata
        self.cfa_metadata = cfa_metadata
        self.variables = variables


    def Parse(self, nc_dataset):
        """ Parse a netCDF dataset to create the CFA class structures"""
        # first get the global metadata
        self.global_metadata = {k: nc_dataset.getncattr(k) for k in nc_dataset.ncattrs()}
        # check this is a CFA file
        if not "Conventions" in self.global_metadata:
            raise CFAException("Not a CFA file.")
        if not "CFA" in self.global_metadata["Conventions"]:
            raise CFAException("Not a CFA file.")

        # next parse the dimensions
        for d in nc_dataset.dimensions:
            # get the dimension's associated variable
            dim_var = nc_dataset.variables[d]
            # get the attributes as a dictionary
            dim_meta = {k: dim_var.getncattr(k) for k in dim_var.ncattrs()}
            # create the dimension and append to list of nc_dims
            self.nc_dims.append(CFADim(dim_name=d, metadata=dim_meta,
                                       type=dim_var.dtype, values=dim_var[:]))

        # next get the variables
        self.variables = OrderedDict()
        for v in nc_dataset.variables:
            # check that this variable has a cf_role
            if "cf_role" in nc_dataset.variables[v].ncattrs():
                # create and append the CFAVariable
                cfa_var = CFAVariable()
                cfa_var.Parse(nc_dataset.variables[v])
                self.variables[v] = cfa_var


cdef class CFADim:
    """
       Class containing details of a dimension in a CFAFile
    """

    cdef public basestring dim_name
    cdef public metadata
    cdef public type
    cdef public np.ndarray values

    def __init__(self, dim_name = None, metadata = {}, type = None, values = []):
        """Initialise the CFADim object"""
        self.dim_name = dim_name
        self.metadata = metadata
        self.type = type
        self.values = np.ndarray(values, dtype=type)


cdef class CFAVariable:
    """
       Class containing details of the variables in a CFAFile
    """

    cdef public basestring var_name
    cdef public metadata
    cdef public cf_role
    cdef public cfa_dimensions
    cdef public np.ndarray pmdimensions
    cdef public np.ndarray pmshape
    cdef public basestring base
    cdef public partitions

    def __init__(self, var_name = "", metadata = {},
                       cf_role="", cfa_dimensions = [],
                       pmdimensions = [], pmshape = [],
                       base = "", partitions = []):
        """Initialise the CFAVariable object"""
        self.var_name = var_name
        self.metadata = metadata
        self.cf_role = cf_role
        self.cfa_dimensions = cfa_dimensions
        self.pmdimensions = np.ndarray(pmdimensions, dtype='i')
        self.pmshape = np.ndarray(pmshape, dtype='i')
        self.base = base
        self.partitions = partitions

    def Parse(self, nc_var):
        """Parse a netCDF variable that contains CFA metadata"""
        self.var_name = nc_var.name

        # Parse the metadata, rather than just copying it
        # We will interpret the cfa metadata but just copy the other metadata
        self.metadata = {}

        # check that it is a CFAVariable - i.e. the metadata is correctly defined
        nc_var_atts = nc_var.ncattrs()
        if not "cf_role" in nc_var_atts:
            raise CFAException("cf_role not defined in %s metadata" % nc_var.name)
        if not ("cfa_dimensions" in nc_var_atts or "cf_dimensions" in nc_var_atts):
            raise CFAException("cfa_dimensions or cf_dimensions not defined in %s metadata" % nc_var.name)
        if not "cfa_array" in nc_var_atts:
            raise CFAException("cfa_array not defined in %s metadata" % nc_var.name)

        for k in nc_var_atts:
            # cf_role
            if k == "cf_role":
                self.cf_role = nc_var.getncattr(k)
            # cfa_dimensions
            elif k == "cfa_dimensions" or k == "cf_dimensions":
                self.cfa_dimensions = nc_var.getncattr(k).split()
            # cfa_array
            elif k == "cfa_array":
                # cfa is a chunk of JSON so load it
                cfa_json = json.loads(nc_var.getncattr(k))
                # check that the partitions are defined in the JSON
                if not "Partitions" in cfa_json:
                    raise CFAException("Partitions not defined in %s:cfa_array metadata" % nc_var.name)
                # load all the data for this class - if it exists
                if "base" in cfa_json:
                    self.base = cfa_json["base"]
                if "pmshape" in cfa_json:
                    self.pmshape = np.ndarray(cfa_json["pmshape"], dtype='i')
                if "pmdimensions" in cfa_json:
                    self.pmdimensions = np.ndarray(cfa_json["pmdimensions"], dtype='i')
                for p in cfa_json["Partitions"]:
                    cfa_part = CFAPartition()
                    cfa_part.Parse(p)
                    self.partitions.append(cfa_part)
            else:
                self.metadata[k] = nc_var.getncattr(k)

cdef class CFAPartition:
    """
       Class containing details of the partitions in a CFAVariable
    """

    cdef public np.ndarray index
    cdef public np.ndarray location
    cdef public subarray

    def __init__(self, index = [], location = [], subarray = None):
        """Initialise the CFAPartition object"""
        self.index = np.ndarray(index, dtype='i')
        self.location = np.ndarray(location, dtype='i')
        self.subarray = subarray

    def Parse(self, part):
        """Parse a partition definition from the metadata."""
        # Check that the "subarray" item exists in the metadata
        if not "subarray" in part:
            raise CFAException("subarray not defined in cfa_array:Partition metadata")
        # Check index and subarray in JSON / metadata
        if "index" in part:
            self.index = np.array(part["index"], 'i')
        if "location" in part:
            self.index = np.array(part["location"], 'i')
        cfa_subarray = CFASubarray()
        cfa_subarray.Parse(part["subarray"])
        self.subarray = cfa_subarray


cdef class CFASubarray:
    """
       Class containing details of a subarray in a CFAPartition
    """

    cdef public basestring ncvar
    cdef public basestring file
    cdef public basestring format
    cdef public np.ndarray shape

    def __init__(self, ncvar = "", file = "", format = "", shape = []):
        """Initialise the CFASubarray object"""
        self.ncvar = ncvar
        self.file = file
        self.format = format
        self.shape = np.ndarray(shape, dtype='i')


    def Parse(self, subarray):
        """Parse the cfa_subarray member of the Partition metadata"""
        # the only item which has to be present is shape
        if not "shape" in subarray:
            raise CFAException("shape not defined in Partition:subarray metadata")
        if "ncvar" in subarray:
            self.ncvar = subarray["ncvar"]
        if "file" in subarray:
            self.file = subarray["file"]
        if "format" in subarray:
            self.format = subarray["format"]
        self.shape = np.array(subarray["shape"], 'i')
