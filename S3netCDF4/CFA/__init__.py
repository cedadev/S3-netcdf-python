"""
   Classes containing the structure of CFA-netCDF files (master array) and the
   CF-netcdf subarray files.
   See:
     http://www.met.reading.ac.uk/~david/cfa/0.4/index.html
   for the specification of the CFA conventions.

   Only a subset of the CFA-netCDF specification is implemented - just what we
   use to fragment the files to store as multiple objects on the object storage.

   The classes here are organised to reflect the implied hierarchy in the CFA
   conventions :
   (NC = netCDF)

    +------------------------------------------------+
    | CFADataset                                     |
    +------------------------------------------------+
    | format             string                      |
    | metadata           dict<mixed>                 |
    | cfa_groups         dict<CFAGroups>             |
    +------------------------------------------------+
    | bool               createGroup(string grp_name)|
    | CFAGroup           getGroup(string grp_name)   |
    | bool               renameGroup(string old_name,|
    |                                string new_name)|
    | list<string>       getGroups()                 |
    | dict<mixed>        getMetadata()               |
    +------------------------------------------------+
                   |
                   |
                   |
    +------------------------------------------------+
    | CFAGroup                                       |
    +------------------------------------------------+
    | cfa_dims        dict<CFADim>                   |
    | grp_name        string                         |
    | metadata        dict<mixed>                    |
    | cfa_vars        dict<CFAVariable>              |
    +------------------------------------------------+
    | CFAVariable  createVariable(string var_name,   |
    |                     array<int> shape,          |
    |                     np.dtype dtype,            |
    |                     list<string> dim_names     |
    |                     dict<mixed> metadata)      |
    | CFAVariable  getVariable(string var_name)      |
    | list<string> getVariables()                    |
    | bool         renameVariable(string old_name,   |
    |                            string new_name)    |
    |                                                |
    | CFADim       createDimension(string dim_name,  |
    |                           int len,             |
    |                           dict<mixed>metadata) |
    | CFADim       getDimension(string dim_name)     |
    | list<string> getDimensions()                   |
    | bool         renameDimension(string old_name,  |
    |                             string new_name)   |
    |                                                |
    | string       getName()                         |
    | dict<mixed>  getMetadata()                     |
    +------------------------------------------------+
                   |
                   +--------------------------------------------------------------+
                   |                                                              |
    +------------------------------------------------+             +------------------------------------------------+
    | CFAVariable                                    |             | CFADim                                         |
    +------------------------------------------------+             +------------------------------------------------+
    | var_name       string                          |             | dim_name         string                        |
    | metadata       dict<mixed>                     |             | dim_len          int                           |
    | cf_role        string                          |             | metadata         dict<mixed>                   |
    | pmdimensions   array<string>                   |             +------------------------------------------------+
    | pmshape        array<int>                      |             | string           getName()                     |
    | base           string                          |             | dict<mixed>      getMetadata()                 |
    | partitions     array<CFAPartition>             |             | array<int>       getIndices()                  |
    +------------------------------------------------+             |                                                |
    | string         getName()                       |             +------------------------------------------------+
    | dict<mixed>    getMetadata()                   |
    | list<string>   getDimensions()                 |
    | bool           parse(dict cfa_metadata)        |
    | CFAPartition   getPartition(array<int> index)  |
    | list<CFAPartition> getPartitions()             |
    +------------------------------------------------+
                    |
                    |
                    |
    +------------------------------------------------+
    | CFAPartition                                   |
    +------------------------------------------------+
    | array<int>    index                            |
    | array<int>    location                         |
    | CFASubArray   subarray                         |
    +------------------------------------------------+
    | bool          parse(dict cfa_metadata)         |
    | array<int>    getIndex()                       |
    | array<int>    getLocation()                    |
    | CFASubarray   getSubArray()                    |
    +------------------------------------------------+
                    |
                    |
                    |
    +------------------------------------------------+
    | CFASubarray                                    |
    +------------------------------------------------+
    | ncvar          string                          |
    | file           string                          |
    | format         string                          |
    | shape          array<int>                      |
    +------------------------------------------------+
    | bool           parse(dict cfa_metadata)        |
    | string         getncVar()                      |
    | string         getFile()                       |
    | string         getFormat()                     |
    | array<int>     getShape()                      |
    +------------------------------------------------+
"""

import pyximport
import numpy as np
import os

os.environ["C_INCLUDE_PATH"] = np.get_include()
pyximport.install(
    setup_args={'include_dirs': np.get_include()},
    language_level=3,
    )

#NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
