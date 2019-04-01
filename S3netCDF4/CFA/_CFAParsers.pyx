"""
   Collection of functions that parse files with embedded CFA metadata and
   return a hierarchy of objects instantiated from the _CFAClasses.
   See the class definitions and documentation in _CFAClasses.pyx for this
   hierarchy.

   See:
     http://www.met.reading.ac.uk/~david/cfa/0.4/index.html
   for the specification of the CFA conventions.
"""

from S3netCDF4.CFA._CFAExceptions import *
from S3netCDF4.CFA._CFAClasses import *

import json

def read_netCDF(nc_dataset):
    """Parse an already open netcdf_dataset to build the _CFAClasses hierarchy

    Args:
        netcdf_dataset (Dataset): the open dataset from the netcdf4-python
        library.

    Returns:
        CFADataset: The CFADataset object, populated with CFAGroups, which are
        in turn populated with CFADims and CFAVariables.
    """

    # check this is a CFA file
    if not "Conventions" in nc_dataset.ncattrs():
        raise CFAError("Not a CFA file.")
    if not "CFA" in nc_dataset.getncattr("Conventions"):
        raise CFAError("Not a CFA file.")

    # check to see if there are any groups and, if there is, create a CFAgroup
    # and add the nc_group to a dictionary of groups
    nc_groups = {}
    if len(nc_dataset.groups) != 0:
        for group in nc_dataset.groups:
            grp_name = group.name
            nc_groups[grp_name] = nc_dataset.group[grp_name]
    # if there isn't then create a root group that is the Dataset
    else:
        nc_groups["root"] = nc_dataset

    # create the CFADataset, with the metadata and format, and empty groups
    cfa_dataset = CFADataset(
                      name="",
                      format=nc_dataset.data_model,
                      metadata=dict(nc_dataset.__dict__)
                  )
    # now loop over all the groups, and add a CFAGroup to each dataset, then
    # the variables and dimensions contained in that group
    for grp_name in nc_groups:
        cfa_group = cfa_dataset.createGroup(grp_name)
        # next parse the dimensions
        for nc_dimname in nc_dataset.dimensions:
            # get the dimension's associated variable
            dim_var = nc_dataset.variables[nc_dimname]
            dim_dim = nc_dataset.dimensions[nc_dimname]
            # get the metadata
            nc_dim_atts = dict(dim_var.__dict__)
            # create the dimension and append to list of cfa_dims
            # is the dimension unlimited?
            unlimited = nc_dataset.dimensions[nc_dimname].isunlimited()
            cfa_dim = cfa_group.createDimension(
                          dim_name=nc_dimname,
                          dim_len=dim_dim.size,
                          metadata=nc_dim_atts
                        )

        # loop over the variables in the group / dataset
        for nc_varname in nc_groups[grp_name].variables:
            nc_var = nc_groups[grp_name].variables[nc_varname]
            nc_var_atts = dict(nc_var.__dict__)
            cfa_var = cfa_group.createVariable(
                          var_name=nc_varname,
                          nc_dtype=nc_var.dtype,
                          metadata=nc_var_atts
                        )
            cfa_var.parse(nc_var_atts)

    return cfa_dataset

def write_netCDF(cfa_dataset, nc_dataset):
    """Write the _CFAClasses hierarchy to an already open netcdf_dataset (opened
    with 'w' write flag).

    Args:
        cfa_dataset (CFADataset): the top class in the _CFAClasses hierarchy
        nc_dataset (Dataset): the open dataset from the netcdf4-python
        library.  Has to have been opened with 'w' flag.

    Returns:
        None
    """
    # set the global metadata
    nc_dataset.setncatts(cfa_dataset.getMetadata())
    # create the groups
    for group in cfa_dataset.getGroups():
        if (group != "root"):
            nc_group = nc_dataset.createGroup(group)
        else:
            nc_group = nc_dataset

        # get the actual group
        cfa_group = cfa_dataset.getGroup(group)
        # get the dimension names
        for dim in cfa_group.getDimensions():
            # get the actual dimension
            cfa_dim = cfa_group.getDimension(dim)
            # create the dimension in the netCDF file
            nc_group.createDimension(
                dimname=cfa_dim.getName(),
                size=cfa_dim.getLen()
            )

        for var in cfa_group.getVariables():
            # get the actual variable
            cfa_var = cfa_group.getVariable(var)
            # create the variable
            nc_var = nc_group.createVariable(
                varname=cfa_var.getName(),
                datatype=cfa_var.getType()
            )
            # get the variable metadata
            var_md = dict(cfa_var.getMetadata())
            # add the cfa metadata - if it is a cfa array
            if cfa_var.getRole() != "":
                var_md['cf_role'] = cfa_var.getRole()
                var_md['cfa_dimensions'] = " ".join(cfa_var.getDimensions())
                var_md['cfa_array'] = json.dumps(cfa_var.dump())
            # set the metadata
            nc_var.setncatts(var_md)
    nc_dataset.close()
