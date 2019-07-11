"""
   Parser to read / write CFA metadata from / to a netCDF file.

   See:
     http://www.met.reading.ac.uk/~david/cfa/0.4/index.html
   for the specification of the CFA conventions.
"""

from S3netCDF4.CFA._CFAExceptions import *
from S3netCDF4.CFA._CFAClasses import *
import netCDF4._netCDF4 as netCDF4
import posixpath
import json

from _CFAParser import CFA_Parser

class CFA_netCDFParser(CFA_Parser):

    def __init__(self):
        """Do nothing, but don't call the base class as that will raise
        NotImplementedError"""
        pass

    def read(self, nc_dataset):
        """Parse an already open netcdf_dataset to build the _CFAClasses
        hierarchy.

        Args:
            netcdf_dataset (Dataset): the open dataset from the netcdf4-python
            library.

        Returns:
            CFADataset: The CFADataset object, populated with CFAGroups, which
            are in turn populated with CFADims and CFAVariables.
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

    def write(self, cfa_dataset, nc_dataset):
        """Write the _CFAClasses hierarchy to an already open netcdf_dataset
        (opened with 'w' write flag).

        Args:
            cfa_dataset (CFADataset): the top class in the _CFAClasses hierarchy
            nc_dataset (Dataset): the open dataset from the netcdf4-python
            library.  Has to have been opened with 'w' flag.

        Returns:
            None
        """
        # set the global metadata
        netCDF4.Dataset.setncatts(nc_dataset, cfa_dataset.getMetadata())
        # get the groups
        for group in cfa_dataset.getGroups():
            if (group == "root"):
                nc_group = nc_dataset
            else:
                nc_group = nc_dataset.groups[group]

            # get the actual group
            cfa_group = cfa_dataset.getGroup(group)
            # set the metadata for the group
            netCDF4.Group.setncatts(nc_group, cfa_group.getMetadata())

            for var in cfa_group.getVariables():
                # get the actual variable
                cfa_var = cfa_group.getVariable(var)
                # get the variable
                nc_var = nc_group.variables[cfa_var.getName()]
                # get the variable metadata
                var_md = dict(cfa_var.getMetadata())
                # add the cfa metadata - if it is a cfa array
                if cfa_var.getRole() != "":
                    var_md['cf_role'] = cfa_var.getRole()
                    var_md['cfa_dimensions'] = " ".join(cfa_var.getDimensions())
                    var_md['cfa_array'] = json.dumps(cfa_var.dump())
                # set the metadata for the variable
                netCDF4.Variable.setncatts(nc_var, var_md)
