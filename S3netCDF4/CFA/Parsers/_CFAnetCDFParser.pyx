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
        """Do nothing but set the CFA version used, but don't call the base
        class as that will raise NotImplementedError"""
        self.CFA_conventions = "CFA-0.4"

    def is_file(self, nc_dataset):
        """Return whether this input nc_dataset has the requisite metadata to
        mark it as a CFA file."""
        if not "Conventions" in nc_dataset.ncattrs():
            return False
        if not "CFA" in nc_dataset.getncattr("Conventions"):
            return False
        return True

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
        if not self.is_file(nc_dataset):
            raise CFAError("Not a CFA file.")

        # check to see if there are any groups and, if there is, create a CFAgroup
        # and add the nc_group to a dictionary of groups
        nc_groups = {}
        if len(nc_dataset.groups) != 0:
            for grp_name in nc_dataset.groups:
                nc_groups[grp_name] = nc_dataset.groups[grp_name]
        # if there isn't then create a root group that is the Dataset
        else:
            nc_groups["root"] = nc_dataset

        # get the metadata from the dataset in a new dictionary
        nc_dataset_md = {a:nc_dataset.getncattr(a) for a in nc_dataset.ncattrs()}
        # create the CFADataset, with the metadata and format, and empty groups
        cfa_dataset = CFADataset(
                          name="",
                          format=nc_dataset.data_model,
                          metadata=nc_dataset_md
                      )
        # now loop over all the groups, and add a CFAGroup to each dataset, then
        # the variables and dimensions contained in that group
        for group_name in nc_groups:
            nc_group = nc_groups[group_name]
            nc_group_md = {a:nc_group.getncattr(a) for a in nc_group.ncattrs()}
            cfa_group = cfa_dataset.createGroup(group_name, nc_group_md)
            # next parse the dimensions
            for nc_dimname in nc_dataset.dimensions:
                # get the dimension's associated variable
                nc_dim = nc_dataset.dimensions[nc_dimname]
                # create the dimension and append to list of cfa_dims
                cfa_dim = cfa_group.createDimension(
                              dim_name=nc_dimname,
                              dim_len=nc_dim.size,
                              metadata={}
                            )

            # loop over the variables in the group / dataset
            for nc_varname in nc_groups[group_name].variables:
                nc_var = nc_groups[group_name].variables[nc_varname]
                nc_var_md = {a:nc_var.getncattr(a) for a in nc_var.ncattrs()}
                cfa_var = cfa_group.createVariable(
                              var_name=nc_varname,
                              nc_dtype=nc_var.dtype,
                              metadata=nc_var_md
                            )
                cfa_var.parse(nc_var_md)

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
        # add the CFA conventions into the metadata
        dataset_metadata = cfa_dataset.getMetadata()
        if "Conventions" in dataset_metadata:
            dataset_metadata["Conventions"] += " " + self.CFA_conventions
        else:
            dataset_metadata["Conventions"] = self.CFA_conventions

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
                    var_md['cfa_array'] = json.dumps(cfa_var.dump()['cfa_array'])
                # set the metadata for the variable
                netCDF4.Variable.setncatts(nc_var, var_md)
