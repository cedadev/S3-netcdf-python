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
        self.CFA_conventions = "CFA"

    def is_file(self, nc_dataset):
        """Return whether this input nc_dataset has the requisite metadata to
        mark it as a CFA file."""
        if not "Conventions" in nc_dataset.ncattrs():
            return False
        if not "CFA" in nc_dataset.getncattr("Conventions"):
            return False
        return True

    def get_cfa_version(self, nc_dataset):
        """Parse the Conventions attribute to get the CFA version."""
        if not "Conventions" in nc_dataset.ncattrs():
            raise CFAError("Not a CFA file.")
        else:
            conventions = nc_dataset.getncattr("Conventions").split(" ")
            cfa_version = "0.0"
            for c in conventions:
                if "CFA-" in c:
                    cfa_version = c[4:]
            if cfa_version == "0.0":
                raise CFAError("Not a CFA file.")
        return cfa_version

    def __create_s3vars_and_dims(self, s3_object, nc_object, cfa_object):
        """Consolidate the variables and dimensions in the nc_object (which may
        be a dataset or a group) into the s3_object (which may also be a dataset
        or a group), matching them up with the variables and dimensions in the
        cfa_object (again, which may be a dataset or group)
        """
        from S3netCDF4._s3netCDF4 import s3Dimension, s3Variable
        # loop over the variables
        s3_object._s3_variables = {}     # reset to empty
        for var in nc_object.variables:
            nc_var = nc_object.variables[var]
            if var in cfa_object.getVariables():
                cfa_var = cfa_object.getVariable(var)
                # create the s3Variable with links to the cfa variable and nc_var
                s3_object._s3_variables[var] = s3Variable(
                                                    nc_var=nc_var,
                                                    cfa_var=cfa_var
                                                )
            else:
                s3_object._s3_variables[var] = nc_var

        # loop over the dimensions
        s3_object._s3_dimensions = {}    # reset to empty
        for dim in nc_object.dimensions:
            nc_dim = nc_object.dimensions[dim]
            if dim in cfa_object.getDimensions():
                cfa_dim = cfa_object.getDimension(dim)
                # create the s3Dimension with links to the cfa dimension and nc_dim
                s3_object._s3_dimensions[dim] = s3Dimension(
                                                    nc_dim=nc_dim,
                                                    cfa_dim=cfa_dim
                                                )
            else:
                s3_object._s3_dimensions[dim] = nc_dim

    def __consolidate_from_read(self, s3_dataset):
        """Consolidate a s3_dataset from the file read in.
        s3_dataset contains a netCDF dataset (_nc_dataset).  This contains all
        the definitions of the variables, dimensions and groups as netCDF4
        variables, dimensions and groups.  We want to convert these into their
        s3 equivalents (s3_variable, s3_dimension and s3_group).
        This involves directly manipulating the s3_dataset object.
        """
        from S3netCDF4._s3netCDF4 import s3Group
        nc_dataset = s3_dataset._nc_dataset
        cfa_dataset = s3_dataset._cfa_dataset

        # loop over the variables and dimensions (in the root group)
        if "root" in s3_dataset._cfa_dataset.getGroups():
            cfa_grp = s3_dataset._cfa_dataset.getGroup("root")
        else:
            cfa_grp = s3_dataset._cfa_dataset.createGroup("root")
        self.__create_s3vars_and_dims(s3_dataset, nc_dataset, cfa_grp)

        # loop over the groups
        for grp in nc_dataset.groups:
            nc_grp = nc_dataset.groups[grp]
            if grp in cfa_dataset.getGroups():
                cfa_grp = cfa_dataset.getGroup(grp)
                # create the s3Group with links to the cfa group and nc_grp
                s3_dataset._s3_groups[grp] = s3Group(
                    cfa_grp=cfa_grp,
                    nc_grp=nc_grp
                )
                # create the vars and dims in the group
                self.__create_s3vars_and_dims(
                    s3_dataset._s3_groups[grp],
                    nc_grp,
                    cfa_grp
                )

            else:
                s3_dataset._s3_groups[grp] = nc_grp


    def read(self, s3_dataset):
        """Parse an already open s3_dataset to build the _CFAClasses
        hierarchy.

        Args:
            netcdf_dataset (Dataset): the open dataset from the netcdf4-python
            library.

        Returns:
            CFADataset: The CFADataset object, populated with CFAGroups, which
            are in turn populated with CFADims and CFAVariables.
        """
        # get the netCDF dataset from the s3 dataset
        nc_dataset = s3_dataset._nc_dataset
        # check this is a CFA file
        if not self.is_file(nc_dataset):
            raise CFAError("Not a CFA file.")

        # get the cfa version so we can interpret it as CFA-0.5 (in netCDF4
        # format) or CFA-0.4 (in netCDF3, CLASSIC or netCDF4 format)
        cfa_version = self.get_cfa_version(nc_dataset)
        # check to see if there are any groups and, if there is, create a CFAgroup
        # and add the nc_group to a dictionary of groups.  Start with the root
        # group pointing to the base Dataset
        nc_groups = {"root" : nc_dataset}
        if len(nc_dataset.groups) != 0:
            for grp_name in nc_dataset.groups:
                nc_groups[grp_name] = nc_dataset.groups[grp_name]

        # get the metadata from the dataset in a new dictionary
        nc_dataset_md = {a:nc_dataset.getncattr(a) for a in nc_dataset.ncattrs()}
        # create the CFADataset, with the metadata and format, and empty groups
        cfa_dataset = CFADataset(
                          name="",
                          format=nc_dataset.data_model,
                          metadata=nc_dataset_md,
                          cfa_version=cfa_version
                      )
        # now loop over all the groups, and add a CFAGroup to each dataset, then
        # the CFAVariables and CFADimensions contained in that group
        output_groups = {}
        for group_name in nc_groups:
            nc_group = nc_groups[group_name]
            nc_group_md = {a:nc_group.getncattr(a) for a in nc_group.ncattrs()}
            cfa_group = cfa_dataset.createGroup(group_name, nc_group_md)
            # next parse the dimensions
            for nc_dimname in nc_group.dimensions:
                # get the dimension's associated variable
                nc_dim = nc_group.dimensions[nc_dimname]
                # create the dimension and append to list of cfa_dims
                cfa_dim = cfa_group.createDimension(
                              dim_name=nc_dimname,
                              dim_len=nc_dim.size,
                              metadata={}
                            )

            # loop over the variables in the group / dataset
            for nc_varname in nc_group.variables:
                nc_var = nc_group.variables[nc_varname]
                nc_var_md = {a:nc_var.getncattr(a) for a in nc_var.ncattrs()}
                if "cf_role" in nc_var_md:
                    cfa_var = cfa_group.createVariable(
                                    var_name=nc_varname,
                                    nc_dtype=nc_var.dtype,
                                    metadata=nc_var_md
                                )
                    if cfa_version == "0.4":
                        # this parses from the 0.4 version - i.e all the
                        # metadata is stored in the netCDF attributes
                        cfa_var.parse(nc_var_md)
                    elif cfa_version == "0.5":
                        # this parses from the 0.5 version - i.e. all the
                        # metadata is stored in a variable in a group
                        cfa_var.load(nc_var_md, nc_group)
                    else:
                        raise CFAError(
                            "Unsupported CFA version ({}) in file.".format(
                                cfa_version
                            )
                        )
        # load the cfa_dataset into the s3_dataset that was passed in
        s3_dataset._cfa_dataset = cfa_dataset
        # need to "consolidate" the dataset - create s3 variants of the netCDF
        # groups, variables and dimensions - call .consolidate_from_read
        # on the s3_dataset passed in
        self.__consolidate_from_read(s3_dataset)

    def write(self, cfa_dataset, s3_dataset):
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
        cfa_version = cfa_dataset.getCFAVersion()
        cfa_conventions = self.CFA_conventions + "-{}".format(cfa_version)
        # get the underlying netCDF4 dataset
        nc_dataset = s3_dataset._nc_dataset
        if "Conventions" in dataset_metadata:
            dataset_metadata["Conventions"] += " " + cfa_conventions
        else:
            dataset_metadata["Conventions"] = cfa_conventions

        # set the global metadata
        nc_dataset.setncatts(dataset_metadata)
        # get the groups
        for group in cfa_dataset.getGroups():
            # get the actual group
            cfa_group = cfa_dataset.getGroup(group)
            if (group == "root"):
                nc_group = nc_dataset
                s3_group = s3_dataset
            else:
                s3_group = s3_dataset.groups[group]
                nc_group = s3_group._nc_grp
            # set the metadata for the group
            netCDF4.Group.setncatts(nc_group, cfa_group.getMetadata())

            # set the metadata for the variables
            for var in cfa_group.getVariables():
                # get the actual cfa variable
                cfa_var = cfa_group.getVariable(var)
                # get the variable
                nc_var = s3_group._s3_variables[var]._nc_var
                # get the variable metadata
                var_md = dict(cfa_var.getMetadata())
                # add the cfa metadata - if it is a cfa variable
                if cfa_var.getRole() != "":
                    var_md['cf_role'] = cfa_var.getRole()
                    var_md['cfa_dimensions'] = " ".join(cfa_var.getDimensions())
                    # if the convention version is >= 0.5 then the data has
                    # already been written into the cfa metagroup
                    # for v0.4 we need to dump it into the attribute string
                    if cfa_version == "0.4":
                        # write the partition data
                        var_md['cfa_array'] = json.dumps(cfa_var.dump()['cfa_array'])
                    elif cfa_version == "0.5":
                        # just need to name the cfa_metagroup as an attribute in
                        # the original variable
                        var_md['cfa_group'] = "cfa_" + var
                    else:
                        raise CFAError(
                            "Unsupported CFA version ({}) in file.".format(
                                cfa_version
                            )
                        )
                # set the metadata for the variable
                netCDF4.Variable.setncatts(nc_var, var_md)
            # set the metadata for the dimension variables
            for dim_var in cfa_group.getDimensions():
                # get the actual cfa dimensions
                cfa_dim = cfa_group.getDimension(dim_var)
                # get the netCDF variable for this dimension
                try:
                    nc_dimvar = s3_group.variables[cfa_dim.getName()]._nc_var
                    # copy the dimension metadata into the (dimension) variable
                    # metadata
                    dim_md = dict(cfa_dim.getMetadata())
                    # set the metadata for the variable
                    netCDF4.Variable.setncatts(nc_dimvar, dim_md)
                except KeyError:
                    pass # don't try to write to dimension with no associated
                         # variable
