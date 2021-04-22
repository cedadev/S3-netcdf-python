from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
from S3netCDF4.CFA._CFAExceptions import CFAError
from netCDF4 import Dataset
import numpy as np


def copy_dims(nc_object, s3_object):
    nc_md_dims = nc_object.dimensions
    for d in nc_md_dims:
        # get the original dimension
        nc_dim = nc_object.dimensions[d]
        # create in the s3Dataset
        if nc_dim.isunlimited():
            size = nc_dim.size
        else:
            size = nc_dim.size
        s3_object.createDimension(d, size)


def copy_vars(nc_object, s3_object, subarray_size, subarray_shape=[]):
    nc_md_vars = nc_object.variables
    for v in nc_md_vars:
        # get the original variable
        nc_var = nc_object.variables[v]
        # create the variable if the sub array shape is given
        nc_var_md_keys = nc_var.ncattrs()
        if "_FillValue" in nc_var_md_keys:
            fill_value = nc_var.getncattr("_FillValue")
        else:
            fill_value = None
        # create the variable - the createVariable method needs to distinguish
        # between whether the shape or size has been passed in
        # Also, if the subarray_shape has been passed in then only attempt to
        # do it for variables with the same number of dimensions as the subarray
        # shape.
        use_shape = False
        if subarray_shape!=[]:
            shape_list = [int(x) for x in subarray_shape.strip("[]").split(",")]
            shape_array = np.array(shape_list)
            # check if shape_array.size is the same as the number of dims in the
            # netCDF variable
            if(shape_array.size == nc_var.ndim):
                use_shape = True

        if use_shape:
            # subarray shape at this moment is a string, [a,b,c,d]
            s3_var = s3_object.createVariable(
                        # can only fill in endian from original dataset as
                        # other initialisation variables are not stored in the
                        # nc_var object
                        nc_var.name,
                        nc_var.dtype,
                        endian=nc_var.endian(),
                        fill_value=fill_value,
                        dimensions=nc_var.dimensions,
                        subarray_shape=shape_array)
        else:
            s3_var = s3_object.createVariable(
                        # can only fill in endian from original dataset as
                        # other initialisation variables are not stored in the
                        # nc_var object
                        nc_var.name,
                        nc_var.dtype,
                        endian=nc_var.endian(),
                        fill_value=fill_value,
                        dimensions=nc_var.dimensions,
                        max_subarray_size=subarray_size)
        # copy the variable's metadata
        nc_var_md_keys = nc_var.ncattrs()
        for k in nc_var_md_keys:
            if k != "_FillValue":
                s3_var.setncattr(k, nc_var.getncattr(k))

        # now copy the data - iterate over every partition
        if (s3_var._cfa_var):
            # it's a CFA variable so we want to copy the data in an intelligent
            # way - by copying it partition by partition.  This will avoid
            # reading the large (potentially huge) dataset into memory all at
            # once
            pm_shape = tuple(s3_var._cfa_var.getPartitionMatrixShape())
            for i in np.ndindex(pm_shape):
                partition = s3_var._cfa_var.getPartition(i)
                location = []
                # this is a bit less obvious as we are using the partition
                # information to get the slices, rather than going from the
                # slices to the partition information, which happens in the
                # _CFAClasses
                for l in partition.location:
                    s = slice(l[0], l[1], 1)
                    location.append(s)
                location = tuple(location)
                nc_data = nc_var[location]
                s3_var[location] = nc_data
        else:
            # not a CFA variable so just copy the data
            s3_var[:] = nc_var[:]


def split_into_CFA(output_path, input_path,
                   subarray_path="",
                   subarray_shape=[], subarray_size=50*1024*1024,
                   cfa_version="0.5", ):
    """Split a netCDF file into a number of subarray files and write the CFA
    master array file."""
    # if the subarray path is empty then get it from the output_path
    if subarray_path == "":
        if ".nca" in output_path:
            subarray_path = output_path[:-4]
        elif ".nc" in output_path:
            subarray_path = output_path[:-3]
        else:
            subarray_path = output_path
            output_path += ".nca"

    # open the input file
    nc_ds = Dataset(input_path, 'r')

    # get the output format for the new Dataset
    # if it's netCDF4 then the output is CFA4
    # if it's netCDF3 then the output is CFA3
    if nc_ds.file_format in ['NETCDF4', 'NETCDF4_CLASSIC']:
        s3_file_format = "CFA4"
    elif nc_ds.file_format == "NETCDF3_CLASSIC":
        s3_file_format = "CFA3"
    else:
        raise CFAError("Cannot split file with format: {}".format(
                        nc_ds.file_format)
                      )

    # open the output file - copy the input from the input file to the output
    # file(s), whilst using the subarray settings to chunk the data
    s3_ds = s3Dataset(output_path, 'w',
                      format=s3_file_format,
                      cfa_version=cfa_version)

    # we now want to copy the information from the original dataset
    # netCDF files have:
    #   global metadata
    #   global dimensions
    #   global variables
    #       Each variable has
    #           metadata
    #           field data
    #
    #   global groups
    #       Each group has
    #           metadata
    #           dimensions
    #           variables
    #               Each variable has
    #                   metadata
    #                   field data

    # global metadata
    nc_md_keys = nc_ds.ncattrs()
    for k in nc_md_keys:
        s3_ds.setncattr(k, nc_ds.getncattr(k))

    # global dimensions
    copy_dims(nc_ds, s3_ds)

    # global variables
    copy_vars(nc_ds, s3_ds, subarray_size, subarray_shape)

    # now do the groups
    for grp in nc_ds.groups:
        nc_grp = nc_ds.groups[grp]
        # create s3 group in the s3 dataset
        s3_grp = s3_ds.createGroup(nc_grp.name)
        # copy group metadata
        nc_md_keys = nc_grp.ncattrs()
        for k in nc_md_keys:
            s3_ds.setncattr(k, nc_grp.getncattr(k))

        # copy group dimensions
        copy_dims(nc_ds, s3_ds)

        # copy group variables
        copy_vars(nc_ds, s3_ds, subarray_size, subarray_shape)

    # close the s3Dataset - super important as everything gets written on close
    s3_ds.close()