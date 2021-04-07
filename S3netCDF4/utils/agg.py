from urllib.parse import urlparse
import os
from glob import glob
import numpy as np

from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
from S3netCDF4.CFA._CFAClasses import CFAPartition
from S3netCDF4.Managers._FileManager import FileManager

from netCDF4 import num2date, date2num

def get_universal_times(nc_var, common_date):
    # get the start date and calendar
    if ("units" in nc_var.ncattrs() and
        "calendar" in nc_var.ncattrs() and
        common_date is not None):
        date_values = num2date(nc_var[:],
                        nc_var.units,
                        nc_var.calendar)
        axis_dim_values = date2num(date_values,
                                   common_date,
                                   nc_var.calendar)
    else:
        axis_dim_values = nc_var[:]
    return axis_dim_values

def add_var_dims(in_object, out_object, axis, fname, common_date):
    """Add the variables and dimensions to the s3Dataset or s3Group"""
    # create dimension, get the axis dimension location
    axis_dim_n = -1
    for d, dim in enumerate(in_object.dimensions):
        in_dim = in_object.dimensions[dim]
        if dim not in out_object.dimensions:
            # get the dim size, 0 is UNLIMITED if dim == axis
            if axis == dim:
                dim_size = 0
            else:
                dim_size = in_dim.size

            out_dim = out_object.createDimension(
                dim, dim_size
            )
        else:
            out_dim = out_object.dimensions[dim]
        # get the axis dimension
        if axis == dim:
            axis_dim_n = d

    # create variable
    for var in in_object.variables:
        in_var = in_object.variables[var]
        # get the variable metadata
        in_var_attrs = {
            x: in_var.getncattr(x) for x in in_var.ncattrs()
        }
        # if the variable does not already exist then create it
        if var not in out_object.variables:
            # get the subarray shape
            shp = in_var.shape
            subarray_shape = np.array(shp, 'i')
            if len(in_var.dimensions) > 0:
                # rejig axis to be unlimited
                if len(subarray_shape) > axis_dim_n:
                    subarray_shape[axis_dim_n] = 0
                # create the variable with subarray
                out_var = out_object.createVariable(
                    var, in_var.dtype, in_var.dimensions,
                    subarray_shape=subarray_shape
                )
            else: # no dimensions, just a scalar variable
                out_var = out_object.createVariable(
                    var, in_var.dtype
                )
        else:
            # variable has already been created so get it
            out_var = out_object.variables[var]

        # only write partitions for field variables - those with _cfa_var != None
        if out_var._cfa_var:
            # get the current partition matrix shape
            c_shape = out_var._cfa_var.getPartitionMatrixShape()
            # create the index to append at the end of the currently used
            # indices
            n_dims = len(out_var.dimensions)
            if n_dims > 0:
                index = np.zeros(n_dims, 'i')
                index[axis_dim_n] = c_shape[0]
                # get the location along the aggregation axis in the Master Array,
                # from the axis dimension variable
                location = np.zeros([n_dims, 2],'i')

            # check whether the axis is in the dimensions of the input_variable
            # and calculate the location from it if it is
            if axis in in_var.dimensions:
                # get the values of the axis variable
                axis_dim_var = in_object.variables[axis]
                # if this is a time variable then covert the values to a common
                # calendar
                if axis_dim_var.name == "time" or axis_dim_var.name[0] == "t":
                    # get the start date and calendar
                    axis_dim_values = get_universal_times(
                        axis_dim_var, common_date
                    )

                # get the axis resolution - i.e. the difference for each step
                # along the axis
                try:
                    axis_res = (axis_dim_values[-1] - axis_dim_values[0]) / len(axis_dim_values)
                except IndexError:
                    axis_res = 1
                # set the location for the aggregating axis dimension
                location[axis_dim_n, 0] = int(axis_dim_values[0] / axis_res)
                location[axis_dim_n, 1] = location[axis_dim_n, 0] + len(axis_dim_var)
                # set the locations for the other dimensions - equal to 0 to the
                # shape of the array
                for d, dim in enumerate(out_var.dimensions):
                    # don't redo the above for axis_dim_n
                    if d != axis_dim_n:
                        location[d, 0] = 0
                        location[d, 1] = in_var.shape[d]
            else:
                for d in range(0, len(in_var.shape)):
                    location[d, 0] = 0
                    location[d, 1] = in_var.shape[d]

            # get the datamodel from the parent object
            try:
                datamodel = out_object._nc_grp.data_model
            except (KeyError, AttributeError):
                datamodel = out_object._nc_dataset.data_model

            # create the partition for none scalar variables
            if len(out_var._cfa_var.getPartitionMatrixShape() != 0):
                partition = CFAPartition(
                    index=tuple(index),
                    location=location,
                    ncvar=var,
                    file=fname,
                    format=datamodel,
                    shape=in_var.shape
                )
                # write the partition
                out_var._cfa_var.writePartition(partition)
                # add the attributes to the s3Dataset by updating the dictionary
                out_var._cfa_var.metadata.update(in_var_attrs)
        else:
            # assign the values from the input variable to the output variable
            # if it is the axis variable then append / concatenate
            if var == axis:
                var_vals = in_object.variables[var]
                axl = out_var._nc_var.shape[axis_dim_n]
                # convert times here as well
                out_var[axl:] = get_universal_times(var_vals, common_date)
            else:
                out_var[:] = in_object.variables[var][:]
            # update the in_var_attrs to the new common_date if applicable
            if (common_date is not None and
                "units" in in_var_attrs and
                in_var.name == axis):
                in_var_attrs["units"] = common_date
            out_var.setncatts(in_var_attrs)

def create_partitions_from_files(out_dataset, files, axis,
                                 cfa_version, common_date):
    """Create the CFA partitions from a list of files."""
    # loop over the files and open as a regular netCDF4 Dataset
    for fname in files:
        in_dataset = s3Dataset(fname, "r")
        # get the global metadata
        in_dataset_attrs = {
            x: in_dataset.getncattr(x) for x in in_dataset.ncattrs()
        }
        # add the attributes to the s3Dataset by updating the dictionary
        out_dataset._cfa_dataset.metadata.update(in_dataset_attrs)
        # loop over the groups
        for grp in in_dataset.groups:
            in_group = in_dataset[grp]
            # create a group if one with this name does not exist
            if grp not in out_dataset.groups:
                out_group = out_dataset.createGroup(grp)
            else:
                out_group = out_dataset.groups[grp]
            # update the metadata
            in_group_attrs = {
                x: in_group.getncattr(x) for x in in_group.ncattrs()
            }
            out_group._cfa_grp.metadata.update(in_group_attrs)
            add_var_dims(in_group, out_group, axis, fname, common_date)

        # add the variables in the root group
        add_var_dims(in_dataset, out_dataset, axis, fname, common_date)
        in_dataset.close()

def sort_partition_matrix(out_var, axis):
    """Sort the partition matrix for a single variable."""
    # get the index of the axis that we are aggregating over
    try:
        axis_dim_n = out_var._cfa_var.getPartitionMatrixDimensions().index(axis)
        # create the index
        n_dims = len(out_var._cfa_var.getDimensions())
        # get the location values from the values
        locs = out_var._cfa_var.getPartitionValues(key="location").squeeze()
        # get the first (start) location values and get the order to sort them
        # in
        sort_order = np.argsort(locs[:,axis_dim_n,0])
        # loop over the sort order and write the partition information into
        # the new location
        # keep a list of partitions
        new_parts = []
        for i, s in enumerate(sort_order):
            # build the index to get the partition, in the sort order
            index = np.zeros(n_dims,'i')
            index[axis_dim_n] = s
            # get the partition
            source_part = out_var._cfa_var.getPartition(index)
            # reassign the index
            source_part.index[axis_dim_n] = i
            # add to the list
            new_parts.append(source_part)
        # now rewrite the partitions, and ensure their integrity - i.e. make
        # sure that the axis partitions are the right length
        for p in range(len(new_parts)):
            part = new_parts[p]
            if p > 0:
                # align with previous partition
                prev_part = new_parts[p-1]
                part.location[axis_dim_n,0] = prev_part.location[axis_dim_n,1]
            # make sure end of partition aligns with shape of array
            part.location[axis_dim_n,1] = (part.location[axis_dim_n,0] +
                part.shape[axis_dim_n])
            out_var._cfa_var.writePartition(part)

    except ValueError:
        axis_dim_n = 0

def sort_axis_variable(out_object, axis):
    # sort the axis variable and write back out to the netCDF object
    try:
        axis_dim_var = out_object.variables[axis]
        axis_dim_var[:] = np.sort(axis_dim_var[:])
    except KeyError:
        pass

def sort_partition_matrices(out_dataset, axis):
    """Sort the partition matrices for all the variables.  Sort is based on the
    first element of the location."""
    # need to sort all groups
    for grp in out_dataset.groups:
        out_group = out_dataset.groups[grp]
        # need to sort all variables in the group
        for var in out_group.variables:
            out_var = out_group.variables[var]
            if out_var._cfa_var:
                sort_partition_matrix(out_var, axis)

        # sort the axis variable in the group
        sort_axis_variable(out_group, axis)

    # need to sort all the variables just in the database
    for var in out_dataset.variables:
        out_var = out_dataset.variables[var]
        if out_var._cfa_var:
            sort_partition_matrix(out_var, axis)

    # sort the axis variable in the dataset
    sort_axis_variable(out_dataset, axis)

def get_file_list(path):
    """Get a list of files given the path.
       The path can be:
            a directory
            a glob with multiple wildcards
            a 'path' on a S3 storage device
    """
    # open the directory as a FileManager object
    fm = FileManager()
    path = os.path.expanduser(path)
    request_object = fm.request_file(path)
    file_object = request_object.file_object

    # get a list of files using the file object if it is a remote system
    if (file_object.remote_system):
        # split the url into the scheme, netloc, etc.
        url_o = urlparse(path)
        # the alias is the scheme + "://" + netloc
        alias = url_o.scheme + "://" + url_o.netloc
        # use a paginator to get multiple pages of the objects in the bucket
        files = file_object.glob()
        # add the alias and bucket to each of the files
        bucket = file_object.file_handle._bucket
        for i, f in enumerate(files):
            files[i] = alias + "/" + bucket + "/" + f
    else:
        if os.path.isdir(path):
            rawfiles = os.listdir(path)
            files = [os.path.join(path, f) for f in rawfiles]
            # or get a list of files using glob
        else:
            files = glob(path)
    return files


def aggregate_into_CFA(output_master_array, path, axis,
                       cfa_version, common_date=None):
    """Aggregate the netCDF files in directory into a CFA master-array file"""
    # get the list of files first of all
    files = get_file_list(path)
    # create the s3Dataset
    # create the output master array file
    out_dataset = s3Dataset(
        output_master_array,
        mode='w',
        clobber=True,
        diskless=False,
        cfa_version=cfa_version
    )
    # create the partitions from the list - these will be created in the order
    # that the files are read in
    create_partitions_from_files(out_dataset, files, axis,
                                 cfa_version, common_date)
    # we need to sort the partition matrices for each variable - i.e. there is
    # one matrix per variable
    sort_partition_matrices(out_dataset, axis)
    # close the dataset to write / upload it
    out_dataset.close()