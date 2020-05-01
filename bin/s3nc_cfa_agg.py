#! /usr/bin/env python

"""Program to aggregate netCDF-CFA files from disk or s3.
This program will produce a master array file, containing references to the
files that have been aggregated.
"""

import argparse
from urllib.parse import urlparse
import os
from glob import glob
import numpy as np

from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
from S3netCDF4.CFA._CFAClasses import CFAPartition
from S3netCDF4.Managers._FileManager import FileManager

def add_var_dims(in_object, out_object, axis, fname):
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
            # rejig axis to be unlimited
            subarray_shape[axis_dim_n] = 0
            out_var = out_object.createVariable(
                var, in_var.dtype, in_var.dimensions,
                subarray_shape=subarray_shape
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
            n_dims = len(in_object.dimensions)
            index = np.zeros(n_dims, 'i')
            index[axis_dim_n] = c_shape[0]
            # get the location along the aggregation axis in the Master Array,
            # from the axis dimension variable
            location = np.zeros([n_dims,2],'i')
            if axis in in_object.variables:
                axis_dim_var = in_object.variables[axis]
                # get the axis resolution - i.e. the difference for each step
                # along the axis
                try:
                    axis_res = axis_dim_var[1] - axis_dim_var[0]
                except IndexError:
                    axis_res = 1
                # set the location for the aggregating axis dimension
                location[axis_dim_n, 0] = int(axis_dim_var[0] / axis_res)
                location[axis_dim_n, 1] = int(axis_dim_var[-1] / axis_res)
                # set the locations for the other dimensions - equal to 0 to the
                # shape of the array
                for d, dim in enumerate(in_object.dimensions):
                    # don't redo the above for axis_dim_n
                    if d != axis_dim_n:
                        location[d, 0] = 0
                        location[d, 1] = in_object.dimensions[dim].size

            # get the datamodel from the parent object
            try:
                datamodel = out_object._nc_grp.data_model
            except (KeyError, AttributeError):
                datamodel = out_object._nc_dataset.data_model

            # create the partition
            partition = CFAPartition(
                index=tuple(index),
                location=location,
                ncvar=var,
                file=fname,
                format=datamodel,
                shape=in_var.shape
            )
            # add the attributes to the s3Dataset by updating the dictionary
            out_var._cfa_var.metadata.update(in_var_attrs)
            # write the partition
            out_var._cfa_var.writePartition(partition)
        else:
            # assign the values from the input variable to the output variable
            # if it is the axis variable then append / concatenate
            if var == axis:
                var_vals = in_object.variables[var][:]
                axl = out_var._nc_var.shape[axis_dim_n]
                out_var[axl:] = var_vals[:]
            else:
                out_var[:] = in_object.variables[var][:]
            # set the attributes
            out_var.setncatts(in_var_attrs)

def create_partitions_from_files(out_dataset, files, axis, cfa_version):
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
            add_var_dims(in_group, out_group, axis, fname)

        # add the variables in the root group
        add_var_dims(in_dataset, out_dataset, axis, fname)

def sort_partition_matrix(out_var, axis):
    """Sort the partition matrix for a single variable."""
    # get the index of the axis that we are aggregating over
    try:
        axis_dim_n = out_var._cfa_var.getPartitionMatrixDimensions().index(axis)
        # get the partition shape
        part_shape = out_var._cfa_var.getPartitionMatrixShape()
        # create the index
        n_dims = len(out_var._cfa_var.getDimensions())
        # get the location values from the partition
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
        # now rewrite the partitions
        for part in new_parts:
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
    file_object = fm._open(path)

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
        # or get a list of files using glob
        files = glob(path)
    return files

def aggregate_into_CFA(output_master_array, path, axis, cfa_version):
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
    create_partitions_from_files(out_dataset, files, axis, cfa_version)
    # we need to sort the partition matrices for each variable - i.e. there is
    # one matrix per variable
    sort_partition_matrices(out_dataset, axis)
    # close the dataset to write / upload it
    out_dataset.close()

if __name__ == "__main__":
    # set up and parse the arguments
    parser = argparse.ArgumentParser(
        prog="s3nc_cfa_agg",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Aggregate a number of netCDF files into a CFA-netCDF "
            "master-array file."
        )
    )

    parser.add_argument(
        "output", action="store", default="", metavar="<output CFA file>",
        help=(
            "Path of the output master-array file."
        )
    )

    parser.add_argument(
        "dir", action="store", default="", metavar="<input path>",
        help=(
            "Path of a directory containing netCDF files to aggregate into a "
            "CFA-netCDF master-array file."
        )
    )

    parser.add_argument(
        "--cfa_version", action="store", default="0.5",
        help=("Version of CFA conventions to use, 0.4|0.5")
    )

    parser.add_argument(
        "--axis", action="store", default="time",
        help=("Axis to aggregate along, default=time")
    )

    args = parser.parse_args()

    if args.output and args.dir:
        aggregate_into_CFA(args.output, args.dir, args.axis, args.cfa_version)