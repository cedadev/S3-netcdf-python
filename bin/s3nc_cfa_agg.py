#! /usr/bin/env python

"""Program to aggregate netCDF-CFA files from disk or s3.
This program will produce a master array file, containing references to the
files that have been aggregated.
"""

import argparse
import os
import glob
from collections import OrderedDict
import numpy as np

from S3netCDF4._Exceptions import *
from S3netCDF4._S3netCDF4 import s3Dataset
from S3netCDF4.CFA._CFAClasses import CFAPartition

#import warnings
#warnings.filterwarnings("error")

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
        # if the variable does not already exist then create it
        if var not in in_object.variables:
            # get the subarray shape
            subarray_shape = np.array(in_var.shape, 'i')
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
            # create the index
            n_dims = len(in_object.dimensions)
            index = np.zeros(n_dims, 'i')
            index[axis_dim_n] = c_shape[0]
            # get the location along the aggregation axis in the Master Array,
            # from the axis dimension variable
            location = np.zeros([n_dims,2],'i')
            if axis in in_object.variables:
                axis_dim_var = in_object.variables[axis]
                location[axis_dim_n, 0] = axis_dim_var[0]
                location[axis_dim_n, 1] = axis_dim_var[-1]
            # get the datamodel from the parent object
            try:
                datamodel = in_object._nc_grp.data_model
            except:
                datamodel = in_object._nc_dataset.data_model

            # create the partition
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
            print(in_group)
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
            add_var_dims(out_group, in_group, axis, fname)

        # add the variables in the root group
        add_var_dims(out_dataset, in_dataset, axis, fname)

def aggregate_into_CFA(output_master_array, directory, axis, cfa_version):
    """Aggregate the netCDF files in directory into a CFA master-array file"""
    # get the list of files first of all
    files = glob.glob(os.path.expanduser(directory))
    # create the s3Dataset
    # create the output master array file
    out_dataset = s3Dataset(
        output_master_array,
        mode='w',
        clobber=True,
        diskless=False,
        cfa_version=cfa_version
    )
    # create the partitions from the list
    partitions = create_partitions_from_files(
         out_dataset, files, axis, cfa_version
    )
    out_dataset.close()
    # print(partitions)
    #print(cfa_dataset)
    #print(cfa_dataset.getGroup(cfa_dataset.getGroups()[0]))
    #print(cfa_dataset.getGroup(cfa_dataset.getGroups()[1]))

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
