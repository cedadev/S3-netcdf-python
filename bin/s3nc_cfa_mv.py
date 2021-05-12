#! /usr/bin/env python

__copyright__ = "(C) 2019-2021 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

"""Program to rewrite partition infomation in a CFA-netCDF master-array file to reflect that a sub-array file has moved.
"""

import argparse
from urllib.parse import urlparse
import os
import numpy as np
import sys

from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset
from S3netCDF4.CFA._CFAClasses import CFAPartition

def split_file_name(input_name):
    # split into prefix and filename
    # this should work on urls and file paths
    file_split = input_name.split("/")
    file_path = "/".join(file_split[:-1])
    file_name = file_split[-1]
    return file_path, file_name

def update_file_in_partition(prefix, cfa_var, partition_index):
    """Update the file_information in a variable for a given partition.
    Args:
        prefix (string): new prefix for files
        cfa_var (CFAVariable): variable to alter the partition for
        partition_index (np.ndarray): index of the partition to alter
    Returns:
        None
    """
    # get the partition from the index
    partition = cfa_var.getPartition(partition_index)
    # get the file name and file path:
    file_path, file_name = split_file_name(partition.file)
    # new file path:
    new_file_path = prefix + "/" + file_name
    # construct a new partition
    new_part = CFAPartition(
        index = partition.index,
        location = partition.location,
        ncvar = partition.ncvar,
        file = new_file_path,
        format = partition.format,
        shape = partition.shape
    )
    # write (and replace) the old partition
    cfa_var.writePartition(new_part)

def update_file_in_variable(cfa_var, prefix, partition="all"):
    """Update the file_information in a variable for a given partition.
    Args:
        cfa_var (CFAVariable): CFA variable to alter, containing the partitions
        prefix (string): new prefix for files
        partition (string): index of the partition to alter, or 'all'
    Returns:
        None
    """
    if partition == "all":
        pmshape = cfa_var.getPartitionMatrixShape()
        for partition_index in np.ndindex(*pmshape):
            update_file_in_partition(prefix, cfa_var, partition_index)
    else:
        # convert from partition string
        partition_index = np.fromstring(args.partition, dtype='i', sep=', ')
        update_file_in_partition(prefix, cfa_var, partition_index)

def update_file_in_group(cfa_group, prefix, variable="all", partition="all"):
    """Update the file_information in a group for a given partition.
    Args:
        cfa_group (CFAGroup): CFA group to alter, containing the cfa_variables
        prefix (string): new prefix for files
        variable (string): name of the variable to alter, or 'all'
        partition (string): index of the partition to alter, or 'all'
    Returns:
        None
    """
    if variable == "all":
        for var in cfa_group.getVariables():
            cfa_var = cfa_group.getVariable(var)
            update_file_in_variable(cfa_var, prefix, partition)
    else:
        if variable in cfa_group.getVariables():
            cfa_var = cfa_group.getVariable(variable)
            update_file_in_variable(cfa_var, prefix, partition)


def update_file_in_partitions(input_dataset,
                              prefix,
                              group="all",
                              variable="all",
                              partition="all"):
    """Update the file information in the given partition.
    This partition could be all or a single partition specified by [t,z,x,y]
    for example.

    Args:
        input_dataset (s3Dataset): dataset to alter
        prefix (string): new prefix for files
        group (string): name of group to alter, or 'all', or 'none'
        variable (string): name of variable to alter, or 'all'
        partition (string): name of partition to alter, or 'all'

    Returns:
        None
    """
    # get the cfa structure from the dataset
    cfa_dataset = input_dataset._cfa_dataset
    if group == "all":
        for grp in cfa_dataset.getGroups():
            cfa_group = cfa_dataset.getGroup(grp)
            update_file_in_group(cfa_group, prefix, variable, partition)
    else:
        # named group
        cfa_group = input_dataset.getGroup(group)
        update_file_in_group(cfa_group, prefix, variable, partition)


if __name__ == "__main__":
    """Utility program to alter the structure of a CFA-netCDF master array
       file, either on the disk or remotely on S3 storage, to change the
       location of the sub-array file.  Note that it doesn't actually move any
       files, it just updates the record in the partition matrix.
       It will only update the prefix of the file location, not the actual
       filename.  i.e. it replaces os.path.dirname
       Options are:
        1. The input master-array file, write back to the same file
        2. The partition to change
            --partition=all|none|<partition index>  default: --partition=all
        3. The prefix of the new address for the file location
            --prefix=
    """
    # set up and parse the arguments
    parser = argparse.ArgumentParser(
        prog="s3nc_cfa_mv",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Alter the paths of the sub-array files in the master-array file to"
            " reflect that those sub-array files have been moved to a new "
            " location. It will only update the prefix of the file location, " " not the actual filename."
        )
    )

    parser.add_argument(
        "input", action="store", default="", metavar="<CFA file>",
        help=(
            "Path of the CFA-netCDF master-array file to alter."
        )
    )

    parser.add_argument(
        "--group", action="store", default="all",
        metavar="<group>",
        help=(
            "Name of a group to change file prefix for, or change all groups. "
            "--group=all|<group_name>"
        )
    )

    parser.add_argument(
        "--variable", action="store", default="all",
        metavar="<variable>",
        help=(
            "Name of a variable to change file prefix for, or change all " "variables."
            "--variable=all|<variable_name>"
        )
    )

    parser.add_argument(
        "--partition", action = "store", default="all",
        metavar="<partition>",
        help=(
            "Choose the partition to change the file location prefix for."
            "--partition=all<partition_index>"
        )
    )

    parser.add_argument(
        "--prefix", action = "store", default="none", required=True,
        metavar="<prefix>",
        help=(
            "New file location prefix"
        )
    )
    args = parser.parse_args()

    # get the input file
    input_path = os.path.expanduser(args.input)
    # open the input dataset in append mode
    input_dataset = s3Dataset(input_path, mode='a')
    # Update the prefix in the partitions
    update_file_in_partitions(input_dataset, args.prefix, args.group,
                              args.variable, args.partition)
    # close the file to save the changes
    input_dataset.close()
