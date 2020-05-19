#! /usr/bin/env python

"""Program to return information about a netCDF-CFA file from disk or S3.
Modelled after ncdump and cdo info.
"""

import argparse
from urllib.parse import urlparse
import os
import numpy as np

from S3netCDF4._s3netCDF4 import s3Dataset as s3Dataset

def print_dimension_info(input_dim, metadata):
    """Print the information for the dimension."""
    dim_size = input_dim.getLen()
    print("        {} = {}".format(input_dim.getName(), dim_size))
    # print the metadata
    if metadata:
        md = input_dim.getMetadata()
        for key in md:
            if key[0:4] != "cfa_":
                print ("            {}:{} = {}".format(
                    input_dim.getName(), key, md[key])
                )

def print_dimensions(group, metadata):
    """Print all the dimensions in a group."""
    for d in group.getDimensions():
        input_dimension = group.getDimension(d)
        print_dimension_info(input_dimension, metadata)

def print_partition_info(input_var, partition_index):
    """Print the partition information for a single partition.
    By this point partition should be a numpy array of the number of
    dimensions of the partition."""
    partition = input_var.getPartition(partition_index)
    var_name_len = len(input_var.getName()) + 16
    just_str = ""
    for x in range(0, var_name_len):
        just_str += " "
    print("            {}:{} {} =".format(
            input_var.getName(), "partition", partition_index
        )
    )
    # location
    location_string = ":location = {}".format(partition.location.tolist())
    print(just_str + location_string)
    # shape
    shape_string = ":shape    = {}".format(partition.shape.tolist())
    print(just_str + shape_string)
    # filename
    filename_string = ":filename = {}".format(partition.file)
    print(just_str + filename_string)
    # varname
    varname_string = ":variable = {}".format(partition.ncvar)
    print(just_str + varname_string)
    # format
    format_string = ":format   = {}".format(partition.format)
    print(just_str + format_string)

def print_variable_info(input_var, partition, metadata):
    """Print the information for the variable."""
    print("        {} {}({})".format(
        input_var.getType(),
        input_var.getName(),
        ",".join(input_var.getDimensions())
        )
    )
    # print the metadata
    if metadata:
        md = input_var.getMetadata()
        for key in md:
            if key[0:4] != "cfa_":
                print ("            {}:{} = {}".format(
                    input_var.getName(), key, md[key])
                )
        # print the minimum partition information
        # print the partition matrix shape
        pmshape_str = "("
        for x in input_var.getPartitionMatrixShape():
            pmshape_str += str(x) + ", "
        pmshape_str = pmshape_str[:-2] + ")"
        print ("            {}:{} = {}".format(
            input_var.getName(), "pmshape", pmshape_str)
        )
        # print the partition matrix dimensions
        pmdims = "(" + ", ".join(input_var.getPartitionMatrixDimensions()) + ")"
        print ("            {}:{} = {}".format(
            input_var.getName(), "pmdimensions", pmdims)
        )
    # print the partition
    if partition == "all":
        pmshape = input_var.getPartitionMatrixShape()
        for index in np.ndindex(*pmshape):
            print_partition_info(input_var, index)
    elif partition == "none":
        pass # do not print anything for partition==none
    else:
        partition_index = np.fromstring(args.partition, dtype='i', sep=', ')
        print_partition_info(input_var, np.array(partition_index))

def print_variables(group, partition, metadata):
    for v in group.getVariables():
        input_var = group.getVariable(v)
        print_variable_info(input_var, partition, metadata)

def print_group_info(input_grp, variable, partition, metadata):
    """Print the information for the group, and all the dimensions and
    variables in the group."""
    if variable == "none":
        print("    {}".format(input_grp.getName()))
    else:
        print("group: {} ".format(input_grp.getName())+"{")
        # print the dimensions
        print("    dimensions:")
        print_dimensions(input_grp, metadata)

        # print the variables in the group
        print("    variables:")
        if variable == "all":
            print_variables(input_grp, partition, metadata)
        else:
            input_var = input_grp.getVariable(variable)
            print_variable_info(input_var, partition, metadata)
        print("    }")
    if metadata:
        print("    // group attributes")
        md = input_grp.getMetadata()
        for key in md:
            if key[0:4] != "cfa_":
                print ("        :{} = {}".format(
                    key, md[key])
                )

def print_dataset_info(input_dataset, group, variable, partition, metadata):
    """Print the information for the dataset.  Use the CFA class.
    Print the name, metadata and groups.  Recurse into the group to print the
    variables if variable==all or variable==<name of variable>."""
    cfa_d = input_dataset._cfa_dataset
    print(cfa_d.getName() + " {")
    # print the root group if group == "all" or group == "root"
    if (group in ["all", "root"]):
        root_grp = cfa_d["root"]
        print("dimensions:")
        print_dimensions(root_grp, metadata)
        print("variables:")
        if variable == "all":
            print_variables(root_grp, partition, metadata)
        else:
            input_var = root_grp.getVariable(variable)
            print_variable_info(input_var, partition, metadata)
        # print the group names, unless just the root group is requested
        if (group != "root"):
            if (variable == "none"):
                print("groups:")
            for g in cfa_d.getGroups():
                input_grp = cfa_d[g]
                if (g != "root" and g[0:4] != "cfa_"):
                    print_group_info(input_grp, variable, partition, metadata)
    else:
        if (variable == "none"):
            print("groups:")
        input_grp = cfa_d[group]
        print_group_info(input_grp, variable, partition, metadata)

    # print the global attributes
    if metadata:
        print("// global attributes")
        md = cfa_d.getMetadata()
        for key in md:
            print ("    :{} = {}".format(key, md[key]))
    print("}")

if __name__ == "__main__":
    """Utility program to display the structure of a CFA-netCDF master array
       file, either on the disk or remotely on S3 storage.
       This program is inspired by ncdump and cdo info / sinfo.
       We need options to control three things:
        1.  Whether to output all the groups, or a particular group and whether
            to output the variables in the group(s)
            --group=all|<group_name>                default: --group=all
            --variable=all|<variable_name>          default: --variable=all
        2.  Whether to output the metadata or not
            --metadata                              default: --metadata(on)
        3.  Whether to output partition information for the variables, either
            all the partition information or for a particular partition.
            --partition=all|none|<partition index>  default: --partion=none (off)
    """
    # set up and parse the arguments
    parser = argparse.ArgumentParser(
        prog="s3nc_cfa_info",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Output information about a CFA-netCDF file, or netCDF file either "
            "on disk or on S3"
        )
    )

    parser.add_argument(
        "input", action="store", default="",
        metavar="<input>",
        help=(
            "Path of the  CFA-netCDF or netCDF file input file, either on disk"
            " or S3."
        )
    )

    parser.add_argument(
        "-group", action="store", default="all",
        metavar="<group>",
        help=(
            "Name of a group to print information about, or print all groups. "
            "-group=all|<group_name>"
        )
    )

    parser.add_argument(
        "-variable", action="store", default="all",
        metavar="<variable>",
        help=(
            "Name of a variable to print information about, print all or no" "variables. "
            "-variable=all|none|<variable_name>"
        )
    )

    parser.add_argument(
        "-partition", action = "store", default="none",
        metavar="<partition>",
        help=(
            "Print the information about a partition. "
            "-partition=all|none|<partition_index>"
        )
    )

    parser.add_argument(
        "-metadata", action = "store_true", default=False,
        help=(
            "Print the metadata for groups, dimensions and variables"
            "-metadata"
        )
    )

    args = parser.parse_args()

    if args.input:
        input_file = args.input
    else:
        input_file = None

    if args.group:
        group = args.group
    else:
        group = "all"

    if args.variable:
        variable = args.variable
    else:
        variable = "all"

    if args.partition:
        # convert the partition string to a numpy array
        partition = args.partition
    else:
        partition = "none"

    if args.metadata:
        metadata = True
    else:
        metadata = False

    if input_file:
        # Get the input file.
        path = os.path.expanduser(input_file)
        input_dataset = s3Dataset(path, mode='r')
        # Print the global dataset information
        print_dataset_info(
            input_dataset,
            group,
            variable,
            partition,
            metadata
        )
    #else:
