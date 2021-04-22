#! /usr/bin/env python

__copyright__ = "(C) 2019-2021 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

"""Program to split a netCDF file into a netCDF-CFA master file and a number
of netCDF sub array files.
"""
import argparse

from S3netCDF4.utils.split import split_into_CFA

if __name__ == "__main__":
    # set up and parse the arguments
    parser = argparse.ArgumentParser(
        prog="s3nc_cfa_split",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Split a netCDF file into a netCDF-CFA master file and a number"
            "of netCDF sub array files."
        )
    )

    parser.add_argument(
        "output", action="store", default="", metavar="<output CFA file>",
        help=(
            "Path of the output CFA-netCDF master-array file."
        )
    )

    parser.add_argument(
        "input", action="store", default="", metavar="<input path>",
        help=(
            "Path of the input netCDF file"
        )
    )

    parser.add_argument(
        "--subarray_path", action="store", default="",
        metavar="<subarray path>",
        help=(
            "Common path of the output sub array files (optional).  Without "
            "this argument, the output will be in a directory below the path of"
            " the output netCDF-CFA master array file."
        )
    )

    parser.add_argument(
        "--subarray_shape", action="store", default=[],
        metavar="<subarray_shape>",
        help=(
            "Shape for the subarray files (optional).  Without this argument, "
            "the shape will be automatically determined."
        )
    )

    parser.add_argument(
        "--subarray_size", action="store", default=50*1024*1024,
        metavar="<subarray_size>",
        help=(
            "Size for the subarray files (optional).  With this argument, the "
            "shape will be automatically determined, with this target size. "
            "The units for the size is <number of elements in the array>, not "
            "any magnitude of bytes."
        )
    )

    parser.add_argument(
        "--cfa_version", action="store", default="0.5",
        help=("Version of CFA conventions to use, 0.4|0.5")
    )

    args = parser.parse_args()

    if args.output and args.input:
        split_into_CFA(args.output, args.input,
                       args.subarray_path,
                       args.subarray_shape,
                       int(args.subarray_size),
                       args.cfa_version)
