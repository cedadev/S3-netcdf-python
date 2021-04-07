#! /usr/bin/env python

__copyright__ = "(C) 2019-2021 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey"

"""Program to aggregate netCDF-CFA files from disk or s3.
This program will produce a master array file, containing references to the
files that have been aggregated.
"""

import argparse
from S3netCDF4.utils.agg import aggregate_into_CFA

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

    parser.add_argument(
        "--common_date", action="store", default=None,
        help=("Common start time across all files")
    )

    args = parser.parse_args()

    if args.output and args.dir:
        aggregate_into_CFA(args.output,
                           args.dir,
                           args.axis,
                           args.cfa_version,
                           args.common_date)
