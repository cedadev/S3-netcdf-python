#! /usr/bin/env python

"""Program to delete cfa files from disk or s3.
This program will delete all of the sub-array files as well as the master array file."""

import argparse
import os

from netCDF4 import Dataset

from S3netCDF4._s3Client import s3Client, s3ClientConfig
from S3netCDF4._s3netCDFIO import get_netCDF_file_details, get_endpoint_bucket_object
from S3netCDF4._s3Exceptions import *
from S3netCDF4._CFAClasses import *

def delete_s3_netcdf_file(master_array_fname, client_config, force_delete=False):
    # Open the master-array file
    try:
        file_details = get_netCDF_file_details(master_array_fname, 'r', s3_client_config=client_config)
    except s3IOException:
        print "File: {} not found.".format(master_array_fname)
        return

    # Stream the file to disk - the filename will be "NETCDF4_dummy.nc" in the user's cache directory
    # The path will be stored in file_details.filename
    # we have to first create the dummy file - check it exists before creating it
    if "s3://" in master_array_fname:
        if not os.path.exists(file_details.filename):
            temp_file = Dataset(file_details.filename, 'w', format=file_details.format).close()
    # create the netCDF4 dataset from the data, using the temp_file
    if file_details.memory != "":
        nc_dataset = Dataset(file_details.filename, mode='r',
                            format=file_details.format, diskless=True, persist=False,
                            memory=file_details.memory)
    else:
        nc_dataset = Dataset(file_details.filename, mode='r', format=file_details.format)

    # Parse the file
    cfa_file = CFAFile()
    cfa_file.parse(nc_dataset)

    # Count how many subarrays there are
    n_files = 0
    for v in cfa_file.cfa_vars:
        n_files += len(cfa_file.cfa_vars[v].partitions)

    # Confirm deletion if force_delete is False
    if not force_delete:
        resp = raw_input("\033[91m"+"Delete master-array file and {} associated sub-array files [Y/N]? ".format(n_files) + "\033[0m")
        if resp == 'n' or resp == 'N':
            return

    if "s3://" in master_array_fname:
        # get the endpoint, bucket name, object name of the master array and create a s3_client
        s3_ep, master_array_bucket, master_array_object = get_endpoint_bucket_object(master_array_fname)
        client = s3Client(s3_ep, client_config)

    # we're going to delete everything!
    for v in cfa_file.cfa_vars:
        # loop over partitions
        for p in cfa_file.cfa_vars[v].partitions:
            sub_array_fname = p.subarray.file
            if "s3://" in sub_array_fname:
                # break the fname into endpoint, bucket, object
                sub_array_ep, sub_array_bucket, sub_array_object = get_endpoint_bucket_object(sub_array_fname)
                # check that the object exists
                if client.object_exists(sub_array_bucket, sub_array_object):
                    # delete the sub-array file
                    client.delete(sub_array_bucket, sub_array_object)
            else:
                if os.path.exists(sub_array_fname):
                    os.unlink(sub_array_fname)
                else:
                    print ("{} not found, not deleting.".format(sub_array_fname))

    if "s3://" in master_array_fname:
        # delete the master-array file
        client.delete(master_array_bucket, master_array_object)

        # is the bucket empty? - delete if it is
        if client.bucket_empty(master_array_bucket):
            client.delete_bucket(master_array_bucket)
    else:
        # delete the master array file
        os.unlink(master_array_fname)
        # delete the directory
        master_array_directory = os.path.splitext(master_array_fname)[0]
        if os.path.exists(master_array_directory):
            os.rmdir(master_array_directory)


if __name__ == "__main__":

    # set up and parse the arguments
    parser = argparse.ArgumentParser(prog="s3_cfa_rm", formatter_class=argparse.RawTextHelpFormatter,
                                     description="Delete a CFA-netCDF master-array file and all its sub-array files.")
    parser.add_argument("file", action="store", default="",
                        help="Path of the CFA-netCDF master-array to delete.  Path can either be on s3 storage or file system.")
    parser.add_argument("-f", action="store_true", default=False, help="Force deletion without prompt")

    args = parser.parse_args()

    # get a client config
    client_config = s3ClientConfig()

    if args.f:
        force_delete = True
    else:
        force_delete = False

    if args.file:
        delete_s3_netcdf_file(args.file, client_config, force_delete)
