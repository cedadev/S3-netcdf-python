"""Quick hacky test to determine suitability of using Cython parallel and multiprocessor to
enable multiple download streams of netCDF files in S3 storage and reconstruction into a
memory-mapped numpy array."""

import sys, os
sys.path.append(os.path.expanduser("~/Coding/S3-netcdf-python/"))
sys.path.append(os.path.expanduser("~/Coding/S3-netcdf-python/archive2"))

from S3netCDF4._s3netCDFIO import get_netCDF_file_details
from netCDF4._netCDF4 import Dataset
from _CFAClasses import *

import json
from timeit import default_timer as timer
import numpy as np

import threading
import multiprocessing

import numpy as np
import pyximport
pyximport.install(setup_args={"include_dirs":np.get_include()})

# these are the file details in a chunk of JSON, this is from the CFA file
cfa_json = '''{"Partitions":
                [
                  {"index": [0, 0, 0, 0], "location": [0, 0, 0, 0], "subarray": {"shape": [60, 1, 59, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[0].nc", "format": "netCDF"}},
                  {"index": [0, 0, 0, 1], "location": [0, 0, 0, 61], "subarray": {"shape": [60, 1, 59, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[1].nc", "format": "netCDF"}},
                  {"index": [0, 0, 1, 0], "location": [0, 0, 59, 0], "subarray": {"shape": [60, 1, 60, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[2].nc", "format": "netCDF"}},
                  {"index": [0, 0, 1, 1], "location": [0, 0, 59, 61], "subarray": {"shape": [60, 1, 60, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[3].nc", "format": "netCDF"}},
                  {"index": [1, 0, 0, 0], "location": [60, 0, 0, 0], "subarray": {"shape": [61, 1, 59, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[4].nc", "format": "netCDF"}},
                  {"index": [1, 0, 0, 1], "location": [60, 0, 0, 61], "subarray": {"shape": [61, 1, 59, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[5].nc", "format": "netCDF"}},
                  {"index": [1, 0, 1, 0], "location": [60, 0, 59, 0], "subarray": {"shape": [61, 1, 60, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[6].nc", "format": "netCDF"}},
                  {"index": [1, 0, 1, 1], "location": [60, 0, 59, 61], "subarray": {"shape": [61, 1, 60, 61], "ncvar": "field1", "file": "a7tzga.pdl3dec/a7tzga.pdl3dec_field1_[7].nc", "format": "netCDF"}}
                ],
               "base": "s3://minio/weather-at-home/data/1314Floods/a_series/hadam3p_eu_a7tz_2013_1_008571189_0/",
               "pmdimensions": ["time0", "z0", "latitude0", "longitude0"],
               "pmshape": [2, 1, 2, 2]
              }'''


def transfer_netCDF_file(cfa, part_n):
    """Get a netCDF file from the partition info"""
    part = cfa["Partitions"][part_n]
    suba = part["subarray"]
    file_name = os.path.join(cfa["base"], suba["file"])
    file_details = get_netCDF_file_details(file_name, 'r')
    # open the netCDF file
    if file_details.memory == "":
        fh = Dataset(file_details.filename, mode=file_details.filemode, diskless=False)
    else:
        fh = Dataset(file_details.filename, mode=file_details.filemode, diskless=True,
                     memory=file_details.memory)

if __name__ == "__main__":
    cfa = json.loads(cfa_json, encoding="utf-8")
    # rejig the location to match the actual cfa definition
    for part in cfa["Partitions"]:
        loc = part["location"]
        shp = part["subarray"]["shape"]
        new_loc = [[loc[0], loc[0]+shp[0]-1],
                   [loc[1], loc[1]+shp[1]-1],
                   [loc[2], loc[2]+shp[2]-1],
                   [loc[3], loc[3]+shp[3]-1]]
        part["location"] = new_loc

    print "Serial opening..."
    start = timer()

    for i in range(0, len(cfa["Partitions"])):
        fd = transfer_netCDF_file(cfa, i)

    end = timer()
    print "   " + str(end-start)


    print "Threading opening..."
    start = timer()
    n_part = len(cfa["Partitions"])
    for i in range(0, n_part):
        p = threading.Thread(target=transfer_netCDF_file, args=(cfa,i))
        p.setDaemon(True)
        p.start()

    main_thread = threading.currentThread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        t.join()

    end = timer()
    print "   " + str(end-start)


    print "Multiprocessing opening..."
    start = timer()

    processes = []
    for i in range(0, n_part):
        p = multiprocessing.Process(target=transfer_netCDF_file, args=(cfa,i))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    end = timer()
    print "   " + str(end-start)
