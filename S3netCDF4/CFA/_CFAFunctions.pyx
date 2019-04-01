"""
  Functions to derive the structure of a split CFA-netCDF file.
  These functions calculate the partition sizes.
"""

__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import operator
import numpy
import json
import os
from functools import reduce

from S3netCDF4.CFA._CFAClasses import *

def _get_axis_types(dataset, dimensions):
    """Get the axis types for the variable.
        These can be T, Z, Y, X, N:
                     T - time - (axis="T", name="t??" or name contains "time")
                     Z - level - (axis="Z", name="z??" or name contains "level")
                     Y - y axis / latitude (axis="Y", name="lat*" or name="y??")
                     X - x axis / longitude (axis="X", name="lon*" or name="x??")
                     N - not defined."""

    axis_types = []
    # loop over the dimensions
    for d in dimensions:
        # dimension variables have the same name as the dimensions
        dim = dataset.variables[d]
        # see if there is an axis attribute
        if "axis" in dim.ncattrs():
            # there is so just add to the axis
            axis_types.append(str(dim.axis))
        else:
            # have to go via the name
            if (len(d) < 3 and d[0] == "t") or "time" in d:
                axis_types.append("T")
            elif (len(d) < 3 and d[0] == "z") or "level" in d:
                axis_types.append("Z")
            elif (len(d) < 3 and d[0] == "y") or d[0:3] == "lat":
                axis_types.append("Y")
            elif (len(d) < 3 and d[0] == "x") or d[0:3] == "lon":
                axis_types.append("X")
            else:
                axis_types.append("N")
    # check to see if all axis types are unknown
    all_N = True
    for a in axis_types:
        all_N &= (a == "N")
    # make a guess as to the axis type based on the shape of the array
    if all_N:
        # timeseries
        if len(dimensions) == 1:
            axis_types = ["T"]
        # 2D Y, X
        if len(dimensions) == 2:
            axis_types = ["Y", "X"]
        # 3D T, Y, X
        if len(dimensions) == 3:
            axis_types = ["T", "Y", "X"]
        # 4D T, Z, Y, X
        if len(dimensions) == 4:
            axis_types = ["T", "Z", "Y", "X"]

    return axis_types


def fill_slices(master_array_shape, elems):
    """Fill out the tuple of slices so that there is a slice for each dimension and each slice
    contains the indices explictly, rather than `None`."""
    # convert the slice into a "full slice" - i.e. having a slice for each dimension and having all the indices
    # explicitly numbered in the slice, without any "Nones"
    lmas = len(master_array_shape)
    # a list of slices - convert to a tuple at the end
    slices = []
    # check first whether this is a single slice or a tuple of them
    if type(elems) is slice:
        # fill in the first of the indices
        elems_list = elems.indices(master_array_shape[0])
        slices.append(slice(elems_list[0], elems_list[1]-1, elems_list[2]))
        # how many slices to fill for the rest of the dimensions
        fill_number = lmas - 1
        start_number = 1
    # check whether this is a single integer
    elif type(elems) is int:
        # loop over the array
        slices.append(slice(elems, elems, 1))
        fill_number = len(master_array_shape) - 1
        start_number = 1
    else:
        # check that length of elems is equal to or less than the master_array_shape
        assert(len(elems) <= len(master_array_shape))
        # fill the indices from the 0 index upwards
        for s in range(0, len(elems)):
            if type(elems[s]) is int:
                slices.append(slice(elems[s], elems[s], 1))
            else:
                elems_list = elems[s].indices(master_array_shape[s])
                slices.append(slice(elems_list[0], elems_list[1]-1, elems_list[2]))

        # where to fill out the rest of the indicates
        fill_number = lmas - len(elems)
        start_number = len(elems)
    # fill out the rest of the slices
    for s in range(0, fill_number):
        slices.append(slice(0, master_array_shape[start_number+s]-1, 1))
    return slices


def get_source_target_slices(partition, elem_slices):
    """Get the slice into the source subarray and the target slice for the destination subarray,
       based on the information in the partition and the subdomain of the master array defined
       by elem_slices.  elem_slices are the filled slices from the function above."""

    # create the default source slice: this is the shape of the subarray
    source_slice = []
    for sl in partition.subarray.shape:
        source_slice.append(CFASlice(0, sl, 1))
    # create the target slice from the location and the passed in elements
    # create the default slice first - we will modify this
    target_slice = []
    for pl in partition.location:
        target_slice.append(CFASlice(pl[0], pl[1], 1))

    # now modify the slice based on the elements passed in
    py_source_slice = []
    py_target_slice = []
    for p in range(0, len(target_slice)):
        # adjust the end target_slice if we are taking a subset of the data
        if elem_slices[p].stop < target_slice[p].stop:
            source_slice[p].stop -= target_slice[p].stop - elem_slices[p].stop
            target_slice[p].stop = elem_slices[p].stop
        # adjust the target start and end for the sub_slice
        target_slice[p].start -= elem_slices[p].start
        target_slice[p].stop -= elem_slices[p].start - 1
        # check if the elem started within the location
        if target_slice[p].start < 0:
            # source_slice.start is absolute value of ts
            source_slice[p].start = -1 * target_slice[p].start
            # target start is 0
            target_slice[p].start = 0

        py_source_slice.append(source_slice[p].to_pyslice())
        py_target_slice.append(target_slice[p].to_pyslice())

    return py_source_slice, py_target_slice
