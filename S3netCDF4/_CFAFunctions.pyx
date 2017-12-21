"""
  Functions to derive the structure of a split CFA-netCDF file.
  These functions calculate the partition sizes.
"""

import operator
import numpy
import json
import os

from _CFAClasses import *

CFA_VERSION = "CFA-0.4"
DEFAULT_OBJECT_SIZE = 2*1024*1024 # 2MB default object size

def _num_vals(shape):
    """Return number of values in chunk of specified shape, given by a list of dimension lengths.

    shape -- list of variable dimension sizes"""
    if (len(shape) == 0):
        return 1
    return reduce(operator.mul, shape)


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
    return axis_types


def _get_linear_operations(c_subarray_shape, axis_types):
    """Get the number of operations required to read one spatial point for every timestep through
       the dataset.
       This is equal to: number of subarrays in the T axis."""
    # get the t axis index, if it exists, otherwise the Z axis, otherwise the N axis
    t_ax = -1
    if "T" in axis_types:
        t_ax = axis_types.index("T")
    elif "Z" in axis_types:
        t_ax = axis_types.index("Z")
    elif "N" in axis_types:
        t_ax = axis_types.index("N")

    # calculate number of operations
    if t_ax != -1:
        return c_subarray_shape[t_ax]
    else:
        # otherwise return -1
        return -1


def _get_field_operations(c_subarray_shape, axis_types):
    """Get the number of operations required to read one 2D field of data at a particular timestep
       through the dataset.
       This is equal to: (X dimension / subarrays in the X axis) *
                         (Y dimension / subarrays in the Y axis)
    """
    # get the X and Y axes, if they exists
    x_ax = -1
    y_ax = -1
    if "X" in axis_types:
        x_ax = axis_types.index("X")
    if "Y" in axis_types:
        y_ax = axis_types.index("Y")

    # four possibilities:
    # 1. X & Y exist            : return subarrays in X * subarrays in Y
    # 2. X exists but Y doesn't : return subarrays in X
    # 3. Y exists but X doesn't : return subarrays in Y
    # 4. Neither X or Y exists  : return -1

    # logic optimised
    if not (x_ax == -1 or y_ax == -1):
        n_ops = c_subarray_shape[x_ax] * c_subarray_shape[y_ax]
    elif y_ax != -1:
        n_ops = c_subarray_shape[y_ax]
    elif x_ax != -1:
        n_ops = c_subarray_shape[x_ax]
    else:
        n_ops = -1

    return n_ops


def _subdivide_array(var_shape, c_subarray_shape, axis_types, permitted_axes=["T"]):
    # calculate the number of elements per sub for the linear axis types
    n_per_subf = numpy.zeros((len(var_shape),),'i')
    for i in range(0, len(var_shape)):
        if axis_types[i] not in permitted_axes:
            n_per_subf[i] = int(1e6)
        # check that we are not going to subdivide more than the axis length!
        elif c_subarray_shape[i] >= var_shape[i]:
            n_per_subf[i] = int(1e6)
        else:
            n_per_subf[i] = c_subarray_shape[i]
    # get the minimum index
    min_i = numpy.argmin(n_per_subf)
    c_subarray_shape[min_i] += 1
    return c_subarray_shape


def _calculate_subarray_shape(dataset, dimensions, var_shape, dtype,
                              max_file_size=DEFAULT_OBJECT_SIZE): # 2MB default object size
    """
    Return a 'good shape' for the sub-arrays for an any-D variable,
    assuming balanced 1D/(n-1)D access

    in_nc_ds       -- netCDF4 data type
    var_name       -- name of variable
    max_field_size -- desired maximum number of values in a field

    Returns floating point field lengths of a field shape that provides balanced access of
    1D subsets and 2D subsets of a netCDF or HDF5 variable with any shape.
    'Good shape' for fields means that the number of fields accessed to read either
    kind of 1D or 2D subset is approximately equal, and the size of each field
    is no more than field_size.
    An extra complication here is that we wish to be able to optimise for any number of
    dimensions (1,2,3,4, etc.) but ensure that the algorithm knows which axis it is
    operating on.  For example, a 2D field with X and Y axes should not be split in
    the same way as a 2D field with T and Z axes.

    The algorithm follows a sub-division process, in this order (if they exist):
        1. sub divide the X axis
        2. sub divide the T axis
        3. sub divide the Y axis
        4. sub divide the Z axis
        5. sub divide any N axes

    Calculating the access operations:
        There are two "types" of access operations
         - linear (accessing a single spatial point across timesteps)
         - field  (accessing a 2D field of data at a particular timestep)
        The number of access operations are:
         - linear :  T dimension / number of subfields in the T axis
         - field  : (X dimension / number of subfields in the X axis)*
                    (Y dimension / number of subfields in the Y axis)
    """

    # calculate the maximum number of fields = max_file_size / size of dtype of data
    max_field_size = max_file_size / dtype.itemsize
    # get the axis_types
    axis_types = _get_axis_types(dataset, dimensions)
    # the algorithm first calculates how many partitions each dimension should be split into
    # this is stored in c_subfield_divs
    # current subfield_repeats shape defaults to var shape
    c_subarray_divs = numpy.ones((len(var_shape),), 'i')
    # if the number of values in the field_shape is greater than max_field_size then divide
    while (_num_vals(var_shape) / _num_vals(c_subarray_divs)) > max_field_size:
        # get the linear access and the field access operations
        linear_ops = _get_linear_operations(c_subarray_divs, axis_types)
        field_ops = _get_field_operations(c_subarray_divs, axis_types)
        # choose to divide on field ops first, if the number of ops are equal
        if field_ops <= linear_ops:
            c_subarray_divs = _subdivide_array(var_shape, c_subarray_divs, axis_types, ["X", "Y"])
        else:
            c_subarray_divs = _subdivide_array(var_shape, c_subarray_divs, axis_types, ["T", "Z", "N"])

    # we have so far calculated the optimum number of times each axis will be divided
    # translate this into a (floating point) number of elements in each chunk, for each axis
    c_subarray_shape = numpy.array(var_shape, 'f') / c_subarray_divs

    return c_subarray_shape


def _build_list_of_indices(n_subarrays, var_shape, subarray_shape):
    # calculate the indices for each metadata component in the CFA standard:
    # n_subarrays = number of subfields
    # location - the indices of the location of the sub-array in the master-array
    #          - has a start [:,0,:] and end [:,1,:] set of indices
    # pindex   - the index into the partition map

    pindex   = numpy.zeros((n_subarrays, len(subarray_shape),),'i')
    location = numpy.zeros((n_subarrays, len(subarray_shape), 2),'i')

    # create the current location and set it to zero
    c_location = numpy.zeros((len(subarray_shape),),'f')
    # create the current partition index
    c_pindex = numpy.zeros((len(subarray_shape),), 'f')

    # iterate through all the subarrays
    for s in range(0, n_subarrays):
        location[s,:,0] = c_location[:]
        location[s,:,1]  = c_location[:] + subarray_shape[:]
        pindex[s,:] = c_pindex[:]
        c_location[-1] += subarray_shape[-1]
        c_pindex[-1] += 1
        for i in range(len(subarray_shape)-1, -1, -1):
            if c_location[i] >= var_shape[i]:
                c_location[i] = 0
                c_location[i-1] += subarray_shape[i-1]
                c_pindex[i] = 0
                c_pindex[i-1] += 1

    return pindex, location


def create_partitions(base_filepath, dataset, dimensions,
                      varname, var_shape, dtype,
                      max_file_size=DEFAULT_OBJECT_SIZE,
                      format="NETCDF4"):
    """Create the CFAPartition(s) from the input data."""
    # get the axis types for the dimensions
    subarray_shape = _calculate_subarray_shape(dataset, dimensions, var_shape,
                                               dtype, max_file_size)
    # calculate the pmshape = var_shape / subarray_shape
    pmshape = numpy.array(var_shape) / subarray_shape

    # calculate the number of subarrays needed
    n_subarrays = int(_num_vals(var_shape) / _num_vals(subarray_shape))

    # build a list of indices into the master array and where these fit into the partition map
    pindex, location = _build_list_of_indices(n_subarrays, var_shape, subarray_shape)

    # get the base_filename - last part of base_filepath
    base_filename = os.path.basename(base_filepath)

    # each of these is a partition
    partitions = []
    for sa in range(0, n_subarrays):
        # create the subarray first
        # output shape is just the difference between the location indices
        out_shape = location[sa,:,1] - location[sa,:,0]
        # get the sub file name
        sub_filename = base_filepath + "/" + base_filename + "_" + varname + "_[" + str(sa) + "].nc"
        cfa_subarray = CFASubarray(varname, sub_filename, format, out_shape)
        # create the output location
        out_location = numpy.array(location[sa])
        # sub 1 from last output location to reflect that indices are inclusive
        out_location[:,1] -= 1
        # create the partition and append to the partition list
        partition = CFAPartition(pindex[sa], out_location, cfa_subarray)
        partitions.append(partition)

    return pmshape, partitions


def partition_overlaps(partition, slices):
    """Check whether a partition overlaps with the start and end indices"""
    if len(partition.location) == 0:
        return False
    assert(len(partition.location) == len(slices))
    overlaps = True
    for i in range(0, len(partition.location)):
        # overlap if the slice start index lies between the start and end index for this partition
        # OR if the slice end index lies between the start and end index for this partition
        # OR if the partition is contained entirely within the slice
        # get the slice
        s = slices[i]
        p = partition.location[i]
        start_overlaps = (s.start >= p[0]) & (s.start <= p[1])
        end_overlaps = (s.stop >= p[0]) & (s.stop <= p[1])
        partition_in = (p[0] >= s.start) & (p[1] <= s.stop)
        overlaps &= (start_overlaps | end_overlaps | partition_in)
    return overlaps


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
