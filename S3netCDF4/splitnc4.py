#! /usr/bin/env python
"""
Program      : splitnc4.py
Date         : 09/08/2017
Author       : Neil Massey
Organisation : CEDA, STFC RAL Space

Split a netCDF file into a number of smaller files, based on the required field_size.
The splitting strategy is as follows:
1. Split variables into separate files
2. Split variables into multiple sub arrays, based on a "chunk shape" computed to be an
   optimum "chunk shape", in terms of accessing either an entire field of data at one
   timestep, or a timeseries of a single point of data, in an analogy to HDF5 chunks.
   See:
    http://www.unidata.ucar.edu/blogs/developer/entry/chunking_data_choosing_shapes
   for how to determine the chunk shape
"""

import argparse
import operator
import os
from netCDF4 import Dataset
import numpy
import sys
import json


def num_vals(shape):
    """Return number of values in chunk of specified shape, given by a list of dimension lengths.

    shape -- list of variable dimension sizes"""
    if (len(shape) == 0):
        return 1
    return reduce(operator.mul, shape)


def get_axis_types(nc4_ds, var_name):
    """Get the axis types for the variable.
        These can be T, Z, Y, X, N:
                     T - time - (axis="T", name="t??" or name contains "time")
                     Z - level - (axis="Z", name="z??" or name contains "level")
                     Y - y axis / latitude (axis="Y", name="lat*" or name="y??")
                     X - x axis / longitude (axis="X", name="lon*" or name="x??")
                     N - not defined."""

    axis_types = []
    # get the variable
    var = nc4_ds.variables[var_name]
    # loop over the dimensions
    for d in var.dimensions:
        # dimension variables have the same name as the dimensions
        dim = nc4_ds.variables[d]
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


def get_linear_operations(c_subfield_shape, axis_types):
    """Get the number of operations required to read one spatial point for every timestep through
       the dataset.
       This is equal to: number of subfields in the T axis."""
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
        return c_subfield_shape[t_ax]
    else:
        # otherwise return -1
        return -1


def get_field_operations(c_subfield_shape, axis_types):
    """Get the number of operations required to read one 2D field of data at a particular timestep
       through the dataset.
       This is equal to: (X dimension / subfields in the X axis) *
                         (Y dimension / subfields in the Y axis)
    """
    # get the X and Y axes, if they exists
    x_ax = -1
    y_ax = -1
    if "X" in axis_types:
        x_ax = axis_types.index("X")
    if "Y" in axis_types:
        y_ax = axis_types.index("Y")

    # four possibilities:
    # 1. X & Y exist            : return subfields in X * subfields in Y
    # 2. X exists but Y doesn't : return subfields in X
    # 3. Y exists but X doesn't : return subfields in Y
    # 4. Neither X or Y exists  : return -1

    # logic optimised
    if not (x_ax == -1 or y_ax == -1):
        n_ops = c_subfield_shape[x_ax] * c_subfield_shape[y_ax]
    elif y_ax != -1:
        n_ops = c_subfield_shape[y_ax]
    elif x_ax != -1:
        n_ops = c_subfield_shape[x_ax]
    else:
        n_ops = -1

    return n_ops


def subdivide_field(var_shape, c_subfield_shape, axis_types, permitted_axes=["T"]):
    # calculate the number of elements per sub for the linear axis types
    n_per_subf = numpy.zeros((len(var_shape),),'i')
    for i in range(0, len(var_shape)):
        if axis_types[i] not in permitted_axes:
            n_per_subf[i] = int(1e6)
        # check that we are not going to subdivide more than the axis length!
        elif c_subfield_shape[i] >= var_shape[i]:
            n_per_subf[i] = int(1e6)
        else:
            n_per_subf[i] = c_subfield_shape[i]
    # get the minimum index
    min_i = numpy.argmin(n_per_subf)
    c_subfield_shape[min_i] += 1
    return c_subfield_shape


def calculate_subfield_shape(nc4_ds, var_name, max_field_size):
        """
        Return a 'good shape' for the small, individual fields (sub arrays)for a any-D variable,
        assuming balanced 1D/(n-1)D access

        nc4_ds         -- netCDF4 data type
        var_name       -- name of variable
        max_field_size -- desired maximum number of values in a field

        Returns floating point field lengths of a field shape that provides balanced access of
        1D subsets and 2D subsets of a netCDF or HDF5 variable var with any shape.
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

        # get the variable
        var = nc4_ds.variables[var_name]
        # get the axis_types
        axis_types = get_axis_types(nc4_ds, var_name)
        # the algorithm first calculates how many partitions each dimension should be split into
        # this is stored in c_subfield_divs
        # current subfield_repeats shape defaults to var shape
        c_subfield_divs = numpy.ones((len(var.shape),), 'i')
        # if the number of values in the field_shape is greater than max_field_size then divide
        while (num_vals(var.shape) / num_vals(c_subfield_divs)) > max_field_size:
            # get the linear access and the field access operations
            linear_ops = get_linear_operations(c_subfield_divs, axis_types)
            field_ops = get_field_operations(c_subfield_divs, axis_types)
            # choose to divide on field ops first, if the number of ops are equal
            if field_ops <= linear_ops:
                c_subfield_divs = subdivide_field(var.shape, c_subfield_divs, axis_types, ["X", "Y"])
            else:
                c_subfield_divs = subdivide_field(var.shape, c_subfield_divs, axis_types, ["T", "Z", "N"])

        # we have so far calculated the optimum number of times each axis will be divided
        # translate this into a (floating point) number of elements in each chunk, for each axis
        c_subfield_shape = numpy.array(var.shape, 'f') / c_subfield_divs

        return c_subfield_divs, c_subfield_shape


def calculate_index_from_indices(indices, subfield_shape):
    assert(len(indices) == len(subfield_shape))
    # add the last index
    idx = indices[-1]
    # now loop backwards from the 2nd to last index to the first,
    # adding the size of the field multiplied by the index
    field_size = 1
    for i in range(len(indices)-2, -1, -1):
        # calculate the field size
        field_size *= subfield_shape[i+1]
        idx += indices[i] * field_size
    return idx


def calculate_indices_from_index(index, subfield_shape):
    indices = numpy.zeros((len(subfield_shape),),'i')
    field_size = 1
    for i in range(1, len(subfield_shape)):
        field_size *= subfield_shape[i]
    for i in range(0, len(subfield_shape)):
        v = index / field_size
        indices[i] = v
        if i < len(subfield_shape) - 1:
            field_size /= subfield_shape[i+1]

    return indices


def build_list_of_indices(nsf, var_shape, subfield_shape):

    # we need two set of subfields - the start and the end, in order to subset the fields correctly, due to
    # the possibility of a half a grid box being cast to 0
    all_subfields = numpy.zeros((nsf, 2, len(subfield_shape),),'i')
    all_indices = numpy.zeros((nsf, len(subfield_shape),),'i')
    # create the current subfield and set it to zero
    c_subfield = numpy.zeros((len(subfield_shape),),'f')
    # create the current index
    c_index = numpy.zeros((len(subfield_shape),), 'f')

    # iterate through all the subfields
    for s in range(0, nsf):
        all_subfields[s,0,:] = c_subfield[:]
        all_subfields[s,1,:] = c_subfield[:] + subfield_shape[:]
        all_indices[s,:] = c_index[:]
        c_subfield[-1] += subfield_shape[-1]
        c_index[-1] += 1
        for i in range(len(subfield_shape)-1, -1, -1):
            if c_subfield[i] >= var_shape[i]:
                c_subfield[i] = 0
                c_subfield[i-1] += subfield_shape[i-1]
                c_index[i] = 0
                c_index[i-1] += 1

    return all_subfields.astype('i'), all_indices


def create_sub_file(sub_path, in_nc4_ds, var_info):
    """
       :param sub_path: the (prefix) path to write the file to
       :param nc4_ds: the original file netCDF4 dataset
       :param var_info: variable info, including the calculated subfield size
       :return:
    """

    # get the global attributes
    in_glob_atts = {}
    for a in in_nc4_ds.ncattrs():
        in_glob_atts[a] = in_nc4_ds.getncattr(a)

    # get the variable from the input
    in_nc4_var = in_nc4_ds.variables[var_info[0]]

    # get the variable attributes
    in_var_atts = {}
    for a in in_nc4_var.ncattrs():
        in_var_atts[a] = in_nc4_var.getncattr(a)

    # need to cope with rotated grids, these should be stored in the "grid_mapping" variable
    if "grid_mapping" in in_var_atts:
        grid_map_var_name = in_var_atts["grid_mapping"]
        grid_map_in_var = in_nc4_ds.variables[grid_map_var_name]
        # copy the attributes
        grid_map_var_atts = {}
        for a in grid_map_in_var.ncattrs():
            grid_map_var_atts[a] = grid_map_in_var.getncattr(a)
    else:
        grid_map_var_name = ""

    # get the number of subfiles needed - the number of values / the number of values in a subfield
    n_subfiles = int(num_vals(var_info[1]) / num_vals(var_info[2]))

    # build a list of subfile indices into the field
    subfield_indices, partition_indices = build_list_of_indices(n_subfiles, var_info[1], var_info[2])

    # get the dimensions and their data
    in_dims = []
    for d in in_nc4_var.dimensions:
        in_dim = in_nc4_ds.dimensions[d]
        # get the dimension variable
        in_dim_var = in_nc4_ds.variables[in_dim.name]
        # get the dimension variable metadata
        in_dim_var_atts = {}
        for a in in_dim_var.ncattrs():
            in_dim_var_atts[a] = in_dim_var.getncattr(a)

        # get the dimension data
        in_dim_data = in_dim_var[:]
        in_dims.append((in_dim.name, in_dim_var.dtype, in_dim_var_atts, in_dim_data))

    # create the metadata required for the CFA file
    cfa_meta_data = {}
    cfa_meta_data["pmdimensions"] = [in_dims[d][0] for d in range(0, len(in_dims))]
    cfa_meta_data["pmshape"] = var_info[3].tolist()
    cfa_meta_data["base"] = os.path.dirname(sub_path)
    cfa_meta_data["Partitions"] = []

    # loop over all the subfiles
    for sf in range(0, n_subfiles):
        # create the output netCDF4 file
        # always create as a netCDF4 file, upgrading all other (netCDF3, etc.) file types to the latest file type
        out_fname = sub_path + "_[" + str(sf) + "].nc"
        out_nc4_ds = Dataset(out_fname, 'w', clobber=True, format="NETCDF4")

        # copy the global metadata
        out_nc4_ds.setncatts(in_glob_atts)

        # calculate the slice, for both the start and end slice
        in_range = []
        in_slice = []

        for i in range(0, len(var_info[2])):
            ss = int(subfield_indices[sf,0,i])
            es = int(subfield_indices[sf,1,i])
            # check for es > shape
            if es >= var_info[1][i]:
                es = int(var_info[1][i])
            in_slice.append(slice(ss,es))
            in_range.append((ss, es))

        # copy the variables dimensions, however the sizes have changed to match the subfield sizes
        for d in range(0, len(in_dims)):
            # create the dimension
            out_dim = out_nc4_ds.createDimension(in_dims[d][0], in_range[d][1] - in_range[d][0])
            # create the dimension variable
            out_dim_var = out_nc4_ds.createVariable(in_dims[d][0], in_dims[d][1], (in_dims[d][0],))
            # copy the dimension variable metadata
            out_dim_var.setncatts(in_dims[d][2])
            # copy the dimension variable data
            out_dim_var[:] = in_dims[d][3][in_range[d][0]:in_range[d][1]]

        # need to cope with rotated grid
        if grid_map_var_name != "":
            grid_map_out_var = out_nc4_ds.createVariable(grid_map_in_var.name, grid_map_in_var.dtype)
            grid_map_out_var.setncatts(grid_map_var_atts)

        # create the output variable
        out_nc4_var = out_nc4_ds.createVariable(in_nc4_var.name, in_nc4_var.dtype, in_nc4_var.dimensions)
        # copy the input variable attributes to the output variable
        out_nc4_var.setncatts(in_var_atts)

        # now copy the data - we need a subset, using the start and end slice
        out_nc4_var[:] = in_nc4_var[in_slice]
        out_shape = out_nc4_var.shape

        # close file to finish write
        out_nc4_ds.close()

        # add the meta_data for the Partition
        partition_md = {"index" : partition_indices[sf].tolist(),
                        "location" : subfield_indices[sf,0].tolist(),
                        "subarray" : {"format" : "netCDF",
                                      "shape" : out_shape,
                                      "file" : os.path.basename(out_fname),
                                      "ncvar" : in_nc4_var.name}}
        cfa_meta_data["Partitions"].append(partition_md)

    return json.dumps(cfa_meta_data)


def split_nc4_file(input_file, field_size):
    """Split the netCDF file into smaller subarray files, creating a CFA file to hold the master array"""
    # open the netCDF4 Dataset
    in_nc4_ds = Dataset(input_file)

    # create the directory to store the subsets in
    sub_dir = input_file[:-3]
    sub_prefix = os.path.basename(sub_dir)
    if not os.path.isdir(sub_dir):
        os.makedirs(sub_dir)

    # create the CFA-netCDF file to store the dimensions and metadata in
    output_file = input_file[:-3] + ".nca"
    out_cfa_ds = Dataset(output_file, 'w')

    # copy the global metadata
    in_nc4_atts = {}
    for a in in_nc4_ds.ncattrs():
        in_nc4_atts[a] = in_nc4_ds.getncattr(a)
    # add to the conventions
    if "Conventions" in in_nc4_atts:
        in_nc4_atts["Conventions"] += " CFA-0.3"
    else:
        in_nc4_atts["Conventions"] = "CF-1.3 CFA-0.3"

    out_cfa_ds.setncatts(in_nc4_atts)

    # copy the dimensions
    for d in in_nc4_ds.dimensions:
        # copy the dimension
        in_dim = in_nc4_ds.dimensions[d]
        out_dim = out_cfa_ds.createDimension(in_dim.name, in_dim.size)

        # copy the dimension variable
        in_dim_var = in_nc4_ds.variables[in_dim.name]
        out_dim_var = out_cfa_ds.createVariable(in_dim.name, in_dim_var.dtype, (in_dim.name,))
        out_dim_var[:] = in_dim_var[:]

        # copy the dimension variable metadata
        in_var_atts = {}
        for a in in_dim_var.ncattrs():
            in_nc4_atts[a] = in_dim_var.getncattr(a)
        out_dim_var.setncatts(in_nc4_atts)

    # loop over all the groups in the file
    # loop over all the variables in the file
    for in_var in in_nc4_ds.variables:
        # get the variable
        in_nc4_var = in_nc4_ds.variables[in_var]
        # add to the var list or dim list
        in_var_shape = in_nc4_var.shape
        if not(in_var in in_nc4_ds.dimensions or len(in_var_shape) == 0):
            # calculate field shape if this is a variable
            subfield_divs, subfield_shape = calculate_subfield_shape(in_nc4_ds, in_var, field_size)
            v = [in_var, in_var_shape, subfield_shape, subfield_divs]
            # create the sub_path for this variable
            sub_path = sub_dir + "/" + sub_prefix + "_" + v[0]
            cfa_meta_data = create_sub_file(sub_path, in_nc4_ds, v)

            # create the cfa variable
            out_var = out_cfa_ds.createVariable(in_var, in_nc4_var.dtype, in_nc4_var.dimensions)

            # copy the variable attributes and add the cfa attributes
            in_var_atts = {}
            for a in in_nc4_var.ncattrs():
                in_var_atts[a] = in_nc4_var.getncattr(a)
            # add the cf-role
            in_var_atts["cf_role"] = "cfa_variable"
            # add the dimensions
            in_var_atts["cf_dimensions"] = [in_nc4_var.dimensions[d][0] for d in range(0, len(in_nc4_var.dimensions))]
            # add the cf meta data
            in_var_atts["cfa_array"] = cfa_meta_data
            # set the attributes
            out_var.setncatts(in_var_atts)

            # close the input dataset
    in_nc4_ds.close()

    # close the output dataset
    out_cfa_ds.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Split a netCDF file into subarray files')
    parser.add_argument('input_file', action="store", help="Input file name")
    parser.add_argument('--field_size', type=int, action='store', default=2048, help="Field size (number of values) for each subarray")

    args = parser.parse_args()

    # split the file
    split_nc4_file(args.input_file, args.field_size)

    # test datasets:
    # ~/Archive/weather_at_home/data/1314Floods/a_series/hadam3p_eu_a7tz_2013_1_008571189_0/a7tzga.pdl3dec.nc
    # ~/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1901.1910.tmp.dat.nc