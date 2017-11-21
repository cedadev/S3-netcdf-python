"""
Write a netCDF file to S3 object store by first splitting the file up into several subfiles (sub-arrays)
using the CFA conventions.  Then write the CFA file (master-array) which references the sub-arrays.

Author: Neil Massey
Date:   15/08/2017
"""

import operator
import numpy
import os
import netCDF4._netCDF4 as netCDF4
import json

CFA_VERSION = "CFA-0.4"

def _num_vals(shape):
    """Return number of values in chunk of specified shape, given by a list of dimension lengths.

    shape -- list of variable dimension sizes"""
    if (len(shape) == 0):
        return 1
    return reduce(operator.mul, shape)


def _get_axis_types(in_nc_ds, var_name):
    """Get the axis types for the variable.
        These can be T, Z, Y, X, N:
                     T - time - (axis="T", name="t??" or name contains "time")
                     Z - level - (axis="Z", name="z??" or name contains "level")
                     Y - y axis / latitude (axis="Y", name="lat*" or name="y??")
                     X - x axis / longitude (axis="X", name="lon*" or name="x??")
                     N - not defined."""

    axis_types = []
    # get the variable
    var = in_nc_ds.variables[var_name]
    # loop over the dimensions
    for d in var.dimensions:
        # dimension variables have the same name as the dimensions
        dim = in_nc_ds.variables[d]
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


def _calculate_subarray_shape(in_nc_ds, var_name, max_file_size):
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

    # get the variable
    var = in_nc_ds.variables[var_name]
    # calculate the maximum number of fields = max_file_size / size of dtype of data
    max_field_size = max_file_size / var.dtype.itemsize
    # get the axis_types
    axis_types = _get_axis_types(in_nc_ds, var_name)
    # the algorithm first calculates how many partitions each dimension should be split into
    # this is stored in c_subfield_divs
    # current subfield_repeats shape defaults to var shape
    c_subarray_divs = numpy.ones((len(var.shape),), 'i')
    # if the number of values in the field_shape is greater than max_field_size then divide
    while (_num_vals(var.shape) / _num_vals(c_subarray_divs)) > max_field_size:
        # get the linear access and the field access operations
        linear_ops = _get_linear_operations(c_subarray_divs, axis_types)
        field_ops = _get_field_operations(c_subarray_divs, axis_types)
        # choose to divide on field ops first, if the number of ops are equal
        if field_ops <= linear_ops:
            c_subarray_divs = _subdivide_array(var.shape, c_subarray_divs, axis_types, ["X", "Y"])
        else:
            c_subarray_divs = _subdivide_array(var.shape, c_subarray_divs, axis_types, ["T", "Z", "N"])

    # we have so far calculated the optimum number of times each axis will be divided
    # translate this into a (floating point) number of elements in each chunk, for each axis
    c_subarray_shape = numpy.array(var.shape, 'f') / c_subarray_divs

    return c_subarray_shape


def _build_list_of_indices(nsf, var_shape, subarray_shape):
    # calculate the indices for each metadata component in the CFA standard:
    # location - the indices of the location of the sub-array in the master-array
    #          - has a start [:,0,:] and end [:,1,:] set of indices
    # pindex   - the index into the partition map

    pindex   = numpy.zeros((nsf, len(subarray_shape),),'i')
    location = numpy.zeros((nsf, len(subarray_shape), 2),'i')

    # create the current location and set it to zero
    c_location = numpy.zeros((len(subarray_shape),),'f')
    # create the current partition index
    c_pindex = numpy.zeros((len(subarray_shape),), 'f')

    # iterate through all the subarrays
    for s in range(0, nsf):
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


class CFAfile():
    """
        Class to hold details of a CFA master-array file, including metadata from the original netCDF file
    """
    def __init__(self, in_nc_ds, filename, max_file_size, s3_url=""):
        """
        Initialise the CFA master-array object and call subfunctions to determine the metadata
        :param in_nc_ds: input netcdf dataset
        """
        # keep a copy of the input netcdf dataset
        self._in_nc_ds = in_nc_ds

        # store the maximum file size
        self._max_file_size = max_file_size

        # get the filename - this will be the cache filename for the s3 client
        self._filename = filename

        # get the url - this will be used in the CFA metadata to point to the subarray files
        self._s3_url = s3_url

        # create the sub-arrays
        self._create_variables_sub_array_metadata()


    def write(self, format="NETCDF4"):
        """
        Write out the sub-array CF-netCDF files, and the master-array CFA-netCDF file
        :return:
        """
        # write out the nca file first
        # get the sub_directory name from the filename
        if ".nc" in self._filename:
            sub_dir = self._filename[:-3]
        else:
            sub_dir = self._filename

        # create the sub directory if it doesn't exist
        if not os.path.isdir(sub_dir):
            os.makedirs(sub_dir)

        # output filename is the sub directory + ".nca
        out_file = sub_dir + ".nca"

        # create the nca file - always overwrite
        out_cfa_ds = netCDF4.Dataset(out_file, 'w', format=format, clobber=True)

        # copy the global metadata
        out_atts =  {k: self._in_nc_ds.getncattr(k) for k in self._in_nc_ds.ncattrs()}
        # add to the conventions
        if "Conventions" in out_atts:
            out_atts["Conventions"] += " " + CFA_VERSION
        else:
            out_atts["Conventions"] = CFA_VERSION
        # set the attributes
        out_cfa_ds.setncatts(out_atts)

        # copy the dimensions from the input
        for d in self._in_nc_ds.dimensions:
            # copy the dimension
            in_dim = self._in_nc_ds.dimensions[d]
            out_dim = out_cfa_ds.createDimension(in_dim.name, in_dim.size)

            # copy the dimension variable
            in_dim_var = self._in_nc_ds.variables[in_dim.name]
            out_dim_var = out_cfa_ds.createVariable(in_dim.name, in_dim_var.dtype, (in_dim.name,))

            # copy the dimension variable metadata
            out_dim_var.setncatts({k: in_dim_var.getncattr(k) for k in in_dim_var.ncattrs()})

            # copy the dimension variable data
            out_dim_var[:] = in_dim_var[:]

        # write out the variables, CFAfile first, then the subarray files
        for sa in self._variables:
            # get the input variable from the input file, using the name in the metadata
            in_var = sa._in_nc_ds.variables[sa._in_var_name]
            out_var = out_cfa_ds.createVariable(in_var.name, in_var.dtype)#, in_var.dimensions)
            # get the variable attributes
            in_var_atts = ({k: in_var.getncattr(k) for k in in_var.ncattrs()})
            # add the cf-role
            in_var_atts["cf_role"] = "cfa_variable"
            # add the dimensions
            in_var_atts["cfa_dimensions"] = " ".join(in_var.dimensions)
            # add the cf meta data
            in_var_atts["cfa_array"] = json.dumps(sa._cfa_meta_data)
            # set the attributes
            out_var.setncatts(in_var_atts)

        # finish writing the cfa file
        out_cfa_ds.close()

        return os.path.basename(out_file)


    def get_number_of_variables(self):
        return len(self._variables)


    def get_number_of_subarrays(self, varnumber):
        """Get the total number of subarrays for a particular variable"""
        nsa = len(self._variables[varnumber]._cfa_meta_data["Partitions"])
        return nsa


    def write_subarray(self, varnumber, subarray_number, format = "NETCDF4"):
        # get the sub directory from the original filename
        if ".nc" in self._filename:
            sub_dir = self._filename[:-3]
        else:
            sub_dir = self._filename

        # loop over the variables and write them out
        var = self._variables[varnumber]
        return var.write(subarray_number, sub_dir, format = format)


    def _create_variables_sub_array_metadata(self):
        """
        Create the sub-array metadata for all of the variables, which is all we need to create the sub-array files
        """
        # create an empty list of sub arrays
        self._variables = []
        # get the sub directory name
        # use the s3 url if it exists
        if self._s3_url != "":
            sub_dir = self._s3_url
        else:
            sub_dir = self._filename

        # trim .nc from the name if it exists
        if ".nc" in sub_dir:
            sub_dir = sub_dir[:-3]

        # loop over all the variables
        for in_var in self._in_nc_ds.variables:
            # Create the CFA_Variables
            # don't try to subdivide dimension variables or variables with no size!
            in_var_shape = self._in_nc_ds.variables[in_var].shape
            if not (in_var in self._in_nc_ds.dimensions or len(in_var_shape) == 0):
                cfa_var = CFAvariable(self._in_nc_ds, in_var, sub_dir, self._max_file_size)
                self._variables.append(cfa_var)


class CFAvariable():
    """
        Class to hold details of a CFA sub-array file, including metadata from the original netCDF variable
    """

    def __init__(self, in_nc_ds, in_var_name, sub_dir, max_file_size):
        # keep the netCDF file handle to use later when writing
        self._in_nc_ds = in_nc_ds

        # keep the variable name
        self._in_var_name = in_var_name

        # suba = sub-array
        # calculate the number of divisions (in each dimension) required and the corresponding
        # sub-array size, to stay under the max_object_size
        suba_shape = _calculate_subarray_shape(in_nc_ds, in_var_name, max_file_size)

        # get the number of subfiles needed - the number of values / the number of values in a subfield
        in_var_shape = in_nc_ds.variables[in_var_name].shape
        n_subarrays = int(_num_vals(in_var_shape) / _num_vals(suba_shape))

        # build a list of subfile indices into the field
        pindex, location = _build_list_of_indices(n_subarrays, in_var_shape, suba_shape)

        # create the metadata
        self._cfa_meta_data = {}
        # dimensions in the partition matrix
        self._cfa_meta_data["pmdimensions"] = [dim for dim in in_nc_ds.variables[in_var_name].dimensions]
        # the shape of the partition matrix (number of divisions in each dimension)
        self._cfa_meta_data["pmshape"] = (in_var_shape / suba_shape).astype('i').tolist()
        # create an empty set of partitions
        self._cfa_meta_data["Partitions"] = []

        # loop over each subarray
        for sa in range(0, n_subarrays):
            # base path will be modified in write (below) as we don't know the path yet, get the sub_file part now
            # sub file names are <sub_dir>/<sub_dir>_<variable_name>_[<file_index>].nc
            cfa_sub_dir = os.path.basename(sub_dir)
            suba_filename = sub_dir + "/" + cfa_sub_dir + "_" + in_var_name + "_[" + str(sa) + "]" + ".nc"
            # calculate the output shape
            out_shape = location[sa,:,1] - location[sa,:,0]
            out_location = numpy.array(location)
            out_location[:,:,1] -= 1
            # create the
            # add the meta_data for the Partition
            partition_md = {"index": pindex[sa].tolist(),
                            "location": out_location[sa].tolist(),
                            "subarray": {"format": "netCDF",
                                         "shape": out_shape.tolist(),
                                         "file": suba_filename,
                                         "ncvar": in_var_name}}
            self._cfa_meta_data["Partitions"].append(partition_md)


    def write(self, subarray_number, path, format="NETCDF4"):
        """
        Write out one sub-array as a CF-netCDF file.  Changed to writing one sub-array at once so we can put it into
        a thread and write / upload multiple sub-arrays at once.
        in_nc_ds: the input netCDF dataset
        :return:
        """
        # get the sub-array metadata for this partition
        sa = self._cfa_meta_data["Partitions"][subarray_number]

        # get the input file global attributes
        in_glob_atts = {k: self._in_nc_ds.getncattr(k) for k in self._in_nc_ds.ncattrs()}

        # get the input variable from the in_nc_ds
        in_var = self._in_nc_ds[self._in_var_name]
        # get the input variable attributes
        in_var_atts = {k: in_var.getncattr(k) for k in in_var.ncattrs()}

        # get the local filename (in the cache)
        out_name = os.path.join(path, os.path.basename(sa["subarray"]["file"]))
        # create the local file
        out_nc_ds = netCDF4.Dataset(out_name, 'w', format=format, clobber=True)   # we always write as NETCDF4 and overwrite

        # write the global attributes
        out_nc_ds.setncatts(in_glob_atts)

        # need to cope with rotated grids, these should be stored in the "grid_mapping" variable
        if "grid_mapping" in in_var_atts:
            grid_map_var_name = in_var_atts["grid_mapping"]
            grid_map_in_var = self._in_nc_ds.variables[grid_map_var_name]
            # copy the attributes
            grid_map_var_atts = {k: grid_map_in_var.getncattr(k) for k in grid_map_in_var.ncattrs()}
            # write out the grid map file
            grid_map_out_var = out_nc_ds.createVariable(grid_map_in_var.name, grid_map_in_var.dtype)
            grid_map_out_var.setncatts(grid_map_var_atts)

        # get the location and shape of the array from the CFA metadata
        location = numpy.array(sa["location"])
        shape = numpy.array(sa["subarray"]["shape"])

        # copy the variable's dimensions, but now we subset them based on the location and shape
        for d in range(0, len(in_var.dimensions)):
            # get the input dimension from the file
            in_dim = self._in_nc_ds.dimensions[in_var.dimensions[d]]
            in_dim_var = self._in_nc_ds.variables[in_var.dimensions[d]]
            # create the dimension - now subset on the shape of this axis
            out_dim = out_nc_ds.createDimension(in_dim.name, shape[d])
            # create the dimension variable
            out_dim_var = out_nc_ds.createVariable(in_dim.name, in_dim_var.dtype, (in_dim.name,))
            # copy the dimension variable metadata
            out_dim_var.setncatts({k: in_dim_var.getncattr(k) for k in in_dim_var.ncattrs()})
            # copy the dimension variable data
            out_dim_var[:] = in_dim_var[location[d,0]:location[d,1]+1]

        # create the output variable
        out_var = out_nc_ds.createVariable(in_var.name, in_var.dtype, in_var.dimensions)
        # copy the input variable attributes to the output variable
        out_var.setncatts(in_var_atts)

        # now copy the data - we need a subset, using the start and end slice
        out_slice = [slice(location[d,0],location[d,1]+1) for d in range(0, len(shape))]
        out_var[:] = in_var[out_slice]

        out_nc_ds.close()

        # return the sub_dir/filename - this can be appended to base to upload to S3 or get_cache_path to get the
        # path on the local filesystem
        return sa["subarray"]["file"]
