"""
S3 enabled version of netCDF4.
Allows reading and writing of netCDF files to object stores via AWS S3.

Requirements: minio, psutil, netCDF4, Cython

Author: Neil Massey
Date:   10/07/2017
"""

# This module inherits from the standard netCDF4 implementation
# import as UniData netCDF4 to avoid confusion with the S3 module
import netCDF4._netCDF4 as netCDF4
from _s3netCDFIO import get_netCDF_file_details, put_netCDF_file
from _s3Exceptions import *
from _CFAClasses import *
from _CFAFunctions import *
from _s3Client import s3ClientConfig

import os
from collections import OrderedDict

# these are class attributes that only exist at the python level (not in the netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_s3_private_atts = [\
 # member variables
 '_file_details', '_cfa_variables', '_s3_client_config',
]
netCDF4._private_atts.extend(_s3_private_atts)

class s3Dataset(netCDF4.Dataset):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member functions to allow the
       read and write of netCDF file to an object store accessed via an AWS S3 HTTP API.
    """

    def __init__(self, filename, mode='r', clobber=True, format='NETCDF4',
                 diskless=False, persist=False, keepweakref=False, memory=None,
                 **kwargs):
        """
        **`__init__(self, filename, mode="r", clobber=True, diskless=False,
           persist=False, keepweakref=False, format='NETCDF4')`**

        `S3netCDF4.Dataset` constructor
        See `netCDF4.Dataset` for full details of all the keywords
        """

        # we've passed all the details of detecting whether this is an S3 or POSIX file to the function
        # get_netCDFFilename(filename).  Diskless == always_stream

        # get the file details
        self._file_details = get_netCDF_file_details(filename, mode, diskless)
        self._cfa_variables = OrderedDict()

        # get the s3ClientConfig for paths to the cache and max file size
        self._s3_client_config = s3ClientConfig()

        # switch on the read / write / append mode
        if mode == 'r' or mode == 'a' or mode == 'r+':             # read
            # check whether the memory has been set from get_netCDF_file_details (i.e. the file is streamed to memory)
            if self._file_details.memory != "" or diskless:
                # we have to first create the dummy file (name held in file_details.memory) - check it exists before creating it
                if not os.path.exists(self._file_details.filename):
                    temp_file = netCDF4.Dataset(self._file_details.filename, 'w', format=self._file_details.format).close()
                # create the netCDF4 dataset from the data, using the temp_file
                netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=True, persist=False,
                                         keepweakref=keepweakref, memory=self._file_details.memory, **kwargs)
            else:
                # not in memory but has been streamed to disk
                netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=False, persist=persist,
                                         keepweakref=keepweakref, memory=None, **kwargs)

            # check if file is a CFA file, for standard netCDF files
            try:
                cfa = "CFA" in self.getncattr("Conventions")
            except:
                cfa = False

            if cfa:
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.parse(self)
                self._file_details.cfa_file.format = self._file_details.format
                # recreate the variables as Variables and attach the cfa data
                for v in self.variables:
                    if v in self._file_details.cfa_file.cfa_vars:
                        self._cfa_variables[v] = s3Variable(self.variables[v],
                                                            self._file_details.cfa_file,
                                                            self._file_details.cfa_file.cfa_vars[v],
                                                            {'s3_cache_location' : self._s3_client_config['cache_location']})
            else:
                self._file_details.cfa_file = None

        elif mode == 'w':           # write
            # check the format for writing - allow CFA4 in arguments and default to it as well
            # we DEFAULT to CFA4 for writing to S3 object stores so as to distribute files across objects
            if format == 'CFA4' or format == 'DEFAULT':
                self._file_details.format = 'NETCDF4'
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.format = self._file_details.format
            elif format == 'CFA3':
                self._file_details.format = 'NETCDF3_CLASSIC'
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.format = self._file_details.format
            else:
                self._file_details.cfa_file = None

            # for writing a file, all we have to do is check that the containing folder in the cache exists
            if self._file_details.filename != "":   # first check that it is not a diskless file
                cache_dir = os.path.dirname(self._file_details.filename)
                # create all the sub folders as well
                if not os.path.isdir(cache_dir):
                    os.makedirs(cache_dir)

            # if the file is diskless and an S3 file then we have to persist so that we can upload the file to S3
            if self._file_details.s3_uri != "" and diskless:
                persist = True
            netCDF4.Dataset.__init__(self, self._file_details.filename, mode=mode, clobber=clobber,
                                     format=self._file_details.format, diskless=diskless, persist=persist,
                                     keepweakref=keepweakref, memory=None, **kwargs)
        else:
            # no other modes are supported
            raise s3APIException("Mode " + mode + " not supported.")


    def __enter__(self):
        """Allows objects to be used with a `with` statement."""
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """Allows objects to be used with a `with` statement."""
        self.close()


    def getVariable(self, name):
        """Get an s3 / cfa variable or just a standard netCDF4 variable,
           depending on its type.
           For a CFA-netCDF file, the dimension variables are standard netCDF4.Variables,
             and the field variables are s3Variables.
           For a netCDF file (no CFA splitting), all variables are standard netCDF4.Variables
        """
        if name in self._cfa_variables:
            return self._cfa_variables[name]
        else:
            return self.variables[name]


    def getVariables(self):
        """Get a list of the variable names"""
        names = []
        for n in self._cfa_variables:
            names.append(n)
        for n in self.variables:
            names.append(n)
        return names

    def createDimension(self, dimname, size=None):
        """Overloaded version of createDimension that records the dimension info into a CFADim instance"""
        netCDF4.Dataset.createDimension(self, dimname, size)
        if self._file_details.cfa_file is not None:
            # add to the dimensions
            self._file_details.cfa_file.cfa_dims[dimname] = CFADim(dim_name=dimname, dim_len=size)


    def createVariable(self, varname, datatype, dimensions=(), zlib=False,
            complevel=4, shuffle=True, fletcher32=False, contiguous=False,
            chunksizes=None, endian='native', least_significant_digit=None,
            fill_value=None, chunk_cache=None):
        """Overloaded version of createVariable that has the following behaviour:
           For standard netCDF files (non CFA split) just pass through to the base method.
           For CF-netCDF files, create the variable with no dimensions, and create the
           required CFA metadata."""
        if self._file_details.cfa_file is None:
            var = netCDF4.Dataset.createVariable(self, varname, datatype, dimensions, zlib,
                    complevel, shuffle, fletcher32, contiguous,
                    chunksizes, endian, least_significant_digit,
                    fill_value, chunk_cache)
            return var
        else:
            # get the variable shape, so we can determine the partitioning scheme
            # (and whether there should even be a partitioning scheme)
            var_shape = []
            for d in dimensions:
                var_shape.append(self.dimensions[d].size)

            # is the variable name in the dimensions?
            if varname in self.dimensions or var_shape == []:
                # it is so create the Variable with dimensions
                var = netCDF4.Dataset.createVariable(self, varname, datatype, dimensions, zlib,
                        complevel, shuffle, fletcher32, contiguous,
                        chunksizes, endian, least_significant_digit,
                        fill_value, chunk_cache)
                return var
            else:
                # it is not so create a dimension free version
                var = netCDF4.Dataset.createVariable(self, varname, datatype, (), zlib,
                        complevel, shuffle, fletcher32, contiguous,
                        chunksizes, endian, least_significant_digit,
                        fill_value, chunk_cache)
                # get the base / root filename of the subarray files
                if self._file_details.s3_uri != "":
                    base_filename = self._file_details.s3_uri[:self._file_details.s3_uri.rfind(".")]
                else:
                    base_filename = self._file_details.filename[:self._file_details.filename.rfind(".")]

                # create the partitions, i.e. a list of CFAPartition, and get the partition shape
                # get the max file size from the s3ClientConfig
                pmshape, partitions = create_partitions(base_filename, self, dimensions,
                                                        varname, var_shape, var.dtype,
                                                        max_file_size=self._s3_client_config["max_object_size"],
                                                        format="netCDF")
                # create the CFAVariable here
                self._file_details.cfa_file.cfa_vars[varname] = CFAVariable(varname,
                                                                cf_role="cfa_variable", cfa_dimensions=list(dimensions),
                                                                pmdimensions=list(dimensions), pmshape=pmshape,
                                                                base="", partitions=partitions)
                # add the metadata to the variable
                cfa_var_meta = self._file_details.cfa_file.cfa_vars[varname].dict()
                for k in cfa_var_meta:
                    if k == "cfa_array":        # convert the cfa_array metadata to json
                        var.setncattr(k, json.dumps(cfa_var_meta[k]))
                    else:
                        var.setncattr(k, cfa_var_meta[k])

                # check whether we need to copy the dimension values and metadata into the variable
                for d in dimensions:
                    if len(self._file_details.cfa_file.cfa_dims[d].values) == 0:
                        # values
                        self._file_details.cfa_file.cfa_dims[d].values = np.array(self.variables[d][:])
                        # metadata
                        md = {k: self.variables[d].getncattr(k) for k in self.variables[d].ncattrs()}
                        self._file_details.cfa_file.cfa_dims[d].metadata = md


                # keep the calling parameters in a dictionary
                parameters = {'varname' : varname, 'datatype' : datatype, 'dimensions' : dimensions, 'zlib' : zlib,
                              'complevel' : complevel, 'shuffle' : shuffle, 'fletcher32' : fletcher32,
                              'contiguous' : contiguous, 'chunksizes' : chunksizes, 'endian' : endian,
                              'least_significant_digit' : least_significant_digit,
                              'fill_value' : fill_value, 'chunk_cache' : chunk_cache}

                # create the s3Variable which is a reimplementation of the netCDF4 variable
                self._cfa_variables[varname] = s3Variable(var, self._file_details.cfa_file,
                                                          self._file_details.cfa_file.cfa_vars[varname],
                                                          parameters)
                return self._cfa_variables[varname]


    def close(self):
        """Close the netCDF file.  If it is a S3 file and the mode is write then upload to the storage."""

        if (self._file_details.filemode == 'w' or
            self._file_details.filemode == "r+" or
            self._file_details.filemode == 'a'):

            # if it's a CFA file then add some metadata before closing the file
            if self._file_details.cfa_file:
                # check that the conventions are present
                try:
                    conv_attrs = self.getncattr("Conventions")
                    self.setncattr("Conventions", conv_attrs+" CFA-0.4")
                except:
                    self.setncattr("Conventions", "CFA-0.4")

            # close the netCDF file now - needed to finish writing to disk so the file can be uploaded
            netCDF4.Dataset.close(self)

            # if it's a CFA file then write out the master CFA file and the sub CF netCDF files
            if self._file_details.cfa_file:
                if self._file_details.s3_uri != "":
                    # upload to s3
                    put_netCDF_file(self._file_details.s3_uri)
                    # remove cached file
                    os.remove(self._file_details.filename)
                # get the base directory where all the subarray files are held
                base_dir = self._file_details.filename[:self._file_details.filename.rfind(".")]
                # loop over the variables
                for v in self._cfa_variables:
                    # loop over the partitions
                    for p in self._cfa_variables[v]._cfa_var.partitions:
                        # filename is part of the subarray structure
                        # s3netCDFIO will handle the putting to s3 object store
                        put_netCDF_file(p.subarray.file)
                        # delete the file if it's in the cache
                        fname = base_dir + "/" + os.path.basename(p.subarray.file)
                        if os.path.exists(fname):
                            os.remove(fname)

            # if it's not a CFA file then just upload it.
            else:
                put_netCDF_file(filename)
        else:
            netCDF4.Dataset.close(self)


class s3Variable(object):
    """
      Reimplement the UniData netCDF4 Variable class and override some key methods so as to enable CFA and S3 functionality
    """

    _private_atts = ["_cfa_var", "_nc_var", "_cfa_file", "_init_params"]

    def __init__(self, nc_var, cfa_file, cfa_var, init_params = {}):
        """Keep a reference to the nc_file, nc_var and cfa_var"""
        self._init_params = init_params
        self._nc_var  = nc_var
        self._cfa_var = cfa_var
        self._cfa_file = cfa_file

    """There now follows a long list of functions, matching the netCDF4.Variable interface.
       The only functions we need to override are __getitem__ and __setitem__, so as to
       use the CFA information in cfa_var.
       The other methods we need to pass through to the _nc_var member variable."""

    def __repr__(self):
        return unicode(self._nc_var).encode('utf-8')

    def __array__(self):
        return self._nc_var.__array__()

    def __unicode__(self):
        return self._nc_var.__unicode__()

    @property
    def name(self):
        return self._nc_var.name
    @name.setter
    def name(self, value):
        raise AttributeError("name cannot be altered")

    @property
    def datatype(self):
        return self._nc_var.datatype

    @property
    def shape(self):
        return self._shape()

    def _shape(self):
        # get the shape from the list of dimensions in the _cfa_var and the
        # size of the dimensions from the _cfa_file
        shp = []
        for cfa_dim in self._cfa_var.cfa_dimensions:
            d = self._cfa_file.cfa_dims[cfa_dim]
            if d.dim_len == -1:         # check for unlimited dimension
                shp.append(d.values.shape[0])
            else:
                shp.append(d.dim_len)
        return shp

    def _size(self):
        return np.prod(self._shape())

    def _dimensions(self):
        # get the dimensions from the _cfa_var
        return self._cfa_var.cfa_dimensions

    def group(self):
        return self._nc_var.group()

    def ncattrs(self):
        return self._nc_var.ncattrs()

    def setncattr(self, name, value):
        # copy to the cfa_var
        self._cfa_var.metadata[name] = value
        # copy to the netCDF var
        self._nc_var.setncattr(name, value)

    def setncattr_string(self, name, value):
        # copy to the cfa_var
        self._cfa_var.metadata[name] = value
        # copy to the netCDF var
        self._nc_var.setncattr(name, value)

    def setncatts(self, attdict):
        # copy to the cfa_var
        self._cfa_var.metadata = attdict
        # copy to the netCDF var
        self._nc_var.setncatts(attdict)

    def getncattr(self, name, encoding='utf-8'):
        return self._nc_var.getncattr(name, encoding)

    def delncattr(self, name):
        self._nc_var.delncattr(name)

    def filters(self):
        return self._nc_var.filters()

    def endian(self):
        return self._nc_var.endian()

    def chunking(self):
        return self._nc_var.chunking()

    def get_var_chunk_cache(self):
        return self._nc_var.get_var_chunk_cache()

    def set_var_chunk_cache(self, size=None, nelems=None, preemption=None):
        self._nc_var.set_var_chunk_cache(size, nelems, preemption)

    def __delattr__(self, name):
        self._nc_var.__delattr__(name)

    def __setattr__(self, name, value):
        if name in s3Variable._private_atts:
            self.__dict__[name] = value
        elif name == "dimensions":
            raise AttributeError("dimensions cannot be altered")
        elif name == "shape":
            raise AttributeError("shape cannot be altered")
        else:
            self._nc_var.__setattr(name, value)

    def __getattr__(self, name):
        # check whether it is _nc_var or _cfa_var
        if name in s3Variable._private_atts:
            return self.__dict__[name]
        elif name == "dimensions":
            return tuple(self._dimensions())
        elif name == "shape":
            return tuple(self._shape())
        elif name == "size":
            return self._size()
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        elif name.startswith('__') and name.endswith('__'):
            # if __dict__ requested, return a dict with netCDF attributes.
            if name == '__dict__':
                names = self._nc_var.ncattrs()
                values = []
                for name in names:
                    values.append(_get_att(self._nc_var.group(), self._nc_var._varid, name))
                return OrderedDict(zip(names, values))
            else:
                raise AttributeError
        elif name in netCDF4._private_atts:
            return self._nc_var.__dict__[name]
        else:
            return self._nc_var.getncattr(name)

    def renameAttribute(self, oldname, newname):
        self._nc_var.renameAttribute(oldname, newname)

    def __len__(self):
        return self._nc_var.__len__()

    def assignValue(self, val):
        self._nc_var.assignValue(val)

    def getValue(self):
        return self._nc_var.getValue()

    def set_auto_chartostring(self, chartostring):
        self._nc_var.set_auto_chartostring(chartostring)

    def set_auto_maskandscale(self, maskandscale):
        self._nc_var.set_auto_maskandscale(maskandscale)

    def set_auto_scale(self, scale):
        self._nc_var.set_auto_scale(scale)

    def set_auto_mask(self, mask):
        self._nc_var.set_auto_mask(mask)

    def __reduce__(self):
        return self._nc_var.__reduce__()

    def __getitem__(self, elem):
        """Overload the [] operator for getting values from the netCDF variable"""
        # get the filled slices
        elem_slices = fill_slices(self.shape, elem)
        # get the partitions from the slice - created the subset of partitions
        subset_parts = []
        # determine which partitions overlap with the indices
        for p in self._cfa_var.partitions:
            if partition_overlaps(p, elem_slices):
                subset_parts.append(p)

        # create a memory mapped array in the cache for the output array
        mmap_name = self._init_params['s3_cache_location'] + "/" + os.path.basename(subset_parts[0].subarray.file) + "_{}".format(int(numpy.random.uniform(0,1e8)))

        # create the target shape from the elem slices
        subset_shape = []
        for s in elem_slices:
            subset_shape.append((s.stop-s.start+1)/s.step)
        # create the target memory mapped array
        mmap_arr = numpy.memmap(mmap_name, dtype=self._nc_var.dtype, mode='w+', shape=tuple(subset_shape))
        # loop over the subset partitions
        for part in subset_parts:
            # get the filename, either in the cache for s3 files or on disk for POSIX
            file_details = get_netCDF_file_details(part.subarray.file, 'r')

            # open the file as a dataset - see if it is first streamed to memory
            if file_details.memory != "":
                # we have to first create the dummy file (name held in file_details.memory) - check it exists before creating it
                if not os.path.exists(file_details.filename):
                    temp_file = netCDF4.Dataset(file_details.filename, 'w', format=file_details.format).close()
                # create the netCDF4 dataset from the data, using the temp_file
                nc_file = netCDF4.Dataset(file_details.filename, mode='r',
                                          diskless=True, persist=False, memory=file_details.memory)
                # get the variable
                nc_var = nc_file.variables[part.subarray.ncvar]
                # create the default source slice: this is the shape of the subarray
                source_slice = []
                for sl in part.subarray.shape:
                    source_slice.append(CFASlice(0, sl, 1))
                # create the target slice from the location and the passed in elements
                # create the default slice first - we will modify this
                target_slice = []
                for pl in part.location:
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
                # place the data in the memory mapped array
                mmap_arr[py_target_slice] = nc_var[py_source_slice]
            else:
                # not in memory but has been streamed to disk - persist in the cache
                nc_file = netCDF4.Dataset(file_details.filename, mode='r')
        return mmap_arr


    def __setitem__(self, elem, data):
        """Overload the [] operator for setting values for the netCDF variable"""
        # get the filled slices
        subset_slices = fill_slices(self.shape, elem)
        # get the partitions from the slice - created the subset of partitions
        subset_parts = []
        # determine which partitions overlap with the indices
        for p in self._cfa_var.partitions:
            if partition_overlaps(p, subset_slices):
                subset_parts.append(p)

        # loop over the subset partitions
        for part in subset_parts:
            # get the filename, either in the cache for s3 files or on disk for POSIX
            file_details = get_netCDF_file_details(part.subarray.file, 'w')
            # check if the file has already been created
            if os.path.exists(file_details.filename):
                # open the file in append mode
                ncfile = netCDF4.Dataset(file_details.filename, mode='r+')
                var = ncfile.variables[self._nc_var.name]
            else:
                # first create the destination directory, if it doesn't exist
                dest_dir = os.path.dirname(file_details.filename)
                if not os.path.isdir(dest_dir):
                    os.makedirs(os.path.dirname(dest_dir))

                # create the netCDF file
                ncfile = netCDF4.Dataset(file_details.filename, 'w', format=self._cfa_file.format)

                # create the dimensions
                for d in range(0, len(self._cfa_var.pmdimensions)):
                    # get the dimension details from the _cfa_var
                    dim_name = self._cfa_var.pmdimensions[d]
                    cfa_dim = self._cfa_file.cfa_dims[dim_name]

                    # the dimension lengths come from the subarray
                    dim_size = part.subarray.shape[d]
                    # create the dimension
                    if cfa_dim.dim_len == -1:       # allow for unlimited dimension
                        ncfile.createDimension(cfa_dim.dim_name, None)
                    else:
                        ncfile.createDimension(cfa_dim.dim_name, dim_size)

                    # create the dimension variable
                    dim_var = ncfile.createVariable(cfa_dim.dim_name, cfa_dim.values.dtype, (cfa_dim.dim_name,))
                    # add the metadata as attributes
                    dim_var.setncatts(cfa_dim.metadata)
                    # add the values for the dimension - we need to build the indices
                    dim_var[:] = cfa_dim.values[part.location[d,0]:part.location[d,1]+1]

                # create the variable - match the parameters to those used in the createVariable function in s3Dataset
                ip = self._init_params # just a shorthand
                var = ncfile.createVariable(self._nc_var.name, self._nc_var.datatype, self._cfa_var.pmdimensions,
                                            zlib = ip['zlib'], complevel = ip['complevel'], shuffle = ip['shuffle'],
                                            fletcher32 = ip['fletcher32'], contiguous = ip['contiguous'],
                                            chunksizes = ip['chunksizes'], endian = ip['endian'],
                                            least_significant_digit = ip['least_significant_digit'],
                                            fill_value = ip['fill_value'], chunk_cache = ip['chunk_cache'])
                # add the variable cfa_metadata
                var.setncatts(self._cfa_var.metadata)

            # now copy the data in.  We have to decide where to copy this fragment of the data to (target)
            # and from where in the original data we want to copy it (source)

            # create the slices for the source
            source_slices = []
            for d in range(0, len(self._cfa_var.pmdimensions)):
                source_slices.append(slice(part.location[d,0], part.location[d,1]+1, 1))

            # copy the data in - still have to deal with the slices
            var[:] = data[source_slices]

            ncfile.close()
