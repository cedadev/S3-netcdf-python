"""
   Class containing the base interface for reading / writing / uploading the netCDF files, to either disk or S3.
   These can be overloaded to provide the capability to use different methods of parallelisation.
   This "base" interface implements a simple serial way of reading, writing and uploading.
"""

from .._s3netCDFIO import get_netCDF_file_details, put_netCDF_file
import netCDF4._netCDF4 as netCDF4
import os
from .._CFAFunctions import get_source_target_slices
import numpy
from Queue import Queue

class _baseInterface(object):
    """Class to represent a base for reading / writing / uploading netCDF files to disk or S3.
       Each class contains three functions:
          read
          write
          upload
       Each function takes a list of CFAPartitions that have been subset to the number of threads in the
       read_threads or write_threads setting in ~/.s3nc4.json
       The read / write functions also take elem_slices: the user supplied slices to the master array, which have been
         filled to contain absolute values for all boundaries (rather than -1, None, etc.)
    """

    def _read_partition(self, thread_number, return_queue, part, elem_slices):
        """Read a single partition.  This is overloaded so we can have local data for each thread."""
        # get the filename, either in the cache for s3 files or on disk for POSIX
        file_details = get_netCDF_file_details(part.subarray.file, 'r')

        # open the file as a dataset - see if it is first streamed to memory
        if file_details.memory != "":
            # add the slot number to the filename to avoid the threads from reading / writing the same file
            file_details.filename += "_" + str(thread_number)
            # we have to first create the dummy file (name held in file_details.filename) - check it exists before creating it
            if not os.path.exists(file_details.filename):
                temp_file = netCDF4.Dataset(file_details.filename, 'w', format=file_details.format).close()
            # create the netCDF4 dataset from the data, using the temp_file
            nc_file = netCDF4.Dataset(file_details.filename, mode='r',
                                      diskless=True, persist=False, memory=file_details.memory)
        else:
            # not in memory but has been streamed to disk - persist in the cache
            nc_file = netCDF4.Dataset(file_details.filename, mode='r')

        # get the source and target slices - use the filled slices from above
        py_source_slice, py_target_slice = get_source_target_slices(part, elem_slices)
        # get the variable
        nc_var = nc_file.variables[part.subarray.ncvar]
        return_queue.put([nc_var, py_source_slice, py_target_slice])


    def _write_partition(self, part, elem_slices):
        """Write a single partition.  This should be used by subclasses."""
        ip = self._init_params # just a shorthand

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
                os.makedirs(dest_dir)

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
            var = ncfile.createVariable(self._nc_var.name, self._nc_var.datatype, self._cfa_var.pmdimensions,
                                        zlib = ip['zlib'], complevel = ip['complevel'], shuffle = ip['shuffle'],
                                        fletcher32 = ip['fletcher32'], contiguous = ip['contiguous'],
                                        chunksizes = ip['chunksizes'], endian = ip['endian'],
                                        least_significant_digit = ip['least_significant_digit'],
                                        fill_value = ip['fill_value'], chunk_cache = ip['chunk_cache'])
            # add the variable cfa_metadata
            if self._cfa_var.metadata:
                var.setncatts(self._cfa_var.metadata)

        # now copy the data in.  We have to decide where to copy this fragment of the data to (target)
        # and from where in the original data we want to copy it (source)
        # get the source and target slices - these are flipped in relation to __getitem__
        py_target_slice, py_source_slice = get_source_target_slices(part, elem_slices)
        # copy the data in
        var[py_target_slice] = self._data[py_source_slice]
        ncfile.close()
        return


    def name():
        """Return the name of the interface for debugging purposes"""
        return "baseInterface"


    def set_read_params(self, data, read_threads):
        """Set the required input papramenets to successfully read the CFA files"""
        self._data = data
        self._read_threads = read_threads


    def set_write_params(self, data, nc_var, cfa_var, cfa_file, write_threads, init_params):
        """Set the required input parameters to successfully write the CFA files"""
        self._data = data
        self._nc_var = nc_var
        self._cfa_var = cfa_var
        self._cfa_file = cfa_file
        self._write_threads = write_threads
        self._init_params = init_params


    def set_upload_params(self, file_details, cfa_variables, upload_threads):
        self._file_details = file_details
        self._cfa_variables = cfa_variables
        self._upload_threads = upload_threads


    def read(self, partitions, elem_slices):
        """Read (in serial) the list of partitions which are in a subgroup determined by S3Variable.__getitem__"""

        # Read all the partitions (serially)
        # We use return slots so that the multi-thread versions can read in the data
        # and then move it into memory afterwards
        for part in partitions:
            return_queue = Queue()
            self._read_partition(0, return_queue, part, elem_slices)
            # collect the data
            ret_vals = return_queue.get()
            nc_var = ret_vals[0]
            py_source_slice = ret_vals[1]
            py_target_slice = ret_vals[2]
            self._data[py_target_slice] = nc_var[py_source_slice]


    def write(self, partitions, elem_slices):
        """Write (in serial) the list of partitions which are in the subgroup determined by S3Variable.__setitem__"""

        # write all the paritions (serially)
        for part in partitions:
            self._write_partition(part, elem_slices)


    def upload(self):
        """Upload (in serial) the master array file and subarray files for the partitions."""
        # upload the master array file to s3
        put_netCDF_file(self._file_details.s3_uri)
        # remove cached file
        os.remove(self._file_details.filename)

        # get the base directory where all the subarray files are held
        base_dir = self._file_details.filename[:self._file_details.filename.rfind(".")]
        # filename is part of the subarray structure
        # s3netCDFIO will handle the putting to s3 object store
        # have to check that the file exists as when writing just parts of the file, it may not!
        # loop over the variables
        for v in self._cfa_variables:
            # loop over the partitions
            for p in self._cfa_variables[v]._cfa_var.partitions:
                fname = base_dir + "/" + os.path.basename(p.subarray.file)
                if os.path.exists(fname):
                    put_netCDF_file(p.subarray.file)
                    os.remove(fname)
