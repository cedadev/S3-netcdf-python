"""
   Class containing the an interface for reading / writing / uploading the CFA-netCDF files, to either disk or S3.
   This interface uses the python multiprocessing package to parallelise the reading, writing and uploading of the
   CFA-netCDF files to disk or S3.
"""

from _baseInterface import _baseInterface
from .._s3netCDFIO import get_netCDF_file_details, put_netCDF_file
from .._CFAFunctions import get_source_target_slices
import netCDF4._netCDF4 as netCDF4

import os
import multiprocessing as mp
import numpy as np
import operator
import ctypes

def _upload_task(subarray_file, fname):
    """Small task to upload and delete the cached file"""
    put_netCDF_file(subarray_file)
    os.remove(fname)

def _read_partition(thread_number, part, py_source_slice, py_target_slice, data):
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

    # get the variable
    nc_var = nc_file.variables[part.subarray.ncvar]
    data[:] = nc_var[py_source_slice].flatten()


class _processInterface(_baseInterface):
    """Class to represent a class for reading / writing / uploading netCDF files to disk or S3.
       This class is inherited from _baseInterface.
       This class uses the python multiprocessing module to read / write / upload the data
    """

    def name():
        """Return the name of the interface for debugging purposes"""
        return "processInterface"

    def read(self, partitions, elem_slices):
        """Read (in parallel) the list of partitions which are in a subgroup determined by S3Variable.__getitem__"""
        # form the subset parts into subgroups based on the number of read threads
        nrt = self._read_threads
        nsp = len(partitions)
        n_loops = int(0.99999 + float(nsp) / nrt)
        # loop over the subset partitions
        for n in range(0, n_loops):
            # get the start and end of the subset parts
            sl = n * nrt
            el = sl + nrt
            # check for over the end of the array
            if el > nsp:
                el = nsp

            # keep track of the threads
            threads = []
            # create the shared data
            shared_arrays = []
            # create the py_source, py_target_slices
            slices_store = []
            shape_store = []
            # now do one subset
            for i in range(0, el-sl):
                # get the source and target slices - use the filled slices from above
                py_source_slice, py_target_slice = get_source_target_slices(partitions[sl+i], elem_slices)
                slices_store.append(py_target_slice)
                # get the size
                size = 1
                shape = []
                for cs in py_source_slice:
                    A = (cs.stop - cs.start) / cs.step
                    size *= A
                    shape.append(A)
                data = mp.Array(ctypes.c_float, size, lock=False)
                shared_arrays.append(data)
                # create the process
                shape_store.append(shape)
                t = mp.Process(target=_read_partition, args=(i, partitions[sl+i], py_source_slice, py_target_slice, data))
                threads.append(t)
                t.start()
            # wait for threads to finish
            n_alive = 1
            while n_alive > 0:
                n_alive = 0
                for i in range(0, len(threads)):
                    t = threads[i]
                    if t.is_alive():
                        n_alive+=1
                    else:
                        # now move the data from the temporary arrays into the large memmapped array
                        self._data[slices_store[i]] = np.frombuffer(shared_arrays[i], 'f').reshape(shape_store[i])


    def write(self, partitions, elem_slices):
        """Write (in parallel) the list of partitions which are in the subgroup determined by S3Variable.__setitem__"""
        # form the partitionss into subsets based on the number of write threads
        nsp = len(partitions)
        nwt = self._write_threads
        n_loops = int(0.99999 + float(nsp) / nwt)

        # loop over the number of loops partitions
        for n in range(0, n_loops):
            # get the start and end of the subset
            sl = n * nwt
            el = sl + nwt
            # check for over the end of the array
            if el > nsp:
                el = nsp

            # loop over the subset partitions
            threads = []
            for part in partitions[sl:el]:
                t = mp.Process(target=self._write_partition, args=(part, elem_slices))
                threads.append(t)
                t.start()

            # wait for threads to complete
            n_alive = 1
            while n_alive > 0:
                n_alive = 0
                for t in threads:
                    if t.is_alive():
                        n_alive+=1
                    else:
                        t.join()


    def upload(self):
        """Upload (in parallel) the master array file and subarray files for the partitions."""
        # upload the master array file to s3
        put_netCDF_file(self._file_details.s3_uri)
        # remove cached file
        os.remove(self._file_details.filename)

        # get the base directory where all the subarray files are held
        base_dir = self._file_details.filename[:self._file_details.filename.rfind(".")]

        # get all the filenames for uploading from the _cfa_variables partitions
        local_files = []
        upload_files = []
        for v in self._cfa_variables:
            # loop over the partitions
            for p in self._cfa_variables[v]._cfa_var.partitions:
                fname = base_dir + "/" + os.path.basename(p.subarray.file)
                local_files.append(fname)
                upload_files.append(p.subarray.file)

        # calculate how many loops we will need, based on the number of upload threads
        nut = self._upload_threads
        nsp = len(upload_files)
        n_loops = int(0.99999 + float(nsp) / nut)

        # loop over the required number
        for n in range(0, n_loops):
            # get the start and end of the subset of files
            sl = n * nut
            el = sl + nut
            # check for over the end of the list
            if el > nsp:
                el = nsp

            # loop over the subset of files and upload if present
            threads = []
            for i in range(sl, el):
                if os.path.exists(fname):
                    t = mp.Process(target=_upload_task, args=(upload_files[i], local_files[i],))
                    threads.append(t)
                    t.start()

            # wait for threads to complete
            n_alive = 1
            while n_alive > 0:
                n_alive = 0
                for t in threads:
                    if t.is_alive():
                        n_alive+=1
                    else:
                        t.join()
