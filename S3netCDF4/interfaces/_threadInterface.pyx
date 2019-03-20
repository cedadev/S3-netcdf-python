"""
   Class containing the an interface for reading / writing / uploading the CFA-netCDF files, to either disk or S3.
   This interface uses the python threading package to parallelise the reading, writing and uploading of the
   CFA-netCDF files to disk or S3.
"""

from _baseInterface import _baseInterface
from .._s3netCDFIO import get_netCDF_file_details, put_netCDF_file
from .._CFAFunctions import get_source_target_slices
import netCDF4._netCDF4 as netCDF4
from Queue import Queue

import os
import threading
import time

def _upload_task(subarray_file, fname):
    """Small task to upload and delete the cached file"""
    put_netCDF_file(subarray_file)
    os.remove(fname)


class _threadInterface(_baseInterface):
    """Class to represent a class for reading / writing / uploading netCDF files to disk or S3.
       This class is inherited from _baseInterface.
       This class uses the python threading module to read / write / upload the data
    """

    def name():
        """Return the name of the interface for debugging purposes"""
        return "threadInterface"

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

            # now do one subset
            threads = []
            return_queue = Queue()
            for i in range(0, el-sl):
                t = threading.Thread(target=self._read_partition, args=(i, return_queue, partitions[sl+i], elem_slices,))
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
                time.sleep(0.01)

            # collect the data
            while not return_queue.empty():
                ret_vals = return_queue.get()
                nc_var = ret_vals[0]
                py_source_slice = ret_vals[1]
                py_target_slice = ret_vals[2]
                self._data[py_target_slice] = nc_var[py_source_slice]


    def write(self, partitions, elem_slices):
        """Write (in parallel) the list of partitions which are in the subgroup determined by S3Variable.__setitem__"""
        # form the partitions into subsets based on the number of write threads
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
                t = threading.Thread(target=self._write_partition, args=(part, elem_slices))
                threads.append(t)
                t.start()

            # wait for threads to complete
            n_alive = 1
            while n_alive > 0:
                n_alive = 0
                for t in threads:
                    if t.isAlive():
                        n_alive+=1
                time.sleep(0.1)


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
                    t = threading.Thread(target=_upload_task, args=(upload_files[i], local_files[i],))
                    threads.append(t)
                    t.start()

            # wait for threads to complete
            n_alive = 1
            while n_alive > 0:
                n_alive = 0
                for t in threads:
                    if t.isAlive():
                        n_alive+=1
                time.sleep(0.1)
