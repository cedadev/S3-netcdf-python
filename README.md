s3-netCDF4-python
=================

An extension module to netCDF4-python to enable reading and writing netCDF files and
CFA-netcdf files from / to object stores which have an S3 HTTP interface.

Current version as of 20170717
==============================

Reads a netCDF file from an object store either directly into memory, for a small file, or
to a cache in the user file space if the file is large.

Quick setup guide
=================

1. Create a virtual-env for python
2. Pip install the following packages into your virtual-env 
      a. minio
      b. netCDF4
      c. psutil
      d. Cython
3. git clone https://github.com/cedadev/S3-netcdf-python.git
4. copy config/.s3nc4.json to $HOME/.s3nc4.json
5. Change the path in test/test_Dataset.py:
`sys.path.append(os.path.expanduser("~/Coding/S3-netcdf-python/"))`
to where you have cloned the git repo.
6. Run the test in test/test_Dataset.py
