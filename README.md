# S3netCDF4
An extension package to netCDF4-python to enable reading and writing netCDF files and CFA-netcdf files from / to object stores which have a S3 HTTP interface, to disk or to OPeNDAP.

# Contents
* [Requirements](#requirements)
* [Configuration](#configuration)
* [Aliases](#aliases)
* [Caching](#caching)
* [Writing files](#writing-files)
  * [CFA-netCDF files](#cfa-netcdf-files)
  * [Creating dimensions and variables](#creating-dimensions-and-variables)
  * [Filenames and file hierarchy of CFA files](#filenames-and-file-hierarchy-of-cfa-files)
  * [Writing field data](#writing-field-data)
  * [File splitting algorithm](#file-splitting-algorithm)

# Requirements
(These are fulfilled by a pip installation, so it is not necessary to install them if you are installing the package via pip, as below.)
* numpy
* cython
* netcdf4
* minio
* psutil

[[Top]](#contents)

# Installation
1. Create a virtual environment:

  `virtualenv /path/to/venv`

2. Activate the virtual environment:

  `source /path/to/venv/bin/activate`

3. Install the S3netCDF4 Library:

  `pip install -e ../S3netCDF4`

4. Copy the configuration template file from `config/.s3nc4.json.template` to `~/.s3nc4.json` and fill in the values for the variables.  See the section "Configuration" below.

5. Run a test to ensure the package has installed correctly:

  `python test/test_netCDF4.py`

[[Top]](#contents)

# Configuration
S3netCDF4 relies on a configuration file to resolve endpoints for the S3 services, and to control various aspects of the way the package operates.  This config file is a JSON file and is located in the user's home directory:

`~/.s3nc4.json`

In the git repository a templatised example of this configuration file is provided:

`config/.s3nc4.json.template`

This can be copied to the user's home directory, and the template renamed to `~/.s3nc4.json`.  The variables in the template should then be filled in.  This file is a [jinja2](http://jinja.pocoo.org/docs/2.10/) template of a JSON file, and so can be used within an [ansible](https://www.ansible.com/) deployment.  Each entry in the file has a key:value pair.  An example of the file is given below:

    {
      "version": "8",
      "hosts":
      {
        "minio":
        {
          "alias": "s3://minio",
          "url": "150.246.130.8:9000",
          "accessKey": "WTL17W3P2K3C7IYRX4W9",
          "secretKey": "VUcT86fJFF0XTPtcrsnjUnvtN7Wj1N3cb9mALRZ9",
          "api": "S3v4"
        },
      },
      "cache_location": "/Users/dhk63261/cache",
      "max_cache_size": "128MB",
      "max_object_size_for_memory": "128MB",
      "max_object_size": "1MB",
      "read_threads" : 8,
      "write_threads" : 8
    }

* `version` indicates which version of the configuration file this is.
* `hosts` contains a list of named hosts and their respective configuration details.
  * `minio` contains the definition of a single host called minio.  For each host a number of configuration details need to be supplied:
    * `alias` the alias for the S3 server.  See the [Aliases](#aliases) section.
    * `url` the DNS resolvable URL for the S3 server, including the port number.
    * `accessKey` the user's access key for the S3 endpoint.
    * `secretKey` the user's secret key / password for the S3 endpoint.
    * `api` the api used to access the S3 endpoint.
* `cache_location` contains the location of the local disk cache for storing files that have been downloaded from the S3 object store.  See [Caching](#caching) section below.
* `max_cache_size` the maximum amount of disk space dedicated to the local disk cache.
* `max_object_size_for_memory` S3netCDF4 will try to stream an object into memory, rather than caching it to disk.  This variable controls the maximum size an object can be to be streamed into memory.  S3netCDF4 will also query the available memory and cache to disk if an object is bigger than the available memory.
* `max_object_size` this controls the maximum size the sub-array objects can be when writing CFA files.  See [Writing files](#writing_files) section.
* `read_threads` controls the number of parallel threads that will be used when reading objects from the S3 object store.
* `write_threads` controls the number of parallel threads that will be user when writing objects to the S3 object store.

[[Top]](#contents)

## Aliases
To enable S3netCDF4 to write to disk, OPeNDAP and S3 object store, aliases are used to identify S3 servers.  They provide an easy to remember (and type) shorthand for the user so that they don't have to use the DNS resolved URL and port number for each S3 object access.  When creating a netCDF4 `s3Dataset` object, either to read or write, the user supplies a filename.  To indicate that the file should be written to / read from a S3 object store, the string must start with `s3://`.  After this must follow the aliased server name, as defined in the config file above.  After this aliased server name a bucket name will follow, for example to read a netCDF file called `test2.nc` from the `test` bucket on the `s3://minio` server, the user would use this code:

    from S3netCDF4 import s3Dataset
    test_dataset = s3Dataset("s3://minio/test/test2.nc", "r")

On creation of the `s3Dataset` object, the S3netCDF4 package reads the filename, determines that the filename starts with `s3://`, reads the next part of the string up to the next `/` (which equates to `minio` in this cases) and searches through the aliases defined in the `~/.s3nc4.json` file to find a matching alias.  If one is not found it will return an error message, if it is found then it will open a connection to that S3 server, using the `url`, `accessKey` and `secretKey` defined for that server.  It is over this connection that all the data transfers for this `s3Dataset` take place.

[[Top]](#contents)

## Caching
If the user requests to access an object that is larger than either the host machines physical memory, or the `max_object_size_for_memory` setting in `~/.s3nc4.json` then S3netCDF4 will download the object to a locally cached file, in the `cache_location` root directory.  Otherwise, S3netCDF4 will stream the object into memory.  This behaviour can be overriden by specifying two keywords to the `s3Dataset` method:

* `persist=True` this will always download the object to the local cache, no matter what its size is.
* `diskless=True` this will always stream the object into memory, no matter its size, e.g:


    from S3netCDF4 import s3Dataset
    test_dataset = s3Dataset("s3://minio/test/test2.nc", "r", diskless=True)

*Currently cache cleanup is left to the user, but a cache management routine will be implemented in future versions.*

[[Top]](#contents)

## Writing files
S3netCDF4 has the ability to write netCDF3, netCDF4, CFA-netCDF3 and CFA-netCDF4 files to a POSIX filesystem, Amazon S3 object storage or OPeNDAP.  Files are created in the same way as the standard netCDF4-python package, by creating a `Dataset` object.  However, the parameters to the `Dataset` constructor can vary in two ways:

1. The `filename` can be an S3 endpoint, i.e. it starts with `s3://`
2. In addition to the formats supported by netCDF4-python The `format` keyword can also, in addition to the formats permitted by netCDF4-python, be `CFA3`, to create a CFA-netCDF3 dataset, or `CFA4`, to create a CFA-netCDF4 dataset.

*Example 1: Create a netCDF4 file in the filesystem*

    from S3netCDF4 import s3Dataset as Dataset
    test_dataset = Dataset("/Users/neil/test_dataset_nc4.nc", 'w', format='NETCDF4')


*Example 2: Create a CFA-netCDF4 file in the filesystem*

    from S3netCDF4 import s3Dataset as Dataset
    cfa_dataset = Dataset("/Users/neil/test_dataset_cfa4.nc", 'w', format='CFA4')


*Example 3: Create a CFA-netCDF3 file on S3 storage*

    from S3netCDF4 import s3Dataset as Dataset
    cfa_dataset = Dataset("s3://minio/test_bucket/test_dataset_s3_cfa3.nc", 'w', format='CFA3')

[[Top]](#contents)

### CFA-netCDF files
Choosing `format="CFA3"` or `format="CFA4"` when creating a file creates a CFA-compliant netCDF file.  This consists of a **master-array** file and a number of **sub-array** files.

The **master-array** file contains:

* the dimension definitions
* dimension variables
* scalar variable definitions: variable definitions without reference to the domain it spans
* variable metadata
* global metadata
* It does not contain any field data, but it *does* contain data for the dimension variables, and therefore the domain of each variable.  
* The **master-array** file may contain a single field variable or multiple field variables.  

The **sub-array** files contain a subdomain of a single variable in the **master-array**.  They contain:

* the dimension definitions for the subdomain
* the dimension variables for the subdomain
* a single variable definition, complete with reference to the dimensions
* metadata for the variable

The *variable metadata* in each variable in the **master-array** file contains a **partition matrix**.  The **partition matrix** contains information on how to reconstruct the **master-array** variables from the associated **sub-arrays** and, therefore, also contains the necessary information to read or write slices of the **master-array** variables.

The **partition matrix** contains:

* The dimensions in the netCDF file that the partition matrix acts over (e.g. `["time", "latitude", "longitude"`)
* The shape of the partition matrix (e.g. `[4,2,2]`)
* A list of partitions

Each **partition** in the **partition matrix** contains:

* An index for the partition into the partition matrix - a list the length of the number of dimensions for the variable (e.g `[3, 1, 0]`)
* The location of the partition in the **master-array** - a list (the length of the number of dimensions) of pairs, each pair giving the range of indices in the **master-array** for that dimension (e.g. `[[0, 10], [20, 40], [0, 45]]`)
* A definition of the **sub-array** which contains:
    * The path or URI of the file containing the **sub-array**.  This may be on the filesystem, an OPeNDAP file or an S3 URI.
    * The name of the netCDF variable in the **sub-array** file
    * The format of the file (always `netCDF` for S3netCDF4)
    * The shape of the variable - i.e. the length of the subdomain in each dimension

For more information see the [CFA conventions 0.4 website](http://www.met.reading.ac.uk/~david/cfa/0.4/).
There is also a useful synopsis in the header of the \_CFAClasses.pyx file in the S3netCDF4 source code.

*Note that indices in the partition matrix are indexed from zero, but the indices are inclusive for the location of the partition in the master-array.  This is different from Python where the indices are non-inclusive.*

[[Top]](#contents)

### Creating dimensions and variables
Creating dimensions and variables in the netCDF or CFA-netCDF4 dataset follows the same method as creating variables in the standard netCDF4-python library, e.g.:

    from S3netCDF4 import s3Dataset as Dataset
    cfa_dataset = Dataset("s3://minio/test_bucket/test_dataset_s3_cfa3.nc", 'w', format='CFA3')

    timed = cfa_dataset.createDimension("time", None)
    times = cfa_dataset.createVariable("time", "f4", ("time",))

When creating variables, a number of different workflows for writing the files occur.  Which workflow is taken depends on the combination of the filename path (`S3`, filesystem or OPeNDAP) and format (`CFA3` and `CFA4` or `NETCDF4` and `NETCDF3_CLASSIC`).  These workflows can be summarised by:

* `format=NETCDF4` or `format=NETCDF3_CLASSIC`.  These two options will create a standard netCDF file.
  * If the filename contains `s3://` then the netCDF file will be created in the local cache and uploaded (PUT) to the S3 filesystem when `Dataset.close()` is called.
  * If the filename does not contain `s3://` then the netCDF file will be written out to the filesystem or OPeNDAP, with the behaviour following the standard netCDF4-python library.

* `format=CFA3` or `format=CFA4`.  These two options will create a CFA-netCDF file.
  * At first only the **master-array** file is written to.  The **sub-array** files are written to when data is written to the **master-array** variable.
  * When the variable is created, the dimensions are supplied and this enables the **partition matrix** metadata to be generated:
    * The [file splitting algorithm](#file-splitting-algorithm) determines how to split the variable into the **sub-arrays**, creates the **partition matrix** shape and builds the list of **partitions**
    * The location in the **master-array** for each **sub-array** (and its shape) is determined by the file splitting algorithm
    * The filenames for each **sub-array** file are generated
  * The **partition matrix** metadata is written to the **master-array**
  * If the filename contains `s3://` then the **master-array** and **sub-array** files are written to the local cache and uploaded to the S3 storage when `Dataset.close()` is called on the **master-array** file
  * If the filename does not contain `s3://` then the **master-array** file is written to the filesystem immediately and the **sub-array** files are written to the filesystem when data is written to the **master-array** variable

### Filenames and file hierarchy of CFA files
As noted above, CFA files actually consist of a single **master-array** file and many **sub-array** files.  These **subarray-files** are referred to by their filepath or URI in the partition matrix.  To easily associate the **sub-array** files with the **master-array** file, a naming convention and file structure is used:

* The [CFA conventions](http://www.met.reading.ac.uk/~david/cfa/0.4/) dictate that the file extension for a CFA-netCDF file should be `.nca`
* A directory is created in the same directory / same root URI as the **master-array** file.  This directory has the same name **master-array** file without the `.nca` extension
* In this directory all of the **sub-array** files are contained.  These subarray files follow the naming convention:

    `<master-array-file-name>_<variable-name>_[<partition-number>].nc`

Example for the **master-array** file `a7tzga.pdl4feb.nca`:

    ├── a7tzga.pdl4feb.nca
    ├── a7tzga.pdl4feb
    │   ├── a7tzga.pdl4feb_field16_[0].nc
    │   ├── a7tzga.pdl4feb_field16_[1].nc
    │   ├── a7tzga.pdl4feb_field186_[0].nc
    │   ├── a7tzga.pdl4feb_field186_[1].nc
    │   ├── a7tzga.pdl4feb_field1_[0].nc
    │   ├── a7tzga.pdl4feb_field1_[1].nc
    │   ├── a7tzga.pdl4feb_field1_[2].nc
    │   ├── a7tzga.pdl4feb_field1_[3].nc

On an S3 storage system, the **master-array** directory will form part of the *prefix* for the **sub-array** objects, as directories do not exist, in a literal sense, on S3 storage systems, only prefixes.

### Writing field data

For netCDF files with `format=CFA3` or `format=CFA4` specified in the

[[Top]](#contents)

### File splitting algorithm

[[Top]](#contents)
