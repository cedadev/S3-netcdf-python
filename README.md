# S3netCDF4
An extension package to netCDF4-python to enable reading and writing netCDF files and CFA-netcdf files from / to object stores and public cloud with a S3 HTTP interface, to disk or to OPeNDAP.

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
* [Reading files](#reading-files)
  * [Reading variables](#reading-variables)
  * [Reading metadata](#reading-metadata)
  * [Reading field data](#reading-field-data)

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
* `max_object_size` this controls the maximum size the **sub-array** objects can be when writing CFA files.  See [Writing files](#writing_files) section.
* `read_threads` controls the number of parallel threads that will be used when reading objects from the S3 object store.
* `write_threads` controls the number of parallel threads that will be user when writing objects to the S3 object store.

*Note that sizes can be expressed in units other than bytes by suffixing the size with a magnitude identifier:, kilobytes (`kB`), megabytes (`MB`), gigabytes (`GB`), terabytes (`TB`), exabytes (`EB`), zettabytes (`ZB`) or yottabytes (`YB`).*

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
S3netCDF4 has the ability to write netCDF3, netCDF4, CFA-netCDF3 and CFA-netCDF4 files to a POSIX filesystem, Amazon S3 object storage (or public cloud) or OPeNDAP.  Files are created in the same way as the standard netCDF4-python package, by creating a `Dataset` object.  However, the parameters to the `Dataset` constructor can vary in two ways:

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

### Writing metadata

Metadata can be written to the variables and the Dataset (global metadata) in the same way as the standard netCDF4 library, by creating a member variable on the Variable or Dataset object:

    from S3netCDF4 import s3Dataset as Dataset
    with Dataset("/Users/neil/test_dataset_cfa3.nca", mode='w', diskless=True, format="CFA3") as s3_data:
        # create the dimensions
        latd = s3_data.createDimension("lat", 196)
        lond = s3_data.createDimension("lon", 256)
        # create the dimension variables
        latitudes = s3_data.createVariable("lat", "f4", ("lat",))
        longitudes = s3_data.createVariable("lon", "f4", ("lon",))
        # create the field variable
        temp = s3_data.createVariable("tmp", "f4", ("lat", "lon"))

        # add some attributes - variable metadata
        s3_data.source = "s3netCDF4 python module tutorial"
        s3_data.units = "degrees C"
        latitudes.units = "degrees north"
        longitudes.units = "degrees east"

        # add some global metadata
        temp.author = "Neil Massey"

### Writing field data

For netCDF files with `format=NETCDF3_CLASSIC` or `format=NETCDF4`, the variable is created and field data is written to the file (as missing values) when `createVariable` is called on the `Dataset` object.  Calls to the `[]` operator (i.e. slicing the array) will write data to the variable and to the file when the operator is called.  This is the same behaviour as netCDF4-python. If a S3 URI is specified (filepath starts with `s3://`) then the file is opened or created in the local cache.

For netCDF files with `format=CFA3` or `format=CFA4` specified in the `Dataset` constructor, only the **master-array** file is written to when `createDimension`, `createVariable` etc. are called on the `Dataset` object.  When `createVariable` is called, a scalar field variable (i.e. with no dimensions) is created, the **partition-matrix** is calculated (see [File splitting algorithm](#file-splitting-algorithm)) and written to the scalar field variable.  The **sub-array** files are only created when the `[]` operator is called on the `Variable` object return from the `Dataset.createVariable` method.  This operator is implemented in S3netCDF as the `__setitem__` member function of the `s3Variable` class, and corresponds to slicing the array.

Writing a slice of field data to the **master-array** file, via `__setitem__` consists of five operations:

1. Determining which of the **sub-arrays** overlap with the slice.  This is currently done via a hypercube overlapping method and a linear search through all **partitions** for the variable.

2. Open or create the file for the **sub-array** according to the filepath or URI in the **partition** information.  If a S3 URI is specified (filepath starts with `s3://`) then the file is opened or created in the local cache, and will be uploaded when `.close` is called on the `Dataset`.  If the file already exists then it will be opened in append mode (`r+`), otherwise it will be opened in create mode (`w`)

3. In create mode (`w`) the dimensions and variable are created for the **sub-array** file, and the metadata is also written.

4. Calculate the source and target slices.  This calculates the mapping between the indices in the **master-array** and each **sub-array**.  This is complicated by allowing the user to choose any slice for the **master-array** and so this must be correctly translated to the **sub-array** indices.

5. Copy the data from the source slice to the target slice.

For those files that have an S3 URI, uploading to S3 object storage is performed when `.close()` is called on the `Dataset`.

[[Top]](#contents)

### File splitting algorithm

To split the **master-array** into it's constituent **sub-arrays** a method for splitting a large netCDF file into smaller netCDF files is used.  The high-level algorithm is:

1. Split the field variables so that there is one field variable per file.  netCDF allows multiple field variables in a single file, so this is an obvious and easy way of partitioning the file.  Note that this only splits the field variables up, the dimension variables all remain in the **master-array** file.

2. For each field variable file, split along the `time`, `latitude` or `longitude` dimensions.  Note that, in netCDF files, the order of the dimensions is arbitrary, e.g. the order could be `[time, latitide, longitude]` or `[longitude, latitude, time]` or even `[latitude, time, longitude]`.  S3netCDF4 uses the metadata and name for each dimension variable to determine the order of the dimensions so that it can split them correctly.  Note that any other dimension (`height` or `z`) will always have length of 1, i.e. the dimension will be split into a number of fields equal to its length.

The maximum size of an object (a **sub-array** file) is given in the `.s3nc4.json` config file by the `max_object_size` key / value pair.  To determine the most optimal number of splits for the `time`, `latitude` or `longitude` dimensions, while still staying under this maximum size constraint, two use cases are considered:

1. The user wishes to read all the timesteps for a single latitude-longitude point of data.
2. The user wishes to read all latitude-longitude points of the data for a single timestep.

For case 1, the optimal solution would be to split the **master-array** into **sub-arrays** that have length 1 for the `longitude` and `latitude` dimension and a length equal to the number of timesteps for the `time` dimension.  For case 2, the optimal solution would be to not split the `longitude` and `latitude` dimensions but split each timestep so that the length of the `time` dimension is 1.  However, both of these cases have the worst case scenario for the other use case.  

Balancing the number of operations needed to perform both of these use cases, while still staying under the `max_object_size` leads to an optimisation problem where the following two equalities must be balanced:

1. use case 1 = n<sub>T</sub> / d<sub>T</sub>
2. use case 2 = n<sub>lat</sub> / d<sub>lat</sub> **X** n<sub>lon</sub> / d<sub>lon</sub>

where n<sub>T</sub> is the length of the `time` dimension and d<sub>T</sub> is the number of splits along the `time` dimension.  n<sub>lat</sub> is the length of the `latitude` dimension and d<sub>lat</sub> the number of splits along the `latitude` dimension.  n<sub>lon</sub> is the length of the `longitude` dimension and d<sub>lon</sub> the number of splits along the `longitude dimension`.

The following algorithm is used:
* Calculate the current object size O<sub>s</sub> = n<sub>T</sub> / d<sub>T</sub> **X** n<sub>lat</sub> / d<sub>lat</sub> **X** n<sub>lon</sub> / d<sub>lon</sub>
* **while** O<sub>s</sub> > `max_object_size`, split a dimension:
  * **if** d<sub>lat</sub> **X** d<sub>lon</sub> <= d<sub>T</sub>:
    * **if** d<sub>lat</sub> <= d<sub>lon</sub>:
        split latitude dimension again: d<sub>lat</sub> += 1
    * **else:**
        split longitude dimension again: d<sub>lon</sub> += 1
  * **else:**
    split the time dimension again: d<sub>T</sub> += 1

Using this simple divide and conquer algorithm ensures the `max_object_size` constraint is met and the use cases require an equal number of operations.

[[Top]](#contents)

## Reading files

S3netCDF4 has the ability to read normal netCDF4 and netCDF3 files, CFA-netCDF4 and CFA-netCDF3 files from a POSIX filesystem, Amazon S3 object store (or cloud service) and OPeNDAP.  Before reading the file, S3netCDF4 will query the file size and determine whether is greater than the `max_object_size_for_memory` setting in the `~/.s3nc4.json` configuration or greater than the current available memory.  If it is, it will download the file to the local cache area.  If it is not then it will stream the file directly into memory.  See [Caching](#caching) for more details.

Files are read in the same way as the standard netCDF4-python package, by creating a `Dataset` object.  As with writing files, the parameters to the `Dataset` constructor can vary in a number of ways:

1.  The `filename` can be an S3 endpoints, i.e. it starts with `s3://`
2.  The `format` can be `CFA3` or `CFA4` to read in a CFA-netCDF3 or CFA-netCDF4 dataset.  **However**, it is not necessary to specify this keyword if the user wishes to read in a CFA file, as S3netCDF4 will determine, from the metadata, whether a netCDF file is a regular netCDF file or a CFA-netCDF file.  S3netCDF4 will also determine, from the file header, whether a netCDF file is a netCDF4 or netCDF3 file.  If the file resides on an S3 storage system, then the first 4 bytes only of the file will be first read to determine whether the file is a netCDF4 or netCDF3 file or an invalid file.  As a CFA-netCDF file is just a netCDF file, determining whether the netCDF file is a CFA-netCDF file is left until the file is read in, i.e. after the interpretation of the header.
3.  The `diskless` keyword can be set to `True` to enforce streaming to memory, even if the file size is greater than the `max_object_size_for_memory`.  Caution should be used when using this option, as the file may be bigger than the available memory which will cause the memory to be paged out to disk (swap file) and performance negatively impacted.
4.  The `persist` keyword can be set to `True` to enforce local caching of the file, even if the file size is lower than `max_object_size_for_memory`.  This ensures that the netCDF file will be available in the local cache the next time the program is run.  This can be useful while developing analysis code, or when the network connection is unreliable or slow.

*Example 4: Read a netCDF file from disk*

    from S3netCDF4 import s3Dataset as Dataset
    with Dataset("/Users/neil/test_dataset_nc4.nc", 'r') as nc_data:
        print(nc_data.variables)

*Example 5: Read a CFA-netCDF file from S3 storage and always cache to disk*

    from S3netCDF4 import s3Dataset as Dataset
    with Dataset("s3://minio/test_bucket/test_dataset_s3_cfa3.nc", 'r', persist=True) as nc_data:
        print(nc_data.variables)

Upon reading a CFA-netCDF file, the **master-array** file is parsed to transform the metadata in the file into the **partition matrix**.  See [CFA-netCDF files](#cfa-netcdf-files) for more information.  Part of this transformation involves creating an instance of the `s3Variable` class for each variable in the CFA-netCDF file.  The `s3Variable` class contains `_nc_var`: the instance of the standard `netCDF4.Variable` object; `_cfa_var`: an instance of `CFAVariable`, containing information about the CFA **sub-array** associated with this variable; and `_cfa_file`: an instance of `CFAFile`, containing information about the CFA **master-array** file that contains this variable.  The metadata in the **master-array** file is parsed to generate these two objects.  These two objects will be used when the field data is read from the CFA-netCDF file.

[[Top]](#contents)

### Reading variables

The only major place that the S3netCDF4 API deviates from the standard netCDF4 python API is in reading variable names and variables.  This is because a circular dependancy would be created if the `variables` member variable of the `netCDF4.Dataset` class was overloaded in the `s3Dataset` class.  S3netCDF4 provides two new functions:

1. `getVariables()` : returns a list of variables in the Dataset.
2. `getVariable(<variable_name>)` : return the `s3netCDF4.s3Variable` instance for `<variable_name>` if the Variable is a **master-array** in a CFA-netCDF file, or a `netCDF4.Variable` instance if it is a dimension variable, or a variable in a standard netCDF file.

*Example 6: Read a netCDF file from disk and get the "field8" variable*

    from S3netCDF4 import s3Dataset as Dataset
    with Dataset("/Users/neil/test_dataset_nc4.nc") as src_file:
        print(src_file.getVariables())
        src_var = src_file.getVariable["field8"]
        print(type(src_var))

[[Top]](#contents)

### Reading metadata

Reading metadata from the Variables or Dataset (global metadata) is done in exactly the same way as in the standard netCDF4 python package, by querying the member variable of either a Variable or Dataset.  The `ncattrs` and `getncattr` member functions of the `Dataset` and `Variable` classes are also supported.

*Example 7: Read a netCDF file, a variable and its metadata*

    from S3netCDF4 import s3Dataset as Dataset
    with Dataset("/Users/neil/test_dataset_nc4.nc") as src_file:
        print(src_file.ncattrs())
        src_var = src_file.getVariable["field8"]
        print(src_var.ncattrs())
        print(src_var.units)
        print(src_var.getncattr("units"))
        print(src_file.author)
        print(src_file.getncattr("author"))

[[Top]](#contents)

### Reading field data

Reading field data in S3netCDF follows the same principles as writing the data:
* If the file is determined to have `format=NETCDF3_CLASSIC` or `format=NETCDF4` then it is read in and the field data is made available in the same manner as the standard netCDF4-python package.  If the file is residing on S3 storage, then the entire file will be streamed to memory, if it is smaller than the `max_object_size_for_memory` setting in `~/.s3nc4.json`, or downloaded to the local cache if it is bigger than that.
* If the file is determined to have `format=CFA3` or `format=CFA4` then just the **master-array** file is read in and any field data will only be read when the `[]` operator (`__getitem__`) is called on a `s3Variable` instance.  Upon opening the **master-array** file, the CFA metadata is interpreted and the **partition-matrix** is calculated (see [File splitting algorithm](#file-splitting-algorithm)).  The **partition-matrix** consists of a number of connected objects, instantiated from classes in the `_CFAClasses.pyx` file.  This information is used when reading the field data to determine which **sub-array** files are read when the user specifies a slice to be read:

    1. A `CFAFile` as the top level container:
        1. A number of `CFADim`s : information about the dimensions in the file
        2. A number of `CFAVariable`s : information about the variables in the file:
            1. The **partition-matrix** for this variable: dimesions and shape
            2. A number of `CFAPartition`s : information about the partitions of the `CFAVariable`:
                1. Partition index (location in the partition matrix)
                2. Partition location in the **master-array**
                3. A `CFASubarray` object:
                    1. The variable name in the **sub-array** netCDF file
                    2. The file name of the **sub-array** netCDF file (can be a POSIX file name, S3 URI, etc.)
                    3. The format of the **sub-array** (always `netcdf` in S3netCDF)
                    4. The shape of the **sub-array**

Reading a slice of field data from a variable in the **master-array** file, via __getitem__ consists of five operations:

1. Determine which of the **sub-arrays** overlaps with the slice, by querying the **partition-matrix**.  This is currently done by a simple linear search through the **partitions** and a hypercube overlapping algorithm.

2. Open the file in read mode for each of the **sub-arrays** according to the filepath or URI in the **partition** information.  If a S3 URI is specified (filepath starts with `s3://`) then the file will be either downloaded to the local cache, if the size of the file is greater than `max_object_size_for_memory` (or the available memory) or streamed to memory if it is less.

3. A netCDF4-python `Dataset` object is opened from the downloaded file or streamed memory.

4. Calculate the source and target slices, the source being the **sub-array** and the target (the **master-array**) a memory-mapped numpy array with a shape equal to the user supplied slice.  The location of this **sub-array** in the **master-array** is given by the **partition** containing the **sub-array**, which gives the slice into the **master-array**.  However, both the slice of the **sub-array** and **master-array** may need to be altered if the user supplied slice does not encapsulate the whole **sub-array**, for example if a range of timesteps are taken.

5. The values in the **sub-array** are copied to the **master-array** (the memory-mapped numpy array) using the source (**sub-array**) slice and the target (**master-array**) slice.

*Currently the reading of data is performed serially.  Future work will concentrate on parallelising the reading of files and objects and copying the **sub-array** values to the **master-array**.*

[[Top]](#contents)
