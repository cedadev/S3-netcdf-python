# S3netCDF4

An extension package to netCDF4-python to enable reading and writing **netCDF**
files and **CFA-netcdf** files from / to object stores and public cloud with a
S3 HTTP interface, to disk or to OPeNDAP.

# Contents
* [Requirements](#requirements)
* [Installation](#installation)
* [Configuration](#configuration)
* [Aliases](#aliases)
* [Caching](#caching)
* [Backends](#backends)
* [Resource Usage](#resource)
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
* [List of examples](#list-of-examples)

# Requirements
S3-netCDF4 requires Python 3.7 or later.

It also requires the following packages:
* numpy==1.19.4
* Cython==0.29.21
* netCDF4==1.5.5.1
* botocore==1.19.20
* aiobotocore==1.1.2
* psutil==5.7.3

(These are fulfilled by a pip installation, so it is not necessary to install
them if you are installing the package via pip, as below.)

[[Top]](#contents)

# Installation

S3netCDF4 is designed to be installed in user space, without the user having
`root` or `sudo` privileges.  System wide installation is also supported.  It
is recommended to install S3netCDF4 into a virtual environment, rather than
using the system Python.  S3netCDF4 does not rely on any external servers,
besides the storage systems, it is run entirely on the host machine.

s3netCDF4 can be installed either from PyPi or directly from the GitHub repository.

### From PyPi

1. Create a Python 3 virtual environment:

    `python3 -m venv /path/to/venv`

2. Activate the virtual environment:

    `source /path/to/venv/bin/activate`

3. Installing S3netCDF4 requires a version of `pip` > 10.0.  To install the
latest version of pip into the virtual environment use the command:

    `pip install --upgrade pip`

4. Install from PyPi:

    `pip install S3netCDF4`

5. Copy the configuration template file from `config/.s3nc.json.template` to
    `~/.s3nc.json` and fill in the values for the variables.  See the section
    [Configuration](#configuration).

6. Run a test to ensure the package has installed correctly:

    `python test/test_s3Dataset.py`

### From GitHub    

0. Users on the STFC/NERC JASMIN system will have to activate Python 3.7 by
using the command:

    `module load jaspy`

1. Create a Python 3 virtual environment:

    `python3 -m venv /path/to/venv`

2. Activate the virtual environment:

    `source /path/to/venv/bin/activate`

3. Installing S3netCDF4 requires a version of `pip` > 10.0.  To install the
latest version of pip into the virtual environment use the command:

    `pip install --upgrade pip`

4. Install the S3netCDF4 library, directly from the github repository:

    `pip install -e git+https://github.com/cedadev/S3-netcdf-python.git#egg=S3netCDF4`

5. Copy the configuration template file from `config/.s3nc.json.template` to
`~/.s3nc.json` and fill in the values for the variables.  See the section
[Configuration](#configuration).

6. Run a test to ensure the package has installed correctly:

    `python test/test_s3Dataset.py`

7. Users on the STFC/NERC JASMIN system will have to repeat step 0 every time
they wish to use S3netCDF4 via the virtual environment.

[[Top]](#contents)

# Configuration
S3netCDF4 relies on a configuration file to resolve endpoints for the S3
services, and to control various aspects of the way the package operates.  This
config file is a JSON file and is located in the user's home directory:

`~/.s3nc.json`

In the git repository a templatised example of this configuration file is
provided:

`config/.s3nc.json.template`

This can be copied to the user's home directory, and the template renamed to
`~/.s3nc.json`.  

Alternatively, an environment variable `S3_NC_CONFIG` can be set to define the
location and name of the configuration file.  This can also be set in code,
before the import of the S3netCDF4 module:

    import os
    os.environ["S3_NC_CONFIG"] = "/Users/neil/.s3nc_different_config.json"
    from S3netCDF4._s3netCDF4 import s3Dataset

Once the config file has been copied, the variables in the template should then
be filled in.  This file is a [jinja2](http://jinja.pocoo.org/docs/2.10/)
template of a JSON file, and so can be used within an
[ansible](https://www.ansible.com/) deployment.  
Each entry in the file has a key:value pair.  An example of the file is given
below:

    {
        "version": "9",
        "hosts": {
            "s3://tenancy-0": {
                "alias": "tenancy-0",
                    "url": "http://tenancy-0.jc.rl.ac.uk",
                    "credentials": {
                        "accessKey": "blank",
                        "secretKey": "blank"
                    },
                    "backend": "s3aioFileObject",
                    "api": "S3v4"
            }
        },
        "backends": {
            "s3aioFileObject" : {
                "maximum_part_size": "50MB",
                "maximum_parts": 8,
                "enable_multipart_download": true,
                "enable_multipart_upload": true,
                "connect_timeout": 30.0,
                "read_timeout": 30.0
            },
            "s3FileObject" : {
                "maximum_part_size": "50MB",
                "maximum_parts": 4,
                "enable_multipart_download": false,
                "enable_multipart_upload": false,
                "connect_timeout": 30.0,
                "read_timeout": 30.0
            }
        },
        "cache_location": "/cache_location/.cache",
        "resource_allocation" : {
            "memory": "1GB",
            "filehandles": 20
        }
    }

* `version` indicates which version of the configuration file this is.
* `hosts` contains a list of named hosts and their respective configuration
details.
  * `s3://tenancy-0` contains the definition of a single host called
  `tenancy-0`.  For each host a number of configuration details need to be
  supplied:
    * `alias` the alias for the S3 server.  See the [Aliases](#aliases)
    section.
    * `url` the DNS resolvable URL for the S3 server, with optional port
    number.
    * `credentials` contains two keys:
        * `accessKey` the user's access key for the S3 endpoint.
        * `secretKey` the user's secret key / password for the S3 endpoint.
    * `backend` which backend to use to write the files to the S3 server.  See
    the [Backends](#backends) section.
    * `api` the api version used to access the S3 endpoint.
* `backends` contains localised configuration information for each of the
backends which may be used (if included in a `host` definition) to write the
files to the S3 server.  See the [Backends](#backends) section for more
details on backends.
    * `enable_multipart_download` allow the backend to split files fetched
    from S3 into multiple parts when downloading.
    * `enable_multipart_upload` allow the backend to split files when
    uploading.
    The advantage of splitting the files into parts is that they can be
    uploaded or downloaded asynchronously, when the backend supports
    asynchronous transfers.
    * `maximum_part_size` the maximum size for each part of the file can reach
    before it is uploaded or the size of each part when downloading a file.
    * `maximum_parts` the maximum number of file parts that are held in memory
    before they are uploaded or the number of file parts that are downloaded
    at once, for asynchronous backends.
    * `connect_timeout` the number of seconds that a connection attempt will
    be made for before timing out.
    * `read_timeout` the number of seconds that a read attempt will be made
    before timing out.
* `cache_location`  S3netCDF4 can read and write very large arrays that are
split into **sub-arrays**. To enable very large arrays to be read, S3netCDF4
uses Numpy memory mapped arrays.  `cache_location` contains the location of
these memory mapped array files.  See [Caching](#caching) section below.
* `resource_allocation` contains localised information about how much
resources each instance of S3netCDF4 should use on the host machine.  See the
the [Resource Usage](#resource) section below.
It contains two keys:
    * `memory` the amount of RAM to dedicate to this instance of S3netCDF4.
    * `file_handles` the number of file handles to dedicate to this instance
    of S3netCDF4

*Note that sizes can be expressed in units other than bytes by suffixing the
size with a magnitude identifier:, kilobytes (`kB`), megabytes (`MB`),
gigabytes (`GB`), terabytes (`TB`), exabytes (`EB`), zettabytes (`ZB`) or
yottabytes (`YB`).*

[[Top]](#contents)

## Aliases
To enable S3netCDF4 to write to disk, OPeNDAP and S3 object store, aliases are
used to identify S3 servers.  They provide an easy to remember (and type)
shorthand for the user so that they don't have to use the DNS resolved URL and
port number for each S3 object access.  When creating a netCDF4 `s3Dataset`
object, either to read or write, the user supplies a filename.  To indicate
that the file should be written to or read from a S3 server, the string
must start with `s3://`.  After this must follow the aliased server name, as
defined in the config file above.  After this aliased server name a bucket
name will follow, for example to read a netCDF file called `test2.nc` from the
`test` bucket on the `s3://tenancy-0` server, the user would use this code:

*Example 1: open a netCDF file from a S3 storage using the alias "tenancy-0"*<a
name=example-1></a>

```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
test_dataset = Dataset("s3://tenancy-0/test/test2.nc", "r")
```

On creation of the `s3Dataset` object, the S3netCDF4 package reads the
filename, determines that the filename starts with `s3://`, reads the next
part of the string up to the next `/` (which equates to `tenancy-0` in this
cases) and searches through the aliases defined in the `~/.s3nc.json` file to
find a matching alias.  If one is not found it will return an error message,
if it is found then it will establish a connection to that S3 server, using the
`url`, `accessKey` and `secretKey` defined for that server.  It is over this
connection that all the data transfers for this `s3Dataset` take place.

[[Top]](#contents)

## Caching
If the user requests to read a variable, or a slice of a variable, that is
larger than either the host machines physical memory or the
`resource_allocation: memory` setting in `~/.s3nc.json`, then S3netCDF4 will
use two strategies to enable reading very large arrays:
* a Numpy memory mapped array is used as the "target array", which will
contain the data requested by the user.  This is stored in a locally cached
file, in the `cache_location` root directory.  These files are deleted in the
destructor of S3netCDF4 - i.e. when the program exits, or the S3netCDF4 object
goes out of scope.  However, during processing, this directory has the
potential to grow quite large so adequate provision should be made on disk for
it.
* If the file being read is a **CFA-netCDF** file, referencing **sub-array**
files, then the **sub-array** files are streamed into memory (for files on S3
storage) or read from disk.  If the amount of memory used exceeds the
`resource_allocation: memory` config setting, or the number of open files
exceeds the `resource_allocation: filehandles` config setting, then the last
accessed **sub-array** file is closed.  This means it will be removed from
memory, or the file handle will be freed, allowing another **sub-array** file
to be read.

See the [Resource Usage](#resource) section below for more information on
this "memory and file shuffling" behaviour.

[[Top]](#contents)

## Backends

In S3-netCDF4, a backend refers to a set of routines that handles the
interface to a storage system.  The interface includes read and write, but
also gathering file information and file listings.  S3-netCDF4 has a pluggable
backend architecture, and so can interact with new storage systems by writing
a new backend plugin.  The backend plugins are extensions of the
`io.BufferedIOBase` Python class and implement Python file object methods, such
as `tell`, `seek`, `read` and `write`.  This enables interaction with the
backend as though they are POSIX disks.
These backends have to be configured on a host by host basis by setting the
`host: backend` value in the `~/.s3nc.json` config file.  Currently there are
two backends:

* `_s3aioFileObject`: This backend enables asynchronous transfers to a S3
compatible storage system.  It is the fastest backend for S3 and should be used
in preference to `_s3FileObject`.
* `_s3FileObject`: This is a simpler, synchronous inferface to S3 storage
systems.  It can be used if there is a problem using `_s3aioFileObject`

[[Top]](#contents)

## Resource Usage

S3netCDF4 has the ability to read and write very large files, much larger than
the available, or allocated, memory on a machine.  It also has the ability to
read and write many files to and from disk, which means the number of open
files may exceed the limit set by the file system, or the settings in `ulimit`.

Files are accessed when a Dataset is opened, and when a slice operator
(`[x,y,z]`) is used on a **CFA-netCDF** file.

To enable very large and very many files to be read and written to, S3netCDF4
employs a strategy where files are "shuffled" out of memory (to free up memory)
or closed (to free up disk handles).  The triggers for this shuffling are
configured in the `"resource_allocation"` section of the `.s3nc.json` config
file:

* `resource_allocation: memory`: the amount of memory that S3netCDF4 is allowed
to use before a shuffle is triggered.  This applies when reading or writing
files from / to remote storage, such as a S3 object store.  S3netCDF4 will
stream the entire netCDF file, or an entire **sub-array** file into memory when
reading.  When writing, it will create an entire netCDF file or **sub-array**
file in memory, writing the file to the remote storage upon closing the file.

* `resource_allocation: disk_handles`: the number of files on disk that
S3netCDF4 is allowed to have open at any one time.  This applies when reading
or writing files to disk.  S3netCDF4 uses the underlying netCDF4 library to
read and write files to disk, but it keeps a track of the number of open files.

*Note that S3netCDF4 allows full flexibility over the location of the
master-array and sub-array files of CFA-netCDF files.  It allows both to be
stored on disk or S3 storage.  For example, the master-array file could be
stored on disk for performance reasons, and the sub-array files stored on S3.  
Or the first timestep of the sub-array files could also be stored on disk to
enable users to quickly perform test analyses*

The file shuffling procedure is carried out by an internal FileManager, which
keeps notes about the files that are open at any time, or have been opened in
the past and the last time they were accessed.  The user does not see any of
this interaction, they merely interact with the S3Dataset, S3Group, S3Variable
and S3Dimension objects.

1. When a file is initially opened, a note is made of the mode and whether the
file is on disk or remote storage.  They are marked as "OPEN_NEW" and then,
"OPEN_EXISTS" when they have been opened successfully.
    - For reading from remote storage, the file is streamed into memory and
    then a netCDF Dataset is created from the read in data.
    - For writing to remote storage, the netCDF Dataset is created in memory.
    - For reading from disk, the file is opened using the underlying netCDF4
    library, and the netCDF Dataset is returned.
    - For writing to disk, the file is created using the netCDF4 library and
    the Dataset is returned.
2. If the file is accessed again (e.g. via the slicing operator), then the
netCDF Dataset is returned.  The FileManager knows these files are already open
or present in memory as they are marked as "OPEN_EXISTS".
3. Steps 1 and 2 continue until either the amount of memory used exceeds
`resource_allocation: memory` or the number of open files exceeds
`resource_allocation: disk_handles`.
4. If the amount of memory used exceeds `resource_allocation: memory`:
  - The size of the next file is determined (read) or calculated (write).  
  Files are closed, and the memory they occupy is freed using the Python
  garbage collector, until there is enough memory free to read in or create the
  next file.
  - Files that were opened in "write" mode are closed, marked as "KNOWN_EXISTS"
  and written to either the remote storage (S3) or disk.
  - Files that were open in "read" mode are simply closed and their entry is
  removed from the FileManager.
  - The priority for closing files is that the last accessed file is closed
  first.  The FileManager keeps a note when each file was accessed last.
  - If a file is accessed again in "write" mode, and it is marked as
  "KNOWN_EXISTS" in the FileManager, then it is opened in "append" mode.  In
  this way, a file can be created, be shuffled in and out of memory, and still
  be written to so that the end result is the same as if it had been in memory
  throughout the operation.
5. If the number of open files exceeds `resource_allocation: disk_handles`:
  - The procedure for point 4 is followed, except rather than closing files
  until there is enough memory available, files are closed until there are free
  file handles.
  - Files are marked as "KNOWN_EXISTS" as in point 4.

This file shuffling procedure is fundamental to the performance of S3netCDF4,
as it minimises the number of times a file has to be streamed from remote
storage, or opened from disk.  There are also optimisations in the File
Manager, for example, if a file has been written to and then read, it will use
the copy in memory for all operations, rather than holding two copies, or
streaming to and from remote storage repeatably.

[[Top]](#contents)

## Writing files
S3netCDF4 has the ability to write **netCDF3**, **netCDF4**, **CFA-netCDF3**
and **CFA-netCDF4** files to a POSIX filesystem, Amazon S3 object storage (or
public cloud) or OPeNDAP.  Files are created in the same way as the standard
netCDF4-python package, by creating a `s3Dataset` object.  However, the
parameters to the `s3Dataset` constructor can vary in two ways:

1. The `filename` can be an S3 endpoint, i.e. it starts with `s3://`
2. The `format` keyword can also, in addition to the formats permitted by
netCDF4-python, be `CFA3`, to create a CFA-netCDF3 dataset, or `CFA4`, to
create a CFA-netCDF4 dataset.
3. If creating a `CFA3` or `CFA4` dataset, then an optional keyword parameter
can be set: `cfa_version`.  This can be either `"0.4"` or `"0.5"`.  See the
[CFA-netCDF](#cfa-netcdf) files section below.

*Example 2: Create a netCDF4 file in the filesystem*<a name=example-2></a>
```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
test_dataset = Dataset("/Users/neil/test_dataset_nc4.nc", 'w',
format='NETCDF4')
```

*Example 3: Create a CFA-netCDF4 file in the filesystem with CFA version
0.5 (the default)*<a name=example-3></a>
```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
cfa_dataset = Dataset("/Users/neil/test_dataset_cfa4.nc", 'w',format='CFA4')
```
*Example 4: Create a CFA-netCDF3 file on S3 storage with CFA version 0.4*<a
name=example-4></a>
```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
cfa_dataset = Dataset("s3://tenancy-0/test_bucket/test_dataset_s3_cfa3.nc",
'w', format='CFA3', cfa_version="0.4")
```

[[Top]](#contents)

### CFA-netCDF files
Choosing `format="CFA3"` or `format="CFA4"` when creating a file creates a
CFA-compliant netCDF file.  This consists of a **master-array** file and a
number of **sub-array** files.
The version of CFA to use can also be specified, either `cfa_version="0.4"` or
`cfa_version="0.5"`.  `"0.4"` follows the
[CFA conventions](http://www.met.reading.ac.uk/~david/cfa/0.4/), where the
**sub-array** metadata is written into the attributes of the netCDF variables.
`"0.5"` refactors the **sub-array** metadata into extra groups and variables
in the **master-array** file.  `"0.5"` is the preferred format as it is more
memory efficient, relying on netCDF slicing and partial reading of files, and
is faster as it does not require parsing when the **master-array** file is
first read.  As it uses features of netCDF4, `cfa_version="0.5"` is only
compatible with `format="CFA4"`

*Note that `cfa_version="0.5"` and `format="CFA3"` are incompatible, as NETCDF3
does not enable groups to be used*

The **master-array** file contains:

* the dimension definitions
* dimension variables
* scalar variable definitions: variable definitions without reference to the
domain it spans
* variable metadata
* global metadata
* It does not contain any field data, but it *does* contain data for the
dimension variables, and therefore the domain of each variable.  
* The **master-array** file may contain a single field variable or multiple
field variables.  

The **sub-array** files contain a subdomain of a single variable in the
**master-array**.  They contain:

* the dimension definitions for the subdomain
* the dimension variables for the subdomain
* a single variable definition, complete with reference to the dimensions
* metadata for the variable

Therefore, each **sub-array** file is a self-describing netCDF file.  If the
**master-array** file is lost, it can be reconstructed from the **sub-array**
files.

In CFA v0.4, the *variable metadata* (netCDF attributes) in each variable in
the **master-array** file contains a **partition matrix**.  The **partition
matrix** contains information on how to reconstruct the **master-array**
variables from the associated **sub-arrays** and, therefore, also contains the
necessary information to read or write slices of the **master-array**
variables.

In CFA v0.5, the **partition matrix** is stored in a group.  This group has the
same name as the variable, but prefixed with `cfa_`.  The group contains
dimensions and variables to store the information for the **partition matrix**
and the **partitions**.
Full documentation for CFA v0.5 will be forthcoming.

The **partition matrix** contains:

* The dimensions in the netCDF file that the partition matrix acts over (e.g.
`["time", "latitude", "longitude"`)
* The shape of the partition matrix (e.g. `[4,2,2]`)
* A list of partitions

Each **partition** in the **partition matrix** contains:

* An index for the partition into the partition matrix - a list the length of
the number of dimensions for the variable (e.g `[3, 1, 0]`)
* The location of the partition in the **master-array** - a list (the length
of the number of dimensions) of pairs, each pair giving the range of
indices in the **master-array** for that dimension (e.g. `[[0, 10], [20,
40], [0, 45]]`)
* A definition of the **sub-array** which contains:
    * The path or URI of the file containing the **sub-array**.  This may be
    on the filesystem, an OPeNDAP file or an S3 URI.
    * The name of the netCDF variable in the **sub-array** file
    * The format of the file (always `netCDF` for S3netCDF4)
    * The shape of the variable - i.e. the length of the subdomain in each
    dimension

For more information see the [CFA conventions 0.4 website](http://
www.met.reading.ac.uk/~david/cfa/0.4/).
There is also a useful synopsis in the header of the `_CFAClasses.pyx` file in
the S3netCDF4 source code.  Documentation for the `"0.5"` version of CFA will
follow.

*Note that indices in the partition matrix are indexed from zero, but the
indices are inclusive for the location of the partition in the master-array.  
This is different from Python where the indices are non-inclusive.  The
conversion between the two indexing methods is handled in the implementation
of _CFAnetCDFParser, so the user does not have to worrying about converting
indices*

[[Top]](#contents)

### Creating dimensions and variables

Creating dimensions and variables in the netCDF or CFA-netCDF4 dataset follows
the same method as creating variables in the standard netCDF4-python library:

*Example 5: creating dimensions and variables*<a name=example-5></a>
```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
cfa_dataset = Dataset("s3://minio/test_bucket/test_dataset_s3_cfa3.nc", 'w',
format='CFA3')

timed = cfa_dataset.createDimension("time", None)
times = cfa_dataset.createVariable("time", "f4", ("time",))
```

When creating variables, a number of different workflows for writing the files
occur.  Which workflow is taken depends on the combination of the filename
path (`S3`, filesystem or OPeNDAP) and format (`CFA3` and `CFA4` or `NETCDF4`
and `NETCDF3_CLASSIC`).  These workflows can be summarised by:

* `format=NETCDF4` or `format=NETCDF3_CLASSIC`.  These two options will create
a standard netCDF file.
  * If the filename is on a remote system, (i.e. it contains `s3://`) then the
  netCDF file will be created in memory and uploaded (PUT) to the S3
  filesystem when `s3Dataset.close()` is called or the file is "shuffled" out
  of memory. (see [Resource Usage](#resource) for more details).
  * If the filename does not contain `s3://` then the netCDF file will be
  written out to the filesystem or OPeNDAP, with the behaviour following the
  standard netCDF4-python library.

* `format=CFA3` or `format=CFA4`.  These two options will create a
  **CFA-netCDF file**.
  * At first only the **master-array** file is created and written to.  The
  **sub-array** files are created and written to when data is written to the
  **master-array** variable.
  * When the variable is created, the dimensions are supplied and this enables
  the **partition matrix** metadata to be generated:
    * The [file splitting algorithm](#file-splitting-algorithm) determines how
    to split the variable into the **sub-arrays**, or the user can supply the
    shape of the **sub-arrays**
    * From this information the **partition matrix** shape and **partition
    matrix** list of dimensions are created.  The **partition matrix** is
    represented internally by a netCDF dataset, and this is also created.
  * Only when a variable is written to, via a slice operation on a variable,
  is each individual **partition** written into the **partition matrix**.
    * The **sub-array** file is created, either in memory for remote
    filesystems (S3), or to disk for local filesystems (POSIX).
    * The filename for the **sub-array** is determined programmatically.
    * The location in the **master-array** for each **sub-array** (and its
     shape) is determined by the slice and the **sub-array** shape determined
     by either the file splitting algorithm, or supplied by the user.
    * This single **partition** information is written into the **partition-
    matrix**
    * The field data is written into the **sub-array** file.
    * On subsequent slices into the same **sub-array**, the **partition**
    information is used, rather than rewritten.
  * When the **master-array** file is closed (by the user calling
      `s3Dataset.close()`):
    * The **partition matrix** metadata is written to the **master-array**
    * If the files are located on a remote filesystem (S3), then they only
    currently exist in memory (unless they have been "shuffled" to storage).
    They are now closed (in memory) and then uploaded to the remote storage.  
    Any appended files are also uploaded to remote storage.
    * If the files are not on a remote filesystem, then they are closed, the
    **sub-array** files in turn, and then the **master-array** file last.

### Filenames and file hierarchy of CFA files

As noted above, CFA files actually consist of a single **master-array** file
and many **sub-array** files.  These **subarray-files** are referred to by
their filepath or URI in the partition matrix.  To easily associate the **sub-
array** files with the **master-array** file, a naming convention and file
structure is used:

* The [CFA conventions](http://www.met.reading.ac.uk/~david/cfa/0.4/) dictate
that the file extension for a CFA-netCDF file should be `.nca`
* A directory is created in the same directory / same root URI as the **master-
array** file.  This directory has the same name **master-array** file without
the `.nca` extension
* In this directory all of the **sub-array** files are contained.  These
subarray files follow the naming convention:

    `<master-array-file-name>.<variable-name>.[<location in the partition
    matrix>].nc`

Example for the **master-array** file `a7tzga.pdl4feb.nca`:

    ├── a7tzga.pdl4feb.nca
    ├── a7tzga.pdl4feb
    │   ├── a7tzga.pdl4feb.field16.0.nc
    │   ├── a7tzga.pdl4feb.field16.1.nc
    │   ├── a7tzga.pdl4feb.field186.0.nc
    │   ├── a7tzga.pdl4feb.field186.1.nc
    │   ├── a7tzga.pdl4feb.field1.0.0.nc
    │   ├── a7tzga.pdl4feb.field1.0.1.nc
    │   ├── a7tzga.pdl4feb.field1.1.0.nc
    │   ├── a7tzga.pdl4feb.field1.1.1.nc

On an S3 storage system, the **master-array** directory will form part of the
*prefix* for the **sub-array** objects, as directories do not exist, in a
literal sense, on S3 storage systems, only prefixes.

*Note that the metadata in the master-array file informs S3netCDF4 where the
sub-array files are located.  The above file structure defines the default
behaviour, but the specification of S3netCDF4 allows sub-array files to be
located anywhere, be that on S3, POSIX disk or OpenDAP.*

### Writing metadata

Metadata can be written to the variables and the Dataset (global metadata) in
the same way as the standard netCDF4 library, by creating a member variable on
the Variable or Dataset object:

*Example 6: creating variables with metadata*<a name=example-6></a>
```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
with Dataset("/Users/neil/test_dataset_cfa3.nca", mode='w', diskless=True,
format="CFA3") as s3_data:
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
```

### Writing field data

For netCDF files with `format=NETCDF3_CLASSIC` or `format=NETCDF4`, the
variable is created and field data is written to the file (as missing values)
when `createVariable` is called on the `s3Dataset` object.  Calls to the `[]`
operator (i.e. slicing the array) will write data to the variable and to the
file when the operator is called.  This is the same behaviour as netCDF4-
python. If a S3 URI is specified (filepath starts with `s3://`) then the file
is first created in memory and then streamed to S3 on closing the file.

For netCDF files with `format=CFA3` or `format=CFA4` specified in the
`s3Dataset` constructor, only the **master-array** file is written to when
`createDimension`, `createVariable` etc. are called on the `s3Dataset`
object.  When `createVariable` is called, a scalar field variable (i.e. with
no dimensions) is created, the **partition-matrix** is calculated (see [File
splitting algorithm](#file-splitting-algorithm)) and written to the scalar
field variable.  The **sub-array** files are only created when the `[]`
operator is called on the `Variable` object return from the
`s3Dataset.createVariable` method.  This operator is implemented in S3netCDF as
the `__setitem__` member function of the `s3Variable` class, and corresponds
to slicing the array.

Writing a slice of field data to the **master-array** file, via `__setitem__`
consists of five operations:

1. Determining which of the **sub-arrays** overlap with the slice.  This is
currently done via a hypercube overlapping method, i.e. the location of the
**sub-array** can be determined by dividing the dimension index by the length
of the dimension in the **partition matrix**.  This assumes that the **sub-arrays** are uniform (per dimension) in size.

2. If the size of the **sub-array** file will cause the currently used amount
of memory to exceed the `resource_allocation: memory` setting in `~/.s3nc.json`
then some files may be shuffled out of memory.  See the [Resource Usage](#resource) section above.  This may result in some files being written
to the remote storage, meaning they will be opened in append mode the next time
they are written to.
If, even after the file shuffling has occurred, the size of the **sub-array**
cannot be contained in memory then a memory error will occur.

3. Open or create the file for the **sub-array** according to the filepath or
URI in the **partition** information.  If a S3 URI is specified (filepath
starts with `s3://`) then the file is opened or created in memory, and
will be uploaded when `.close()` is called on the `s3Dataset`.  The file will be
will be opened in create mode (`w`).

4. The dimensions and variable are created for the **sub-array** file, and the
metadata is also written.

5. Calculate the source and target slices.  This calculates the mapping
between the indices in the **master-array** and each **sub-array**.  This is
complicated by allowing the user to choose any slice for the **master-array**
and so this must be correctly translated to the **sub-array** indices.

6. Copy the data from the source slice to the target slice.

For those files that have an S3 URI, uploading to S3 object storage is
performed when `.close()` is called on the `s3Dataset`.

### Partial writing of field data

The **partition** information is only written into the **partition-matrix**
when the s3Dataset is in "write" mode and the user slices into the part of the
**master-array** that is covered by that **partition**.  Consequently, the
**sub-array** file is only created when the **partition** is written into the
**partition-matrix**.

This leads to the situation that a large part of the **partition-matrix** may
have undefined data, and a large number of **sub-array** files may not exist.  
This makes s3netCDF4 excellent for sparse data, as the **sub-array** size can
be optimised so that the sparse data occupies minimal space.

If, in "read" mode, the user specifies a slice that contains a **sub-array**
that is not defined, then the **missing value** (`_FillValue`) is returned for
the sub-domain of the **master-array** which the **sub-array** occupies.

[[Top]](#contents)

### File splitting algorithm

To split the **master-array** into it's constituent **sub-arrays** a method
for splitting a large netCDF file into smaller netCDF files is used.  The
high-level algorithm is:

1. Split the field variables so that there is one field variable per file.  
netCDF allows multiple field variables in a single file, so this is an obvious
and easy way of partitioning the file.  Note that this only splits the field
variables up, the dimension variables all remain in the **master-array** file.

2. For each field variable file, split along the `time`, `level`, `latitude` or
`longitude` dimensions.  Note that, in netCDF files, the order of the
dimensions is arbitrary, e.g. the order could be `[time, level, latitide,
longitude]` or `[longitude, latitude, level, time]` or even `[latitude, time,
longitude, level]`.  
S3netCDF4 uses the metadata and name for each dimension variable to determine
the order of the dimensions so that it can split them correctly.  Note that
any other dimension (`ensemble` or `experiment`) will always have length of 1,
i.e. the dimension will be split into a number of fields equal to its length.

The maximum size of an object (a **sub-array** file) can be given as a keyword
argument to `s3Dataset.createVariable` or `s3Group.createVariable`:
`max_subarray_size=`.  If no `max_subarray_size` keyword is supplied, then it
defaults to 50MB.
To determine the most optimal number of splits for the `time`, `latitude` or
`longitude` dimensions, while still staying under this maximum size
constraint, two use cases are considered:

1. The user wishes to read all the timesteps for a single latitude-longitude
point of data.
2. The user wishes to read all latitude-longitude points of the data for a
single timestep.

For case 1, the optimal solution would be to split the **master-array** into
**sub-arrays** that have length 1 for the `longitude` and `latitude` dimension
and a length equal to the number of timesteps for the `time` dimension.  For
case 2, the optimal solution would be to not split the `longitude` and
`latitude` dimensions but split each timestep so that the length of the `time`
dimension is 1.  However, both of these cases have the worst case scenario for
the other use case.  

Balancing the number of operations needed to perform both of these use cases,
while still staying under the `max_subarray_size` leads to an optimisation
problem where the following two equalities must be balanced:

1. use case 1 = n<sub>T</sub> / d<sub>T</sub>
2. use case 2 = n<sub>lat</sub> / d<sub>lat</sub> **X** n<sub>lon</sub> /
d<sub>lon</sub>

where n<sub>T</sub> is the length of the `time` dimension and d<sub>T</sub> is
the number of splits along the `time` dimension.  n<sub>lat</sub> is the
length of the `latitude` dimension and d<sub>lat</sub> the number of splits
along the `latitude` dimension.  n<sub>lon</sub> is the length of the
`longitude` dimension and d<sub>lon</sub> the number of splits along the
`longitude dimension`.

The following algorithm is used:
* Calculate the current object size O<sub>s</sub> = n<sub>T</sub> / d<sub>T</
sub> **X** n<sub>lat</sub> / d<sub>lat</sub> **X** n<sub>lon</sub> /
d<sub>lon</sub>
* **while** O<sub>s</sub> > `max_subarray_size`, split a dimension:
  * **if** d<sub>lat</sub> **X** d<sub>lon</sub> <= d<sub>T</sub>:
    * **if** d<sub>lat</sub> <= d<sub>lon</sub>:
        split latitude dimension again: d<sub>lat</sub> += 1
    * **else:**
        split longitude dimension again: d<sub>lon</sub> += 1
  * **else:**
    split the time dimension again: d<sub>T</sub> += 1

Using this simple divide and conquer algorithm ensures the `max_subarray_size`
constraint is met and the use cases require an equal number of operations.

*Note that in v2.0 of S3netCDF4, the user can specify the sub-array shape in
the s3Dataset.createVariable method.  This circumvents the file-splitting
algorithm and uses just the sub-array shape specified by the user.*

[[Top]](#contents)

## Reading files

S3netCDF4 has the ability to read normal netCDF4 and netCDF3 files, **CFA-
netCDF4** and **CFA-netCDF3** files from a POSIX filesystem, Amazon S3 object
store and OPeNDAP.  
For files on remote storage, before reading the file, S3netCDF4 will query
the file size and determine whether it is greater than the
`resource_allocation: memory` setting in the `~/.s3nc.json` configuration or
greater than the current available memory.  If it is, then some files will be
"shuffled" out of memory until there is enough allocated memory available.  See
[Resource Usage](#resource) for more details.  If it is less than the
`resource_allocation: memory` setting then it will stream the file
directly into memory.  Files on local disk (POSIX) are opened in the same way
as the standard netCDF4 library, i.e. the header, variable and dimension
information and metadata are read in, but no field data is read.

From a user perspective, files are read in the same way as the standard
netCDF4-python package, by creating a `s3Dataset` object.  As with writing
files, the parameters to the `s3Dataset` constructor can vary in a number of
ways:

1.  The `filename` can be an S3 endpoint, i.e. it starts with `s3://`, or a
file on the disk, or an OpenDAP URL.
2.  The `format` can be `CFA3` or `CFA4` to read in a **CFA-netCDF3** or **CFA-
netCDF4** dataset.  **However**, it is not necessary to specify this keyword if
the user wishes to read in a CFA file, as S3netCDF4 will determine, from the
metadata, whether a netCDF file is a regular netCDF file or a CFA-netCDF file.  
S3netCDF4 will also determine, from the file header, whether a netCDF file is a
netCDF4 or netCDF3 file.  If the file resides on an S3 storage system, then the
first 6 bytes only of the file will be first read to determine whether the file
is a netCDF4 or netCDF3 file or an invalid file.  As a CFA-netCDF file is just
a netCDF file, determining whether the netCDF file is a CFA-netCDF file is left
until the file is read in, i.e. after the interpretation of the header.
3. Files that are on remote storage are streamed into memory.  As files are
read in, other files may be "shuffled" out of memory if the currently used
memory exceeds the `resource_allocation: memory` setting in the `~/.s3nc.json`
config file.  See [Resource Usage](#resource).

*Example 7: Read a netCDF file from disk*<a name=example-7></a>
```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
with Dataset("/Users/neil/test_dataset_nc4.nc", 'r') as nc_data:
    print(nc_data.variables)
```

*Example 8: Read a CFA-netCDF file from S3 storage*<a name=example-8></a>
```
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
from S3netCDF4 import s3Dataset as Dataset
with Dataset("s3://tenancy-0/test_bucket/test_dataset_s3_cfa3.nc", 'r') as nc_data:
    print(nc_data.variables)
```

Upon reading a CFA-netCDF file, the **master-array** file is interpreted to
transform the metadata in the file (for CFA `"v0.4"`), or the information in
the CFA group for the variable (for CFA `"v0.5"`) into the **partition
matrix**.  See [CFA-
netCDF files](#cfa-netcdf-files) for more information.  
Part of this transformation involves creating an instance of the `s3Variable`
class for each variable in the CFA-netCDF file.  The `s3Variable` class
contains `_nc_var`: the instance of the standard `netCDF4.Variable` object;
`_cfa_var`: an instance of `CFAVariable`, containing information about the CFA
**sub-array** associated with this variable; and `_cfa`: an instance of
`CFADataset`, containing information about the CFA **master-array** file that
contains this variable.
The metadata, or CFA group, in the **master-array** file is parsed to generate
these two objects.  These two objects will be used when a user calls a slice
operation on a s3Variable object.

[[Top]](#contents)

### Reading variables

In v2.0.x,the s3netCDF4 API now matches the standard netCDF4 python API in
reading variable names and variables.  Previously, two extra functions were used
(`variables()`, and `getVariable()`).  During the rework, a way was found to
provide 100% compatibility with the netCDF4 python API.  This is reflected in
the method of handling variables:

1. `s3Dataset.variables`, or `s3Group.variables` : returns a list of variables
in the Dataset.
2. `s3Dataset.variables[<variable_name>]`, or
`s3Group.variables[<variable_name>]` : return the `s3netCDF4.s3Variable`
instance for `<variable_name>` if the variable is a **master-array** in a CFA-
netCDF file, or a `netCDF4.Variable` instance if it is a dimension variable, or
a variable in a standard netCDF file.

*Example 9: Read a netCDF file from disk and get the "field8" variable*<a name=example-9></a>
```
from S3netCDF4 import s3Dataset as Dataset
with Dataset("/Users/neil/test_dataset_nc4.nc") as src_file:
    print(src_file.variables)
    src_var = src_file.variables["field8"]
    print(type(src_var))
```
[[Top]](#contents)

### Reading metadata

Reading metadata from the Variables or Dataset (global metadata) is done in
exactly the same way as in the standard netCDF4 python package, by querying the
member variable of either a Variable or Dataset.  The `ncattrs` and `getncattr`
member functions of the `Dataset` and `Variable` classes are also supported.

*Example 10: Read a netCDF file, a variable and its metadata*<a name=example-10></a>
```
from S3netCDF4 import s3Dataset as Dataset
with Dataset("/Users/neil/test_dataset_nc4.nc") as src_file:
    print(src_file.ncattrs())
    src_var = src_file.getVariable["field8"]
    print(src_var.ncattrs())
    print(src_var.units)
    print(src_var.getncattr("units"))
    print(src_file.author)
    print(src_file.getncattr("author"))
```

[[Top]](#contents)

### Reading field data

Reading field data in S3netCDF follows the same principles as writing the
data:
1. If the file is determined to have `format=NETCDF3_CLASSIC` or
`format=NETCDF4` then it is read in and the field data is made available in
the same manner as the standard netCDF4-python package.  If the file is
residing on S3 storage, then the entire file will be streamed to memory, if
it is larger than the `resource_allocation: memory` setting in `~/
.s3nc.json`, or larger than the available memory, then a memory error will
be returned.
2. If the file is determined to have `format=CFA3` or `format=CFA4` then just
the **master-array** file is read in and any field data will only be read
when the `[]` operator (`__getitem__`) is called on a `s3Variable` instance.  
Upon opening the **master-array** file:
3. if the file is `"v0.4"` of the CFA conventions, the CFA metadata is taken
from the variable metadata. The **partition-matrix** is constructed (see [File
splitting algorithm](#file-splitting-algorithm)) internally as a netCDF group
with dimensions and variables containing the partition information.
4. if the files is CFA `"v0.5"`, then the **partition-matrix** is read in
directly from the Groups, Dimensions and Variables in the file, without any
parsing having to take place.
5. the `_cfa`, `_cfa_grp`, `_cfa_dim` and `_cfa_var` objects are created as
member variables of the `s3Dataset`, `s3Group`, `s3Dimension` and `s3Variable`
objects respectively.  These are instances of `CFADataset`, `CFAGroup`,
`CFADimension` and `CFAVariable` respectively.  The **partition-matrix** is
contained within a `netCDF4.Group` within the `_cfa_var` instance of
`CFAVariable`

Internally, the **partition-matrix** consists of a netCDF group, which itself
contains the dimensions of the partition-matrix, and variables containing the
partition information.
Within the s3Dataset, s3Variable and s3Group objects, there are objects that
contain higher level CFA data, and the methods to operate on that data.
This information is used when a user slices the field data to determine which
**sub-array** files are read and which portion of the **sub-array** files are
included in the slice:

1. A `CFADataset` as the top level container:
    1. A number of `CFAGroup`s: information about groups in the file.  There
    is always at least one group: the `root` group is explicit in its
    representation in the `CFADataset`.  Within the `CFAGroup` there are:
        1. A number of `CFADim`s : information about the dimensions in the
        Dataset
        2. A number of `CFAVariable`s : information about the variables in
        the Dataset, which contains:
            1. The **partition-matrix** which consists of a netCDF group
            containing:
                1. the scalar dimensions, with no units or associated
            dimension variable
                2. the variables containing the partition information:
                    1. `pmshape` : the shape of the **partition-matrix**
                    2. `pmdimensions` : the dimensions in the **master-array**
                    file which the partition matrix acts over.
                    3. `index` : the index in the **partition-matrix**.  This
                    is implied by the location in the **partition-matrix** but
                    it is retained to detect erroneous lookups by the slicing
                    algorithm.
                    4. `location` : the location in the **master-array file**
                    5. `ncvar` : the name of the variable in the **sub-array**
                    file.
                    6. `file` : the URL or path of the **sub-array** file.
                    7. `format` : the format of the **sub-array** file.
                    8. `shape` : the shape of the **sub-array** file.

            2. Methods to act upon the variable and its **partition-matrix**,
            including:
                1. `__getitem__` : returns the necessary information to
                read and write **sub-array** files.
                2. `getPartition` : return a user-readable version of a
                partition (a single element in the **partition-matrix**) as a
                Python named tuple, rather than a netCDF Group or Variable.

Reading a slice of field data from a variable in the **master-array** file,
via __getitem__ consists of five operations:

1. If the total size of the requested slice is greater than
`resource_allocation: memory` (or the available memory) then a Numpy memory
mapped array is created in the location indicated by the `cache_location:`
setting in the `~/.s3nc.json` config file.

2. Determine which of the **sub-arrays** overlaps with the slice, by querying
the **partition-matrix**.  This is currently done by a simple arithmetic
operation that relies on the **partitions** all being the same size.

3. Calculate the source and target slices, the source being the **sub-array**
and the target (the **master-array**) a memory-mapped Numpy array with a shape
equal to the user supplied slice.  The location of this **sub-array** in the
**master-array** is given by the **partition** containing the **sub-array**,
which gives the slice into the **master-array**.  However, both the slice of
the **sub-array** and **master-array** may need to be altered if the user
supplied slice does not encapsulate the whole **sub-array**, for example if a
range of timesteps are taken.

4. For each of the **sub-arrays** the file specified by the `file` variable in
the **partition** information is opened.  If the file is on disk, it is simply
opened in the same way as a standard netCDF4 python file.  If it is on a remote
file system, such as S3, then it is streamed into memory.  If the size of the
**sub-array** file will cause the currently used amount
of memory to exceed the `resource_allocation: memory` setting in `~/.s3nc.json`
then some files may be shuffled out of memory.  See the [Resource
Usage](#resource) section above.
If, even after the file shuffling has occurred, the size of the **sub-array**
cannot be contained in memory then a memory error will occur.

5. A netCDF4-python `Dataset` object is opened from the downloaded file or
streamed memory.

6. The values in the **sub-array** are copied to the **master-array** (the
memory-mapped Numpy array) using the source (**sub-array**) slice and the
target (**master-array**) slice.

*Currently the reading of data is performed asynchronously, using
aiobotocore.  S3netCDF4 allows parallel workflows using multi-processing or
Dask, by using the CFA information stored in the CFADataset, CFAGroup,
CFADimension and CFAVariable classes.  Examples of this will follow*

## List of examples

* [Example 1: open a netCDF file from a S3 storage using the alias "tenancy-0"](#example-1)
* [Example 2: Create a netCDF4 file in the filesystem](#example-2)
* [Example 3: Create a CFA-netCDF4 file in the filesystem with CFA version 0.5](#example-3)
* [Example 4: Create a CFA-netCDF4 file in the filesystem](#example-4)
* [Example 5: Create a CFA-netCDF3 file on S3 storage](#example-5)
* [Example 6: creating dimensions and variables](#example-6)
* [Example 7: creating variables with metadata](#example-7)
* [Example 8: Read a netCDF file from disk](#example-8)
* [Example 9: Read a netCDF file from disk and get the "field8" variable](#example-9)
* [Example 10: Read a netCDF file, a variable and its metadata](#example-10)

[[Top]](#contents)
