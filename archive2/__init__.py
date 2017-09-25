# init for AWS S3 enabled version of netCDF4. package
# Docstring comes from extension module _s3netCDF4.

import pyximport
import numpy
pyximport.install(setup_args={"include_dirs":numpy.get_include()})

from ._s3netCDF4 import *
from ._s3netCDFIO import *
from ._CFAClasses import *