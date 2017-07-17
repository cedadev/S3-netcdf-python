# init for AWS S3 enabled version of netCDF4. package
# Docstring comes from extension module _S3netCDF4.

import pyximport
pyximport.install()

from ._S3netCDF4 import *
