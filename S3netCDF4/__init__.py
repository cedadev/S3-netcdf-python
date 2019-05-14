# init for AWS S3 enabled version of netCDF4. package
# Docstring comes from extension module _s3netCDF4.

import pyximport
import numpy as np
pyximport.install(setup_args={'include_dirs': np.get_include()},
                  language_level=2)
