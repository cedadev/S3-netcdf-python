import os
from setuptools import Extension, setup
from Cython.Build import cythonize
import numpy

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

s3nc_define_macros = [(
    "NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION"
)]

s3nc_extra_compile_args = ['-fno-strict-aliasing', '-O3']

extensions = [
    Extension(
            name="S3netCDF4.Backends.*",
            sources=["S3netCDF4/Backends/*.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.CFA._CFA*",
            sources=["S3netCDF4/CFA/_CFA*.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.CFA.Parsers._CFA*",
            sources=["S3netCDF4/CFA/Parsers/_CFA*.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.Managers.*",
            sources=["S3netCDF4/Managers/*.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.*",
            sources=["S3netCDF4/*.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
]

print(extensions)

setup(
    name='S3netCDF4',
    version='0.2.0',
    packages=['S3netCDF4'],
    install_requires=[
      'numpy',
      'cython',
      'netcdf4',
      'botocore',
      'aiobotocore==0.12.0',
      'psutil',
    ],
    language_level=3,
    ext_modules=cythonize(extensions),
    zip_safe=False,
    include_package_data=True,
    license='my License',  # example license
    description='A library to facilitate the storage of netCDF files on ObjectStores in an efficient manner.',
    long_description=README,
    url='http://www.ceda.ac.uk/',
    author='Neil Massey',
    author_email='neil.massey@stfc.ac.uk',
    classifiers=[
        'Environment :: Library',
        'Framework :: netCDF',
        'Intended Audience :: Software Developers, Data Scientists',
        'License :: OSI Approved :: BSD License', # example license
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
)
