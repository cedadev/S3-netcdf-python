import os
from setuptools import Extension, setup
from Cython.Build import cythonize
s3nc_define_macros = [(
    "NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION"
)]
import numpy

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

s3nc_extra_compile_args = ['-fno-strict-aliasing', '-O3']

extensions = [
    Extension(
            name="S3netCDF4.Backends._s3aioFileObject",
            sources=["S3netCDF4/Backends/_s3aioFileObject.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
            inplace=True
    ),
    Extension(
            name="S3netCDF4.Backends._s3FileObject",
            sources=["S3netCDF4/Backends/_s3FileObject.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.CFA._CFAClasses",
            sources=["S3netCDF4/CFA/_CFAClasses.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.CFA._CFAExceptions",
            sources=["S3netCDF4/CFA/_CFAExceptions.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.CFA._CFASplitter",
            sources=["S3netCDF4/CFA/_CFASplitter.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.CFA.Parsers._CFAnetCDFParser",
            sources=["S3netCDF4/CFA/Parsers/_CFAnetCDFParser.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.CFA.Parsers._CFAParser",
            sources=["S3netCDF4/CFA/Parsers/_CFAParser.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.Managers._ConfigManager",
            sources=["S3netCDF4/Managers/_ConfigManager.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.Managers._ConnectionPool",
            sources=["S3netCDF4/Managers/_ConnectionPool.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4.Managers._FileManager",
            sources=["S3netCDF4/Managers/_FileManager.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4._Exceptions",
            sources=["S3netCDF4/_Exceptions.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
    Extension(
            name="S3netCDF4._s3netCDF4",
            sources=["S3netCDF4/_s3netCDF4.pyx"],
            define_macros=s3nc_define_macros,
            extra_compile_args=s3nc_extra_compile_args,
            include_dirs=[numpy.get_include()],
    ),
]

setup(
    name='S3netCDF4',
    version='2.0.7',
    packages=['S3netCDF4'],
    install_requires=[
      'numpy',
      'cython',
      'netcdf4',
      'botocore',
      'aiobotocore',
      'psutil',
    ],
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
    ]
)
