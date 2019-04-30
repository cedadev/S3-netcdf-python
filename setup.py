import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='S3netCDF4',
    version='0.1.1',
    packages=['S3netCDF4'],
    install_requires=[
      'numpy',
      'cython',
      'netcdf4',
      'minio',
      'psutil',
    ],
    directives= {'language_level':2},
    include_package_data=True,
    license='my License',  # example license
    description='A Django app to control retrival and cache managment for data held only on storage-d tape.',
    long_description=README,
    url='http://www.ceda.ac.uk/',
    author='Sam Pepler, Neil Massey',
    author_email='neil.massey@stfc.ac.uk',
    classifiers=[
        'Environment :: Library',
        'Framework :: netCDF',
        'Intended Audience :: Data Scientists',
        'License :: OSI Approved :: BSD License', # example license
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
)
