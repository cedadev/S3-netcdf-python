S3-netCDF-python tutorials for JASMIN users
===========================================

Setup
-----
To access these	tutorials you will need access to the cedadev-o Caringo tenancy.
Please see the following webpage to set up the account:

https://help.jasmin.ac.uk/article/4847-using-the-jasmin-object-store

> module load jaspy
> create a venv
> pip install -e git+https://github.com/cedadev/S3-netcdf-python.git@version2

Config
------
You will need to create a configuration file in your home directory:
Using nano text editor:

> nano ~/.s3nc.json

Copy this text into the file opened in nano:
{
    "version": "9",
    "hosts": {
	"s3://cedadev-o": {
            "alias": "cedadev-o",
                "url": "http://cedadev-o.s3.jc.rl.ac.uk",
                "credentials": {
                    "accessKey": "access_key",
                    "secretKey": "secret_key"
                },
                "backend": "s3aioFileObject",
                "api": "S3v4"
        }
    },
    "cache_location": "~/.cache"
}

replace "access_key" and "secret_key" with the credentials you got from the Caringo
Swarm portal.

Contents
--------
Tutorial_1 - Read data from a CMIP6 file
