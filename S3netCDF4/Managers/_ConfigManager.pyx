"""
Configuration management for S3netCDF.  Configuration is stored for each user in
a JSON file in their home directory: ~/.sem-sl.json
"""

__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import os
import json
from .._Exceptions import IOException, APIException

COMPATIBLE_VERSIONS = ["9"]

def convert_file_size_string(value):
    """Convert a string containing a file size and suffix to an integer number
    of bytes.
    value <string> : string containing integer number and an optional suffix
    """
    # list of file format sizes
    file_format_sizes = ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    # dictionary mapping to multiplier
    file_format_scale = {"B" : 1,
                         "kB" : 1e3,
                         "MB" : 1e6,
                         "GB" : 1e9,
                         "TB" : 1e12,
                         "EB" : 1e15,
                         "ZB" : 1e18,
                         "YB" : 1e21}
    if value.endswith(file_format_sizes):
        suffix = value[-2:]
        size = int(value[:-2])
    elif value[-1] == "B":
        suffix = "B"
        size = int(value[:-1])
    else:
        suffix = "B"
        size = int(value)
    # multiply by scalar
    size *= file_format_scale[suffix]
    return size


def interpret_config_file(node, keys_to_convert):
    """Recursively search the dictionary for keys to convert, and convert them
    using the convert_file_size_string function above."""
    # First time entry node == dictionary
    for key, item in node.items():
        if type(item) is dict:
            interpret_config_file(item, keys_to_convert)
        elif key in keys_to_convert:
            # reassign to the dictionary
            node[key] = convert_file_size_string(item)


class Config(object):
    """Class to read in config file, interpret it and make the information
    available.
    """

    def __init__(self):
        """Initialise S3netCDF4 for this user by reading the config file from their
        home directory.  Config file is called ~/.s3nc.json"""
        # First read the JSON config file from the user home directory
        # get user home directory
        user_home = os.environ["HOME"]

        # create the path
        sl_config_path = os.path.join(user_home, ".s3nc.json")

        # open the file
        try:
            fp = open(sl_config_path)
            # deserialize from the JSON
            self._sl_user_config = json.load(fp)
            # check the version number
            if ("version" not in self._sl_user_config or
                self._sl_user_config["version"] not in COMPATIBLE_VERSIONS):
                raise APIException(
                    "User config file is not compatible with current version of"
                    " S3netCDF4.  Please update the config file at: {}".format(
                        sl_config_path
                    )
                )
            # add the filename to the config so we can refer to it in error
            # messages
            self._sl_user_config["filename"] = sl_config_path
            # we currently have no keys to convert as there are no numeric
            # fields in the config file at the moment
            keys_to_convert = []
            # interpret the config file, converting the above keys
            interpret_config_file(self._sl_user_config, keys_to_convert)
            # close the config file
            fp.close()
        except IOError:
            raise IOException(
                "User config file does not exist with path: {}".format(
                    sl_config_path
                )
            )

    def __getitem__(self, name):
        """Get a value from the s3 config"""
        return self._sl_user_config[name]

    @property
    def items(self):
        """Return the items in the dictionary / config definition"""
        return self._sl_user_config.items()

    @items.setter
    def items(self, value):
        raise AttributeError("items cannot be altered")
