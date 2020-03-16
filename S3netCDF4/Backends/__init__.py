import pyximport
pyximport.install()

from ._s3FileObject import s3FileObject
from ._s3aioFileObject import s3aioFileObject
