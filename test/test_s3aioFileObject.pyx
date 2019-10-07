from S3netCDF4.Backends._s3aioFileObject import s3aioFileObject
from S3netCDF4._Exceptions import IOException, APIException
import unittest
import asyncio
import json
import time
import io
import inspect

class AsyncIOTestFactory(type):
    """Metaclass that creates a `test_something` function for all those functions
    called `_test_something` which simply calls asyncio.run(`_test_something`)"""
    def __new__(cls, name, bases, dct):
        def mapper(attribute):
            if inspect.iscoroutinefunction(attribute):
                def async_wrapper(*args, **kwargs):
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(attribute(*args, **kwargs))
                return async_wrapper
            else:
                return attribute
        return super().__new__(
            cls,
            name,
            bases,
            { k: mapper(v) for k, v in dct.items() }
        )

class s3aioFileObjectGeneralTest(object, metaclass=AsyncIOTestFactory):
    """All of the general tests for either a read or write transaction."""

    async def test_detach(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="w"
        ) as s3c:
            try:
                s3c.detach()
            except io.UnsupportedOperation:
                return
            self.fail(
                "s3aioFileObject.detach did not raise io.UnsupportedOperation"
            )

    async def test_close(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="rw"
        ) as s3c:
            if await s3c.close():
                return
            else:
                self.fail("s3aioFileObject.close returned False")

    async def test_readable(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="rw"
        ) as s3c:
            if s3c.readable():
                return
            else:
                self.fail("s3aioFileObject.readable returned False")

    async def test_truncate(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="w"
        ) as s3c:
            try:
                s3c.truncate()
            except io.UnsupportedOperation:
                return
            self.fail(
                "s3aioFileObject.truncate did not raise io.UnsupportedOperation"
            )

    async def test_fileno(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="w"
        ) as s3c:
            try:
                s3c.fileno()
            except io.UnsupportedOperation:
                return
            self.fail(
                "s3aioFileObject.fileno did not raise io.UnsupportedOperation"
            )

    async def test_seekable(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="rw"
        ) as s3c:
            if s3c.seekable():
                return
            else:
                self.fail("s3aioFileObject.seekable returned False")

    async def test_tell(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="rw"
        ) as s3c:
            if s3c.tell() == 0:
                return
            else:
                self.fail("s3aioFileObject.tell did not return 0")

    async def test_seek(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="rw"
        ) as s3c:
            # Three different methods for seek:
            #   whence = io.SEEK_SET
            #   whence = io.SEEK_CUR
            #   whence = io.SEEK_END
            # the current pointer is on zero
            if not await s3c.seek(0, whence=io.SEEK_SET) == 0:
                self.fail("s3aioFileObject.seek did not return 0")

            if not await s3c.seek(10, whence=io.SEEK_SET) == 10:
                self.fail("s3aioFileObject.seek did not return 10")
            # now on 10
            try:
                await s3c.seek(-1, whence=io.SEEK_SET)
            except IOException:
                pass
            else:
                self.fail("s3aioFileObject.seek did not raise IOException")
            # should have failed so still on 10

            # the current pointer is on ten (10)
            if not await s3c.seek(-10, whence=io.SEEK_CUR) == 0:
                self.fail("s3aioFileObject.seek did not return 0")

            # now on 0 - should raise an exception if we seek below 0
            try:
                await s3c.seek(-1, whence=io.SEEK_CUR)
            except IOException:
                pass
            else:
                self.fail("s3aioFileObject.seek did not raise IOException")

            # still on zero: get the size to seek past it
            size = await s3c._getsize()
            try:
                await s3c.seek(size+1, whence=io.SEEK_CUR)
            except IOException:
                pass
            else:
                self.fail("s3aioFileObject.seek did not raise IOException")

            # still on zero - seek from the end
            try:
                await s3c.seek(size+1, whence=io.SEEK_END)
            except IOException:
                pass
            else:
                self.fail("s3aioFileObject.seek did not raise IOException")

            # still on 0 - seek backwards from the end
            try:
                await s3c.seek(-1, whence=io.SEEK_END)
            except IOException:
                pass
            else:
                self.fail("s3aioFileObject.seek did not raise IOException")

            if await s3c.seek(10, whence=io.SEEK_END) != size-10:
                self.fail("s3aioFileObject.seek did not return {}".format(
                    size-10
                ))


class s3aiot1FileObjectWriteTest(unittest.TestCase, s3aioFileObjectGeneralTest):

    def setUp(self):
        """Set up the s3FileObject but don't connect."""
        # load the credentials from the hidden file
        fh = open(".s3config.json")
        self.cfg = json.load(fh)
        fh.close()

    async def test_1writable(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="w"
        ) as s3c:
            if s3c.writable():
                return
            else:
                self.fail("s3aioFileObject.writable returned False")

    async def test_1write(self):
        async with s3aioFileObject(
            self.cfg["STFC"]["url"] + "/buckettest/thefox2a.nc",
            credentials=self.cfg["STFC"]["credentials"],
            mode="w"
        ) as s3c:
            # create random bytes - if we keep it below s3c._getsize() then it will
            # only do one upload
            size = await s3c._getsize()
            bytes = bytearray(size)
            for b in range(0, size):
                bytes[b] = 128
            # convert bytes to io.BytesIO
            if await s3c.write(bytes) == 0:
                self.fail("s3aioFileObject.write returned zero")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    unittest.main()
    loop.close()
