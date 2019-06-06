from S3netCDF4.Backends._s3FileObject import s3FileObject
from S3netCDF4._Exceptions import IOException, APIException
import unittest
import json
import io

"""To run the tests, you need to create a .s3config.json file in the same
directory as these tests.  This file should contain:
{
    "url": "<url of s3 service>",
    "credentials": {
        "accessKey": "<your access key>",
        "secretKey": "<your secret key>"
    }
}
"""

class s3FileObjectGeneralTest(object):
    """All of the general tests for either a read or write transaction."""

    def tearDown(self):
        self.s3c.close()
        self.s3c_lines.close()

    def test_connect(self):
        self.assertTrue(self.s3c.connect())

    def test_detach(self):
        self.assertTrue(self.s3c.connect())
        self.assertRaises(io.UnsupportedOperation, self.s3c.detach)

    def test_close(self):
        self.assertTrue(self.s3c.connect())
        self.assertTrue(self.s3c.close())

    def test_readable(self):
        self.assertTrue(self.s3c.connect())
        self.assertTrue(self.s3c.readable())

    def test_truncate(self):
        self.assertTrue(self.s3c.connect())
        self.assertRaises(io.UnsupportedOperation, self.s3c.truncate)

    def test_fileno(self):
        self.assertTrue(self.s3c.connect())
        self.assertRaises(io.UnsupportedOperation, self.s3c.fileno)

    def test_seekable(self):
        self.assertTrue(self.s3c.connect())
        self.assertTrue(self.s3c.seekable())

    def test_tell(self):
        self.assertTrue(self.s3c.connect())
        self.assertEqual(self.s3c.tell(), 0)

    def test_seek(self):
        self.assertTrue(self.s3c.connect())
        # Three different methods for seek:
        #   whence = io.SEEK_SET
        #   whence = io.SEEK_CUR
        #   whence = io.SEEK_END
        # the current pointer is on zero
        self.assertEqual(0, self.s3c.seek(0, whence=io.SEEK_SET))
        self.assertEqual(10, self.s3c.seek(10, whence=io.SEEK_SET))
        # now on 10
        with self.assertRaises(IOException) as contx:
            self.s3c.seek(-1, whence=io.SEEK_SET)
        # failed so still on 10

        # the current pointer is on ten (10)
        self.assertEqual(0, self.s3c.seek(-10, whence=io.SEEK_CUR))
        # now on 0 - should raise an exception if we seek below 0
        with self.assertRaises(IOException):
            self.s3c.seek(-1, whence=io.SEEK_CUR)
        # still on zero: get the size to seek past it
        size = self.s3c._getsize()
        with self.assertRaises(IOException):
            self.s3c.seek(size+1, whence=io.SEEK_CUR)

        # still on zero - seek from the end
        with self.assertRaises(IOException):
            self.s3c.seek(size+1, whence=io.SEEK_END)
        # still on 0 - seek backwards from the end
        with self.assertRaises(IOException):
            self.s3c.seek(-1, whence=io.SEEK_END)
        # seek just a normal amount from the end
        self.assertEqual(size-10, self.s3c.seek(10, whence=io.SEEK_END))


class s3t1FileObjectWriteTest(unittest.TestCase, s3FileObjectGeneralTest):

    def setUp(self):
        """Set up the s3FileObject but don't connect."""
        # load the credentials from the hidden file
        fh = open(".s3config.json")
        cfg = json.load(fh)
        fh.close()
        self.s3c = s3FileObject(
            cfg["url"] + "/buckettest/thefox1a.nc",
            credentials=cfg["credentials"],
            mode="w"
        )

        # for writing with the write line methods
        self.s3c_lines = s3FileObject(
            cfg["url"] + "/buckettest/thefox1b.txt",
            credentials=cfg["credentials"],
            mode="w"
        )

    def test_seek(self):
        with self.assertRaises(IOException):
            self.s3c.seek(0)

    def test_readable(self):
        self.assertTrue(self.s3c.connect())
        self.assertFalse(self.s3c.readable())

    def test_writable(self):
        self.assertTrue(self.s3c.connect())
        self.assertTrue(self.s3c.writable())

    def test_write(self):
        self.assertTrue(self.s3c.connect())
        # create random bytes - if we keep it below s3c._getsize() then it will
        # only do one upload
        size = self.s3c._getsize()
        bytes = bytearray(size)
        for b in range(0, size):
            bytes[b] = 128
        self.assertNotEqual(0, self.s3c.write(bytes))

    def test_write_multipart(self):
        self.assertTrue(self.s3c.connect())
        # create random bytes - if we make it above 3c._getsize() then it will
        # do a multipart upload
        size = 3 * self.s3c._getsize()
        bytes = bytearray(size)
        for b in range(0, size):
            bytes[b] = 128
        self.assertNotEqual(0, self.s3c.write(bytes))

    def test_write_lines(self):
        self.assertTrue(self.s3c_lines.connect())
        lines = ["The","quick","brown","fox","jumped",
                 "over","the","lazy","red","hen"]
        self.assertTrue(self.s3c_lines.writelines(lines))


class s3t2FileObjectReadTest(unittest.TestCase, s3FileObjectGeneralTest):

    def setUp(self):
        """Set up the s3FileObject but don't connect."""
        # load the credentials from the hidden file
        fh = open(".s3config.json")
        cfg = json.load(fh)
        fh.close()
        self.s3c = s3FileObject(
            cfg["url"] + "/buckettest/thefox1a.nc",
            credentials=cfg["credentials"],
            mode="r"
        )

        self.s3c_lines = s3FileObject(
            cfg["url"] + "/buckettest/thefox1b.txt",
            credentials=cfg["credentials"],
            mode="r"
        )

    def test_writable(self):
        self.assertTrue(self.s3c.connect())
        self.assertFalse(self.s3c.writable())

    def testread(self):
        self.assertTrue(self.s3c.connect())
        self.assertNotEqual(0, len(self.s3c.read()))

    def testreadrange(self):
        self.assertTrue(self.s3c.connect())
        self.assertEqual(1024, len(self.s3c.read(size=1024)))
        self.assertNotEqual(0, len(self.s3c.read(size=1024)))

    def testreadinto(self):
        buffer = bytearray()
        self.assertTrue(self.s3c.connect())
        self.assertNotEqual(0, self.s3c.readinto(buffer))
        self.assertNotEqual(0, len(buffer))

    def testreadline(self):
        self.assertTrue(self.s3c_lines.connect())
        self.s3c_lines.seek(0)
        self.assertNotEqual(0, len(self.s3c_lines.readline()))
        self.s3c_lines.seek(0)
        self.assertNotEqual(0, len(self.s3c_lines.readlines()))

if __name__ == '__main__':
    unittest.main()
