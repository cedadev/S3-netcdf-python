from S3netCDF4.Backends._s3FileObject import s3FileObject
from S3netCDF4._Exceptions import IOException, APIException
import time
import json

def create_file_object():
    # load the credentials from the hidden file
    fh = open(".s3config.json")
    cfg = json.load(fh)
    fh.close()
    s3c = s3FileObject(
        cfg["url"] + "/buckettest/speed_test_file_object.bin",
        credentials=cfg["credentials"],
        mode="w"
    )
    return s3c

def create_test_data(s3c):
    size = 10 * s3c._getsize()  # do at least ten multipart uploads
    bytes = bytearray(size)
    # for b in range(0, size):
    #     bytes[b] = 128
    return bytes

def write_test_data(s3c, data):
    s3c.connect()
    s3c.write(data)
    s3c.close()

if __name__ == "__main__":
    s3c = create_file_object()
    bytes = create_test_data(s3c)
    start_time = time.time()
    write_test_data(s3c, bytes)
    end_time = time.time()
    print(end_time - start_time)
