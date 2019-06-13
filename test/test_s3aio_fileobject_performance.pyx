from S3netCDF4.Backends._s3aioFileObject import s3aioFileObject
from S3netCDF4._Exceptions import IOException, APIException
import time
import json
import asyncio

def create_write_file_object():
    # load the credentials from the hidden file
    fh = open(".s3config.json")
    cfg = json.load(fh)
    fh.close()
    s3c = s3aioFileObject(
        cfg["url"] + "/buckettest/speed_test_aio_file_object.bin",
        credentials=cfg["credentials"],
        mode="w"
    )
    return s3c

def create_read_file_object():
    # load the credentials from the hidden file
    fh = open(".s3config.json")
    cfg = json.load(fh)
    fh.close()
    s3c = s3aioFileObject(
        cfg["url"] + "/buckettest/speed_test_aio_file_object.bin",
        credentials=cfg["credentials"],
        mode="r",
        multipart_download = False
    )
    return s3c

async def create_test_data(s3c):
    fsize = await s3c._getsize()
    size = 10 * fsize + int(0.4 * fsize) # do at least ten multipart uploads
    bytes = bytearray(size)
    bytes[237] = 89
    bytes[238] = 0
    bytes[fsize-1] = 23
    # for b in range(0, size):
    #     bytes[b] = 128
    return bytes, fsize

async def write_test_data(s3c, data):
    await s3c.connect()
    await s3c.write(data)
    await s3c.close()

async def read_test_data(s3c):
    await s3c.connect()
    data = await s3c.read()
    await s3c.close()
    return data

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    s3c = create_write_file_object()

    # bytes, fsize = loop.run_until_complete(create_test_data(s3c))
    # print(fsize)
    # start_time = time.time()
    # loop.run_until_complete(write_test_data(s3c, bytes))
    # end_time = time.time()
    # print(end_time - start_time)

    fsize = 52428800
    s3cr = create_read_file_object()
    start_time = time.time()
    bytes = loop.run_until_complete(read_test_data(s3cr))
    end_time = time.time()
    print(end_time - start_time)
    print(bytes[237], bytes[238], bytes[fsize-1])
