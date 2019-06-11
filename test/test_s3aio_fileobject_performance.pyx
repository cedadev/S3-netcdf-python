from S3netCDF4.Backends._s3aioFileObject import s3aioFileObject
from S3netCDF4._Exceptions import IOException, APIException
import time
import json
import asyncio

def create_file_object():
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

async def create_test_data(s3c):
    size = 10 * await s3c._getsize()  # do at least ten multipart uploads
    bytes = bytearray(size)
    # for b in range(0, size):
    #     bytes[b] = 128
    return bytes

async def write_test_data(s3c, data):
    await s3c.connect()
    await s3c.write(data)
    await s3c.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    s3c = create_file_object()

    bytes = loop.run_until_complete(create_test_data(s3c))
    start_time = time.time()
    loop.run_until_complete(write_test_data(s3c, bytes))
    end_time = time.time()
    print(end_time - start_time)
