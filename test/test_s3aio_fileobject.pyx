from S3netCDF4.Backends._s3aioFileObject import s3aioFileObject
import asyncio
import time

# test caringo asyncio

async def async_write(fname, buf):
    async with s3aioFileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/"+fname,
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
        ) as car:
        await car.write(buf)

async def test_asyncio_write():
    in_fh = open("/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1991.2000.tmp.dat.nc", mode='rb')
    buf = in_fh.read()
    in_fh.close()

    start=time.time()
    task1 = asyncio.create_task(async_write("thefoxb1.nc", buf))
    task2 = asyncio.create_task(async_write("thefoxb2.nc", buf))
    task3 = asyncio.create_task(async_write("thefoxb3.nc", buf))
    task4 = asyncio.create_task(async_write("thefoxb4.nc", buf))
    task5 = asyncio.create_task(async_write("thefoxb5.nc", buf))

    await task1
    await task2
    await task3
    await task4
    await task5

    end=time.time()
    print(end-start)

async def async_read(fname):
    async with s3aioFileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/"+fname,
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="r"
        ) as car:
        buf = await car.read()
    return buf

async def test_asyncio_read():
    start=time.time()
    task1 = asyncio.create_task(async_read("thefoxb1.nc"))
    task2 = asyncio.create_task(async_read("thefoxb2.nc"))
    task3 = asyncio.create_task(async_read("thefoxb3.nc"))
    task4 = asyncio.create_task(async_read("thefoxb4.nc"))
    task5 = asyncio.create_task(async_read("thefoxb5.nc"))

    x = await task1
    y = await task2
    z = await task3
    a = await task4
    b = await task5
    end=time.time()
    print(end-start)

#asyncio.run(test_asyncio_write())
asyncio.run(test_asyncio_read())
