from S3netCDF4.Backends._s3FileObject import s3FileObject
import netCDF4

def _get_netCDF_filetype(input_bytes):
    """
       Interpret the magic number for netCDF type in input_bytes.
       This should be 4 bytes that can be obtained from using s3FileObject.read()
       See NC_interpret_magic_number in netcdf-c/libdispatch/dfile.c

       Check that it is a netCDF file before fetching any data and
       determine what type of netCDF file it is so the temporary empty file can
       be created with the same type.

       The possible types are:
       `NETCDF3_CLASSIC`, `NETCDF4`,`NETCDF4_CLASSIC`, `NETCDF3_64BIT_OFFSET` or `NETCDF3_64BIT_DATA`
       or
       `NOT_NETCDF` if it is not a netCDF file - raise an exception on that

       :return: string filetype
    """
    input_string = input_bytes.decode('utf8','replace').strip()
    # start with NOT_NETCDF as the file_type
    file_version = 0
    file_type = 'NOT_NETCDF'
    # check whether it's a netCDF file (how can we tell if it's a NETCDF4_CLASSIC file?
    if input_string[1:5] == 'HDF':
        # netCDF4 (HD5 version)
        file_type = 'NETCDF4'
        file_version = 5
    elif (input_string[0] == '\016' and
          input_string[1] == '\003' and
          input_string[2] == '\023' and
          input_string[3] == '\001'):
        file_type = 'NETCDF4'
        file_version = 4
    elif input_string[0:3] == 'CDF':
        file_version = ord(input_string[3])
        if file_version == 1:
            file_type = 'NETCDF3_CLASSIC'
        elif file_version == '2':
            file_type = 'NETCDF3_64BIT_OFFSET'
        elif file_version == '5':
            file_type = 'NETCDF3_64BIT_DATA'
        else:
            file_version = 1 # default to one if no version
    else:
        file_type = 'NOT_NETCDF'
        file_version = 0
    return file_type, file_version

# with s3FileObject(
#         "http://130.246.129.81:9000/databuckettest/testnctest.nc",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="w") as x:
#     x.read()
#
# with s3FileObject(
#         "http://130.246.129.81:9000/databuckettest/testnctest.nc",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="w") as x:
#     print(x.readable())
#     x.read()

# test using two separate connections
# x = s3FileObject(
#         "http://130.246.129.81:9000/databuckettest/testnctest.nc",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="r")
# print (x.readable())
# xd = x.read()
# print(type(xd))
#temp_file = netCDF4.Dataset("./test.nc", 'w').close()
#nc = netCDF4.Dataset("./test.nc", memory=xd)
#print(nc)

# y = s3FileObject(
#         "http://130.246.129.81:9000/databuckettest/testnctest.nc",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="r")
# print (y.readable())
# y.read()

# test identifying a remote filetype using subset

# y = s3FileObject(
#         "http://130.246.129.81:9000/databuckettest/testnctest.nc",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="r"
#     )
#
# input_bytes = y.read(size=4)
# format = _get_netCDF_filetype(input_bytes)
# y.seek(0)
# input_bytes = y.read()
# tmp_file = "./test.nc"
# temp_file = netCDF4.Dataset(
#     tmp_file,
#     'w',
#     format=format[0],
#     clobber=True
# ).close()
#
# nc = netCDF4.Dataset(tmp_file, mode='r', memory=input_bytes, diskless=True)
# #nc = netCDF4.Dataset(tmp_file)
# #print(nc)
#
# # test read into
# ba = bytearray()
# n_read = y.readinto(ba)
# #print(n_read)
# nc = netCDF4.Dataset(tmp_file, mode='r', memory=ba, diskless=True)
# #print(nc)
# nc.close()

# # test short write
# y = s3FileObject(
#         "http://130.246.129.81:9000/databuckettest/testwrite.txt",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="w"
#     )
# y.write("Hello and welcome.".encode('utf-8'))
# y.write("\nBienvenue and Wilkommen.".encode('utf-8'))
# y.close()

# # test multipart (large) write
# z = s3FileObject(
#         "http://130.246.129.81:9000/databuckettest/testwrite_big.txt",
#         credentials=["accessKey":"WTL17W3P2K3C7IYVX4W9",
#                      "secretKey":"VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9"
#                     ],
#         mode="w"
#     )
#
# for a in range(0, int(1e6)):
#     ba = bytearray('AAAAAA'.encode('utf-8'))
#     z.write(ba)

# testing destructor here by not calling close

# test write to bucket that doesn't exist
# y = s3FileObject(
#         "http://130.246.129.81:9000/buckettest/testnowrite.txt",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="w"
#     )
# y.write("Does not exist".encode('utf-8'))
# y.write("\nDoes not exist.".encode('utf-8'))
# y.close()

# test writeline
# lines = ['The quick', 'brown fox', 'jumped over', 'the lazy', 'red hen']
# y = s3FileObject(
#         "http://130.246.129.81:9000/buckettest/thefox.txt",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="w"
#     )
# y.writelines(lines)
# y.close()
#
# # test readline
# y = s3FileObject(
#         "http://130.246.129.81:9000/buckettest/thefox.txt",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="r"
#     )
# print(y.readlines())
# y.close()

# test readline
# y = s3FileObject(
#         "http://130.246.129.81:9000/buckettest/thefox.txt",
#         access_key="WTL17W3P2K3C7IYVX4W9",
#         secret_key="VUcT86fJFF0XTPtcrsnjUnvtM7Wj1N3cb9mALRZ9",
#         mode="r",
#         buffer_size=10
#     )
# for line in y:
#     print(line)
# y.close()

# test caringo


def test_write():
    in_fh = open("/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.1991.2000.tmp.dat.nc", mode='rb')
    buf = in_fh.read()
    in_fh.close()

    import time

    start=time.time()

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa1.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car:
        car.write(buf)

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa2.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car2:
        car2.write(buf)

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa3.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car3:
        car3.write(buf)

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa4.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car4:
        car4.write(buf)

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa5.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car5:
        car5.write(buf)

    end=time.time()
    print(end-start)

def test_read():

    import time

    start=time.time()
    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa1.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="r"
         ) as car:
        buff = car.read()

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa2.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car2:
        buff2 = car2.read()

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa3.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="r"
         ) as car3:
        buff3 = car3.read()

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa4.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car4:
        buff4 = car4.read()

    with s3FileObject(
             "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefoxa5.nc",
             credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
                          "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
                         },
             mode="w"
         ) as car5:
        buff5 = car5.read()

    end=time.time()
    print(end-start)

test_read()

# car_read = s3FileObject(
#         "http://cedadev-o.s3.jc.rl.ac.uk/buckettest/thefox.txt",
#         credentials={"accessKey":"266e98d367a13ba66e940250e7f1f23f",
#                      "secretKey":"yhVxDkTAHihjApn9xxSxfA9GJxngs1xqSragIIGn"
#                     },
#         mode="r"
#     )
#car_read.connect()
#lines = car_read.readlines()
#car_read.write(b'12')
#print(lines)
