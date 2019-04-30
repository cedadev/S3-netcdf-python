from S3netCDF4.Managers._FileManager import FileManager

fm = FileManager()
#fh = fm.open("s3://cedadev-o/buckettest/thefox.txt", mode="r")
fh = fm.open("/cedadev-o/buckettest/thefox.txt", mode="r")
print(fh.readlines())
fh.close()
