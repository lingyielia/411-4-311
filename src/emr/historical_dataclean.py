from pyspark.sql.types import (StructField, StringType,
                               IntegerType, StructType)

# define schema
data_schema  = [StructField('Unique Key', IntegerType(), True),
               StructField('Created Date', StringType(), True),
               StructField('Closed Date', StringType(), True),
               StructField('Agency', StringType(), True),
               StructField('Agency Name', StringType(), True),
               StructField('Complaint Type', StringType(), True),
               StructField('Descriptor', StringType(), True),
               StructField('Location Type', StringType(), True),
               StructField('Incident Zip', StringType(), True),
               StructField('Incident Address', StringType(), True),
               StructField('Street Name', StringType(), True),
               StructField('Cross Street 1', StringType(), True),
               StructField('Cross Street 2', StringType(), True),
               StructField('Intersection Street 1', StringType(), True),
               StructField('Intersection Street 2', StringType(), True),
               StructField('Address Type', StringType(), True),
               StructField('City', StringType(), True),
               StructField('Landmark', StringType(), True),
               StructField('Facility Type', StringType(), True),
               StructField('Status', StringType(), True),
               StructField('Due Date', StringType(), True),
               StructField('Resolution Description', StringType(), True),
               StructField('Resolution Action Updated Date', StringType(), True),
               StructField('Community Board', StringType(), True),
               StructField('BBL', StringType(), True),
               StructField('Borough', StringType(), True),
               StructField('X Coordinate (State Plane)', StringType(), True),
               StructField('Y Coordinate (State Plane)', StringType(), True),
               StructField('Open Data Channel Type', StringType(), True),
               StructField('Park Facility Name', StringType(), True),
               StructField('Park Borough', StringType(), True),
               StructField('Vehicle Type', StringType(), True),
               StructField('Taxi Company Borough', StringType(), True),
               StructField('Taxi Pick Up Location', StringType(), True),
               StructField('Bridge Highway Name', StringType(), True),
               StructField('Bridge Highway Direction', StringType(), True),
               StructField('Road Ramp', StringType(), True),
               StructField('Bridge Highway Segment', StringType(), True),
               StructField('Latitude', StringType(), True),
               StructField('Longitude', StringType(), True),
               StructField('Location', StringType(), True)]
final_struc = StructType(fields = data_schema)

# read in the original dataset
df = spark.read.csv("s3://nyc311forinsight/test/311_Service_Requests_from_2010_to_Present.csv",
                    header=True, schema=final_struc)

# only keep useful columns and fill null values
df_short = df.select('Agency', 'Closed Date', 'Complaint Type', 'Created Date',
                     'Latitude', 'Longitude', 'Open Data Channel Type')
df_short = df_short.fillna({'Agency':'unknown', 'Closed Date':'2050-01-10T04:08:32.000', 'Complaint Type':'unknown',
                            'Created Date':'2000-09-14T04:08:32.000', 'Latitude':'20.86125849849244',
                            'Longitude':'-23.92566793186856', 'Open Data Channel Type':'unknown'})

# write to s3
df_short.write.save("s3://nyc311forinsight/cleaned_311_ny.csv", format='csv', header=True)

# write to rds postgres
df_short.write\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://testdbins.cmbnhjkwvaqz.us-east-1.rds.amazonaws.com:5432/db311")\
    .option("driver", "org.postgresql.Driver")\
    .option("truncate", "true")\
    .option("fetchsize", 1000)\
    .option("batchsize", 100000)\
    .option("dbtable", "events_test")\
    .option("user", "lz_db_user")\
    .option("password", "teawhalefortest") \
    .mode('overwrite')\
    .save()
