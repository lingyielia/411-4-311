from pyspark.sql.types import (StructField, StringType,
                               IntegerType, StructType)

# define schema
data_schema  = [StructField('address_type', StringType(), True),
               StructField('agency', StringType(), True),
               StructField('agency_name', StringType(), True),
               StructField('bbl', IntegerType(), True),
               StructField('borough', StringType(), True),
               StructField('bridge_highway_direction', StringType(), True),
               StructField('bridge_highway_name', StringType(), True),
               StructField('bridge_highway_segment', StringType(), True),
               StructField('city', StringType(), True),
               StructField('closed_date', StringType(), True),
               StructField('community_board', StringType(), True),
               StructField('complaint_type', StringType(), True),
               StructField('created_date', StringType(), True),
               StructField('cross_street_1', StringType(), True),
               StructField('cross_street_2', StringType(), True),
               StructField('descriptor', StringType(), True),
               StructField('due_date', StringType(), True),
               StructField('facility_type', StringType(), True),
               StructField('incident_address', StringType(), True),
               StructField('incident_zip', StringType(), True),
               StructField('intersection_street_1', StringType(), True),
               StructField('intersection_street_2', StringType(), True),
               StructField('landmark', StringType(), True),
               StructField('latitude', StringType(), True),
               StructField('location', StringType(), True),
               StructField('location_type', StringType(), True),
               StructField('longitude', StringType(), True),
               StructField('open_data_channel_type', StringType(), True),
               StructField('park_borough', StringType(), True),
               StructField('park_facility_name', StringType(), True),
               StructField('resolution_action_updated_date', StringType(), True),
               StructField('resolution_description', StringType(), True),
               StructField('road_ramp', StringType(), True),
               StructField('status', StringType(), True),
               StructField('street_name', StringType(), True),
               StructField('taxi_company_borough', StringType(), True),
               StructField('taxi_pick_up_location', StringType(), True),
               StructField('unique_key', StringType(), True),
               StructField('vehicle_type', StringType(), True),
               StructField('x_coordinate_state_plane', StringType(), True),
               StructField('y_coordinate_state_plane', StringType(), True)]
final_struc = StructType(fields = data_schema)

# read in the original dataset
df = spark.read.csv("s3://nyc311forinsight/test/311_Service_Requests_from_2010_to_Present.csv",
                    header=True, schema=final_struc)

# only keep useful columns
df_short = df.select('agency', 'closed_date', 'complaint_type', 'created_date',
                     'latitude', 'longitude', 'open_data_channel_type')

# write to s3
df_short.write.save("s3://nyc311forinsight/cleaned_311_ny.csv", format='csv', header=True)
