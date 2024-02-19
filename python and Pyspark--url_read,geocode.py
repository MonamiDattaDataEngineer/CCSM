>>>>>>>>>ITERATE THROUGH THE DATAFRAME USING PYSPARK?>>>>>>>>>

from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

# Assuming 'spark' is your SparkSession and 'df' is your DataFrame

# Create a Spark session
spark = SparkSession.builder.appName("DataFrameIteration").getOrCreate()

# Assuming 'df' is your PySpark DataFrame

# Iterate through rows in the DataFrame
for row in df.rdd.collect():
    pin = row['BILL_PIN']
    address = row['final_customer_address']
    state = row['STATE_DESC']
    district = row['DISTRICT'].upper()
    sub_dist = row['TEHSIL_DESC']
    village = row['village_name']

    # Print or process the extracted values as needed
    print('pin', pin)
    print('address', address)
    print('state', state)
    print('sub_dist', sub_dist)
    print('village', village)

# Stop the Spark session
spark.stop()





>>>>>>ITERATE THROUGH THE DATAFRAME USING PANDAS>>>>>>>>>

# Loop through the DataFrame and get the latitude and longitude for each address
for index, row in df.iterrows():
    pin = row['BILL_PIN']
    address = row['final_customer_address']
    state = row['STATE_DESC']
    district = row['DISTRICT'].upper()
    sub_dist= row['TEHSIL_DESC']
    village = row['village_name']
    # village = re.escape(village)
    
    print('pin',pin)
    print('address',address)
    print('state',state)
    print('sub_dist',sub_dist)
    print('village',village)
    
    
    -----------------------------------------------done---------------------------------------------------------------------
    
    
    
    
    
>>>>Geocoding URL operation in Python>>>>>>>>>>>>

 url = "https://maps.googleapis.com/maps/api/geocode/json?address={0}&components=country:IN|administrative_area:{1}|administrative_area:
 {2}|locality:{3}|locality:{4}&key={5}".format(address,state,district,sub_dist, village,API_KEY)


 print("print of geocoding URL", url)
        
 response = requests.get(url)
 data = json.loads(response.content)
 
 
 
 
 >>>>>>>Same in Pyspark using UDF>>>>>>>>
 
 ##Import PySpark and create a SparkSession.
 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GeocodingApp").getOrCreate()

##Define DataFrame:Create a DataFrame with the address details.

data = [("Your address", "Your state", "Your district", "Your sub dist", "Your village", "Your API key")]
columns = ["address", "state", "district", "sub_dist", "village", "api_key"]

df = spark.createDataFrame(data, columns)


##Define UDF (User Defined Function):Define a function that constructs the geocoding URL and makes the API request.

import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def geocode_udf(address, state, district, sub_dist, village, api_key):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address},{state},{district},{sub_dist},{village}&key={api_key}"
    response = requests.get(url)
    return response.content



##Apply UDF:Apply the UDF to the DataFrame to get the geocoding results.

result_df = df.withColumn("geocoding_result", geocode_udf("address", "state", "district", "sub_dist", "village", "api_key"))

##Show Results:Show the results.

result_df.show(truncate=False)

    