#Module 1: Spark Basics
#1. Check hotels data on incorrect (null) values (Latitude & Longitude). For incorrect values map (Latitude & Longitude) from OpenCage Geocoding API in job on fly (Via REST API).
#2. Generate geohash by Latitude & Longitude using one of geohash libraries (like geohash-java) with 4-characters length in extra column.
#3. Left join weather and hotels data by generated 4-characters geohash (avoid data multiplication and make you job idempotent)

import os
import requests  #calling the OpenCage reverse geocoding API (transform into real address) - hotels dataset has latitude/longitude, but no readable address.
import geohash2 as geohash  #Encodes latitude/longitude into geohash strings for joins
from pyspark.sql import SparkSession #Imports SparkSession
from pyspark.sql.functions import col, udf, trim #Imports common Spark SQL functions: col for selecting columns, udf for defining custom functions, trim to clean whitespace.
from pyspark.sql.types import StringType #Imports data type string used to define the return type of UDFs

#Corrections:
# Get config from environment variables or use Azure path for data files
HOMEWORK_DATA_DIR = os.getenv("HOMEWORK_DATA_DIR", "wasbs://data@m06storageaccountbasic.blob.core.windows.net/m06sparkbasics/") #Allows to choose between local and cloud environments.

# OPENCAGE_API_KEY set up in env variables $env:OPENCAGE_API_KEY=""

OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY") #changed, and also set in env variables: $env:OPENCAGE_API_KEY=""
spark.conf.set(
    "fs.azure.account.key.stdevwesteuropemeim.blob.core.windows.net",
    os.getenv("AZURE_STORAGE_KEY")
) # AZURE_STORAGE_KEY set up in env variables: $env:AZURE_STORAGE_KEY = ""

# Get formatted address using OpenCage (returns a readable address (e.g., "Zurich, Switzerland"))
def reverse_geocode(lat, lon):
    if not lat or not lon or not OPENCAGE_API_KEY:
        return None # Skips if any input is missing
    try:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={lat}+{lon}&key={OPENCAGE_API_KEY}" #OpenCage API request and replace with actual lat/ longitude
        response = requests.get(url)  #Sends the request
        if response.status_code == 200: # if request is successful to move forward
            result = response.json() #Converts the response to a dictionary
            if result["results"]:
                return result["results"][0]["formatted"] #Extracts the formatted address in the format "Zurich"
    except Exception as e: # except API errors
        print(f"[API ERROR] Reverse geocode failed for lat={lat}, lon={lon}: {str(e)}")
        return None #Returns none if address lookup fails

# Create 4-character geohash
def generate_geohash(lat, lon): #Encodes lat/lon into a 4-character geohash
    try:
        return geohash.encode(float(lat), float(lon), precision=4) #Converts GPS coordinates to a geohash (string), precision=4 appr. 20 km accuracy
    except:
        return None #Returns None if geohash can't be generated

# Convert python functions to spark df, string type column  
reverse_geocode_udf = udf(reverse_geocode, StringType()) #register goecode as spark function
geohash_udf = udf(generate_geohash, StringType())

if __name__ == "__main__": #run spark session, assign a name and don't start multiple sessions
    spark = SparkSession.builder.appName("ETL_Join_Hotels_Weather").getOrCreate()

    # input datasets (weather, hotels), output (hotels_enriched.parquet) and output (joined) after the join
    weather_path = os.path.join(HOMEWORK_DATA_DIR, "weather") #input path
    hotels_path = os.path.join(HOMEWORK_DATA_DIR, "hotels") #input path
    enriched_output_path = os.path.join(HOMEWORK_DATA_DIR, "hotels_enriched") 
    final_output_path = os.path.join(HOMEWORK_DATA_DIR, "joined")

    # Read input data from .csv and parquet files and create data frames
    # Read input data from .csv and parquet files and create data frames
    weather_df = spark.read.parquet(weather_path)
    hotels_df = spark.read.option("header", True).option("sep", ",").csv(hotels_path) #check separator, use ',' or ';' 

    # Clean hotels data - delete/ trims extra spaces and creates 'clean' data frame
    cleaned_hotels_df = hotels_df \
        .withColumn("Latitude", trim(col("Latitude"))) \
        .withColumn("Longitude", trim(col("Longitude"))) \
        .filter( #filter rows without blank/ missing values
            (col("Latitude").isNotNull()) &
            (col("Longitude").isNotNull()) &
            (trim(col("Latitude")) != "") &
            (trim(col("Longitude")) != "")
        )

    # Add columns to hotels 2 columns with geohash and readable city address names
    enriched_hotels_df = cleaned_hotels_df \
        .withColumn("Geohash", geohash_udf("Latitude", "Longitude")) \
        .withColumn("FormattedAddress", reverse_geocode_udf("Latitude", "Longitude"))

#write updated hotel data to parquet format, just in case replace old versions by overwrite
    enriched_hotels_df.write.mode("overwrite").parquet(enriched_output_path)

    # Adds a geohash column to the weather so it can be joined with hotels  
    weather_df_short = weather_df.withColumn("Geohash", geohash_udf("lat", "lng"))

    # Join hotels and weather by geohash with left join to keep all hotels and match weather
    joined_df = enriched_hotels_df.join(weather_df_short, on="Geohash", how="left")

    # Save output partitioned by year, month and day
    joined_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(final_output_path)

    print(f"Enriched hotels saved to: {enriched_output_path}")
    print(f"Joined hotels + weather saved to: {final_output_path}")

    spark.stop()





