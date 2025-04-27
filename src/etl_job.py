# Module 1: Spark Basics
# Improvements: Fix missing lat/lon using API, optimize join with broadcast

import os
import requests
import geohash2 as geohash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, trim, when, broadcast
from pyspark.sql.types import StringType

# Setup configuration
HOMEWORK_DATA_DIR = os.getenv("HOMEWORK_DATA_DIR", "wasbs://data@m06storageaccountbasic.blob.core.windows.net/m06sparkbasics/")
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY", "0996e7ca0ce24e3494fbb81a862c53d2")

if not os.getenv("AZURE_STORAGE_KEY"):
    raise ValueError("AZURE_STORAGE_KEY is not set! Please export before running.")

if not os.getenv("OPENCAGE_API_KEY"):
    raise ValueError("OPENCAGE_API_KEY is not set! Please export before running.")


# Reverse geocode function
def reverse_geocode(lat, lon):
    if not lat or not lon or not OPENCAGE_API_KEY:
        return None
    try:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={lat}+{lon}&key={OPENCAGE_API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            result = response.json()
            if result["results"]:
                return result["results"][0]["formatted"]
    except Exception as e:
        print(f"[API ERROR] Reverse geocode failed for lat={lat}, lon={lon}: {str(e)}")
    return None

# Generate geohash function
def generate_geohash(lat, lon):
    try:
        return geohash.encode(float(lat), float(lon), precision=4)
    except:
        return None

# UDFs
reverse_geocode_udf = udf(reverse_geocode, StringType())
geohash_udf = udf(generate_geohash, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL_Join_Hotels_Weather").getOrCreate()
    spark.conf.set(
        "fs.azure.account.key.stdevwesteuropemeim.blob.core.windows.net",
        os.getenv("AZURE_STORAGE_KEY")
    )

    # Paths
    weather_path = os.path.join(HOMEWORK_DATA_DIR, "weather")
    hotels_path = os.path.join(HOMEWORK_DATA_DIR, "hotels")
    enriched_output_path = os.path.join(HOMEWORK_DATA_DIR, "hotels_enriched")
    final_output_path = os.path.join(HOMEWORK_DATA_DIR, "joined")

    # Load data
    weather_df = spark.read.parquet(weather_path)
    hotels_df = spark.read.option("header", True).option("sep", ";").csv(hotels_path)

    # Clean hotels and fix missing lat/lon
    cleaned_hotels_df = hotels_df \
        .withColumn("Latitude", trim(col("Latitude"))) \
        .withColumn("Longitude", trim(col("Longitude")))

    hotels_fixed_df = cleaned_hotels_df \
        .withColumn("Latitude", when(col("Latitude").isNull() | (col("Latitude") == ""), reverse_geocode_udf(col("Latitude"), col("Longitude"))).otherwise(col("Latitude"))) \
        .withColumn("Longitude", when(col("Longitude").isNull() | (col("Longitude") == ""), reverse_geocode_udf(col("Latitude"), col("Longitude"))).otherwise(col("Longitude")))

    # Add Geohash and Formatted Address
    enriched_hotels_df = hotels_fixed_df \
        .withColumn("Geohash", geohash_udf("Latitude", "Longitude")) \
        .withColumn("FormattedAddress", reverse_geocode_udf("Latitude", "Longitude"))

    # Save enriched hotels
    enriched_hotels_df.write.mode("overwrite").parquet(enriched_output_path)

    # Process weather
    weather_df_short = weather_df.withColumn("Geohash", geohash_udf("lat", "lng"))

    # Optimized left join with broadcast (broadcast hotels dataset)
    joined_df = broadcast(enriched_hotels_df).join(weather_df_short, on="Geohash", how="left")

    # Save output partitioned by year, month, day
    joined_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(final_output_path)

    print(f"Enriched hotels saved to: {enriched_output_path}")
    print(f"Joined hotels + weather saved to: {final_output_path}")

    spark.stop()






