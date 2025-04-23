import os
import requests
import geohash2 as geohash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, trim
from pyspark.sql.types import StringType

# Get config from environment
HOMEWORK_DATA_DIR = os.getenv("HOMEWORK_DATA_DIR", "gs://storage-bucket-polished-owl/data/m06sparkbasics/")
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY")

# UDF: Get formatted address using OpenCage
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

# UDF: Create 4-character geohash
def generate_geohash(lat, lon):
    try:
        return geohash.encode(float(lat), float(lon), precision=4)
    except:
        return None

# Register UDFs
reverse_geocode_udf = udf(reverse_geocode, StringType())
geohash_udf = udf(generate_geohash, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL_Join_Hotels_Weather").getOrCreate()

    # Input/output paths
    weather_path = os.path.join(HOMEWORK_DATA_DIR, "weather")
    hotels_path = os.path.join(HOMEWORK_DATA_DIR, "hotels")
    enriched_output_path = os.path.join(HOMEWORK_DATA_DIR, "hotels_enriched.parquet")
    final_output_path = os.path.join(HOMEWORK_DATA_DIR, "joined")

    # Read input data from GCS
    weather_df = spark.read.option("basePath", weather_path).parquet(weather_path)
    hotels_df = spark.read.option("header", True).csv(hotels_path)

    # Clean hotels data
    cleaned_hotels_df = hotels_df \
        .withColumn("Latitude", trim(col("Latitude"))) \
        .withColumn("Longitude", trim(col("Longitude"))) \
        .filter(
            (col("Latitude").isNotNull()) &
            (col("Longitude").isNotNull()) &
            (trim(col("Latitude")) != "") &
            (trim(col("Longitude")) != "")
        )

    # Enrich hotels
    enriched_hotels_df = cleaned_hotels_df \
        .withColumn("Geohash", geohash_udf("Latitude", "Longitude")) \
        .withColumn("FormattedAddress", reverse_geocode_udf("Latitude", "Longitude"))

    enriched_hotels_df.write.mode("overwrite").parquet(enriched_output_path)

    # Enrich weather with geohash
    weather_df_short = weather_df.withColumn("Geohash", geohash_udf("lat", "lng"))

    # Join on geohash
    joined_df = enriched_hotels_df.join(weather_df_short, on="Geohash", how="left")

    # Save joined output partitioned (optional but encouraged)
    joined_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(final_output_path)

    print(f"[OK] Enriched hotels saved to: {enriched_output_path}")
    print(f"[OK] Joined hotels + weather saved to: {final_output_path}")

    spark.stop()





