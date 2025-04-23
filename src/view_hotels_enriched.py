from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Preview Enriched Hotels") \
    .getOrCreate()

# Adjust this path if needed
path = "C:/Users/HP/Documents/M06_SparkBasics_PYTHON_AZURE/output/hotels_enriched.parquet"

df = spark.read.parquet(path)
df.show(10, truncate=False)

spark.stop()
