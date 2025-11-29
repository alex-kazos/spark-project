from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

df = spark.range(10)
df.show()

spark.stop()
