from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType,TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,from_json

spark = SparkSession \
    .builder \
    .appName("read-app") \
    .getOrCreate()

pubsub_subscription_path =""

df_streaming = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        pubsub_subscription_path
    )
    .load()
)

df = df_streaming.withColumn("data",df_streaming.data.cast(StringType()))

query = (
    df.writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)
query.awaitTermination()