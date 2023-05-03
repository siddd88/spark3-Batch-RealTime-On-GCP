from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType,TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import window
from pyspark.sql.functions import col,from_json

spark = SparkSession \
    .builder \
    .appName("sliding-window-agg") \
    .getOrCreate()

pubsub_subscription_path ="projects/44942850786/locations/us-central1/subscriptions/subscription_sliding_window"

schema = StructType([
    StructField("user_id",StringType(),False),
    StructField("session_id",StringType(),False),  
    StructField("ip_address", StringType(), False),
    StructField("browser", StringType(), False),
    StructField("traffic_source", StringType(),False),
    StructField("uri", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_date",StringType(),False)
  ])

df_streaming = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        pubsub_subscription_path
    )
    .load()
)

df = df_streaming.withColumn("data",df_streaming.data.cast(StringType()))
df_json = df.withColumn("json_data",from_json(col("data").cast("string"),schema))

df_final = df_json.select(
        [
         df_json.json_data.user_id.alias("user_id"),
         df_json.json_data.session_id.alias("session_id"),
         df_json.json_data.ip_address.alias("ip_address"),
         df_json.json_data.event_date.cast(TimestampType()).alias("event_date"),
         df_json.json_data.browser.alias("browser"),
         df_json.json_data.traffic_source.alias("traffic_source"),
         df_json.json_data.uri.alias("uri"),
         df_json.json_data.event_type.alias("event_type")
         ]
    )

df_sliding_window = df_final \
                    .groupBy(
                        window("event_date", "10 minutes","5 minutes"),
                        "user_id"
                    ) \
                    .agg(
                        F.count("session_id")
                    )

query = (
    df_sliding_window.writeStream \
    .outputMode("complete") \
    .option("truncate",False) \
    .format("console") \
    .start()
)

query.awaitTermination()
