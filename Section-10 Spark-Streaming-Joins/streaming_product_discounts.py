from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,from_json

spark = SparkSession \
    .builder \
    .appName("product-discount-job") \
    .getOrCreate()

df_user_discounts = spark.read.csv("gs://bucket-name/user_product_discounts.csv",header=True)

pubsub_subscription_path ="your-topic-subscription-path"

streaming_df_schema = StructType([
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
df_json = df.withColumn("json_data",from_json(col("data").cast("string"),streaming_df_schema))

df_events = df_json.select(
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

df_joined = df_events \
            .join(df_user_discounts,df_events.user_id==df_user_discounts.user_id,"inner") \
            .select(
                df_user_discounts.user_id,
                df_user_discounts.product_id,
                df_user_discounts.sales_price,
                df_user_discounts.discounted_price
             ) 


query = ( df_joined.writeStream
  .format("csv")
  .option("checkpointLocation", "/tmp/product-discount/")
  .option("path", "gs://bucket-name/streaming_output")
  .trigger(processingTime='10 second')
  .outputMode("append")
  .start()
)

# query = (
#     df_joined.writeStream.format("console")
#     .outputMode("append")
#     .option("truncate", "false")
#     .start()
# )

query.awaitTermination()