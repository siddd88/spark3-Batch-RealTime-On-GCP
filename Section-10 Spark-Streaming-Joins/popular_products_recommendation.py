from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,from_json
from pyspark.sql.functions import window

spark = SparkSession \
    .builder \
    .appName("popular-products-recommendation") \
    .getOrCreate()

purchase_events_subscription_path ="your-subscription-path"
browsing_events_subscription_path ="your-subscription-path"

browsing_events_schema = StructType([
    StructField("user_id",StringType(),False),
    StructField("session_id",StringType(),False),
    StructField("ip_address",StringType(),False),
    StructField("browser",StringType(),False),  
    StructField("traffic_source", StringType(), False),
    StructField("uri", StringType(), False),
    StructField("event_type", StringType(),False),
    StructField("event_date", StringType(),False),
    StructField("product_id", StringType(),False)
  ])

products_purchased_schema = StructType([
    StructField("event_date",StringType(),False),
    StructField("product_id",StringType(),False),  
    StructField("user_id", StringType(), False)
  ])

df_events_streaming = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        browsing_events_subscription_path
    )
    .load()
)

df_purchase_streaming = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        purchase_events_subscription_path
    )
    .load()
)

# Streaming browsing events Dataframe
df1 = df_events_streaming.withColumn("data",df_events_streaming.data.cast(StringType()))
df_json1 = df1.withColumn("json_data",from_json(col("data").cast("string"),browsing_events_schema))

df_browsing_events = df_json1.select(
        [
         df_json1.json_data.user_id.alias("user_id"),
         df_json1.json_data.product_id.alias("product_id"),
         df_json1.json_data.event_type.alias("event_type"),
         df_json1.json_data.event_date.cast(TimestampType()).alias("event_date"),
         df_json1.json_data.uri.alias("uri")
         ]
    ).withWatermark("event_date", "2 minutes")

# Streaming purchase events Dataframe 
df2 = df_purchase_streaming.withColumn("data",df_purchase_streaming.data.cast(StringType()))
df_json2 = df2.withColumn("json_data",from_json(col("data").cast("string"),products_purchased_schema))

df_purchase_events = df_json2.select(
        [
         df_json2.json_data.event_date.cast(TimestampType()).alias("event_date"),
         df_json2.json_data.product_id.alias("product_id"),
         df_json2.json_data.user_id.alias("user_id")
         ]
    ).withWatermark("event_date", "2 minutes")

# Join both streaming dfs
df_joined = df_browsing_events \
            .join(df_purchase_events,df_browsing_events.product_id==df_purchase_events.product_id,"inner") \
            .select(
                df_browsing_events.user_id.alias("visiting_user_id"),
                df_purchase_events.user_id.alias("purchased_user_id"),
                df_purchase_events.product_id,
                df_purchase_events.event_date
             )

# Perform windowed aggregation to get the count of users who have purchased a product 
df_final_aggregated = df_joined \
                    .groupBy(
                        window("event_date","30 seconds","10 seconds"),
                        "product_id",
                        "visiting_user_id"
                    ) \
                    .agg(
                        F.approx_count_distinct("purchased_user_id").alias("total_users_buying")
                    )

def write_to_sink(df, epoch_id):
    # Check if dataframe is not empty
    if not df.rdd.isEmpty():
        df.write.format("json") \
        .option("checkpointLocation", "/tmp/popular-products/") \
        .save("gs://bucket-name/popular-products-recommendation/")

df_final_aggregated.writeStream.foreachBatch(write_to_sink).start().awaitTermination()

# query = (
#     df_final_aggregated.writeStream.format("console")
#     .trigger(processingTime='40 seconds')
#     .option("truncate", "false")
#     .outputMode("append")
#     .start()
# )

# query.awaitTermination()