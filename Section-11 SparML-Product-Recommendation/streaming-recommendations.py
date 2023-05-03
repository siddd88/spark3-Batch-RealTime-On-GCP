from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType,IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,from_json
from pyspark.ml.recommendation import ALSModel
from google.cloud.pubsublite.cloudpubsub import PublisherClient

spark = SparkSession \
    .builder \
    .appName("read-app") \
    .getOrCreate()

model = ALSModel.load("gs://bucket-name/collabertive_filtering_artifact")

recommendations_df = model.recommendForAllUsers(10).select(
    col("user_id"),
    F.explode(col("recommendations")).alias("recommendation")
    ).select(
        col("user_id"),
        col("recommendation.product_id").alias("product_id")
    )

pubsub_subscription_path ="your-subscription-path"
output_topic = "your-topic-path"

schema = StructType([
    StructField("event_id",StringType(),False),
    StructField("user_id",StringType(),False),
    StructField("session_id",StringType(),False),
    StructField("browser", StringType(), False),
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

df_streaming_events = df_json.select(
        [
         df_json.json_data.user_id.cast(IntegerType()).alias("user_id")
         ]
    )

prediction_df = df_streaming_events.join(recommendations_df, "user_id", "left")
# prediction_df = model.recommendForUserSubset(df_final,10)

df_recommendations = prediction_df.groupBy("user_id").agg(F.collect_list("product_id").alias("products"))


def write_to_pubsub(batch_df,batch_id):
    records = batch_df.collect()
    with PublisherClient() as publisher_client:
        for row in records:
            message = row.asDict()
            message_bytes = str(message).encode("utf-8")
            api_future = publisher_client.publish(
                output_topic,
                data=message_bytes
            )

# df_recommendations \
#     .writeStream \
#     .foreachBatch(write_to_pubsub) \
#     .outputMode("complete") \
#     .start() \
#     .awaitTermination()

query = (
    df_recommendations.writeStream.format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start()
)
query.awaitTermination()