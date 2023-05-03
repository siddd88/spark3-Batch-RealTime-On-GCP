# Streaming product discount job 
gcloud dataproc jobs \
submit pyspark streaming_product_discounts.py \
--cluster=spark-streaming \
--region us-central1 \
--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar


# Popular product recommendation job 
gcloud dataproc jobs \
submit pyspark popular_products_recommendation.py \
--cluster=spark-streaming \
--region us-central1 \
--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar