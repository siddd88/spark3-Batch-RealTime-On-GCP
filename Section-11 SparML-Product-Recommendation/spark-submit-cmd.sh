gcloud dataproc jobs \
submit pyspark streaming-recommendations.py \
--cluster=spark-streaming \
--region us-central1 \
--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar