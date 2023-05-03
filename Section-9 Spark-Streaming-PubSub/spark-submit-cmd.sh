gcloud dataproc jobs \
submit pyspark test-streams.py \
--cluster=spark-streaming \
--region us-central1 \
--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar


# Tumbling Window Aggregates
gcloud dataproc jobs \
submit pyspark tumbling-window.py \
--cluster=spark-streaming \
--region us-central1 \
--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar


# Tumbling Window Watermarked Aggregates
gcloud dataproc jobs \
submit pyspark tumbling-window-watermark.py \
--cluster=spark-streaming \
--region us-central1 \
--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar


# Sliding Window Aggregates
gcloud dataproc jobs \
submit pyspark sliding-window.py \
--cluster=spark-streaming \
--region us-central1 \
--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar