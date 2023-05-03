gcloud dataproc jobs \
submit pyspark rollingagg-sparksubmit.py \
--cluster=pyspark-cluster \
--region us-central1 \
--jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar