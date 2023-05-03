# Part-1 : Data Extraction Job from Mysql to GCS 
gcloud dataproc jobs \
submit pyspark pyspark-mysql-extraction.py \
--cluster=mysql-spark-dataproc \
--region us-central1

# Part-2 : ETL Job
gcloud dataproc jobs \
submit pyspark pyspark-etl-pipeline.py \
--cluster=mysql-spark-dataproc \
--region us-central1