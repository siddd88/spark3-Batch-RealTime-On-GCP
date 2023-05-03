gcloud beta dataproc workflow-templates create mysql-spark-template --region us-central1

gcloud dataproc workflow-templates \
    set-cluster-selector mysql-spark-template \
    --region=us-central1 \
    --cluster-labels=goog-dataproc-cluster-name=mysql-spark-dataproc

gcloud dataproc workflow-templates add-job pyspark gs://pyspark-fs-sid/spark-jobs/pyspark-mysql-extraction.py \
    --region=us-central1 \
    --step-id=mysql-data-extraction \
    --workflow-template=mysql-spark-template

gcloud dataproc workflow-templates add-job pyspark gs://pyspark-fs-sid/spark-jobs/pyspark-etl-pipeline.py \
    --region=us-central1 \
    --step-id=data-transformation-job \
    --start-after=mysql-data-extraction \
    --workflow-template=mysql-spark-template

gcloud dataproc workflow-templates instantiate mysql-spark-template --region us-central1