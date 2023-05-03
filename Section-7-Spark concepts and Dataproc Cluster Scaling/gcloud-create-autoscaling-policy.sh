gcloud dataproc autoscaling-policies import dataproc-autoscaling-pcy \
    --source=autoscaling-policy.yaml \
    --region=us-central1


gcloud dataproc clusters update pyspark-cluster \
    --autoscaling-policy=dataproc-autoscaling-pcy \
    --region=us-central1