gcloud dataproc clusters create mysql-etl \
    --image-version=2.1-ubuntu20 \
    --region=us-central1 \
    --enable-component-gateway \
    --num-masters=1 \
    --master-machine-type=n2-standard-2 \
    --worker-machine-type=n2-standard-2 \
    --master-boot-disk-size=30GB \
    --worker-boot-disk-size=30GB \
    --num-workers=2 \
    --initialization-actions=gs://pyspark-fs-sid/install.sh \
    --metadata 'PIP_PACKAGES=mysql-connector-python' \
    --optional-components=JUPYTER

gcloud dataproc clusters create mysql-etl \
    --image-version=2.1-ubuntu20 \
    --region=us-central1 \
    --enable-component-gateway \
    --num-masters=1 \
    --master-machine-type=n2-standard-2 \
    --worker-machine-type=n2-standard-2 \
    --master-boot-disk-size=30GB \
    --worker-boot-disk-size=30GB \
    --num-workers=2 \
    --initialization-actions=gs://pyspark-fs-sid/install.sh \
    --optional-components=JUPYTER
    



    gcloud dataproc clusters create $MY_CLUSTER \
    --region $REGION \
    --initialization-actions $initialization_actions_script \
    --metadata 'PIP_PACKAGES=pyspark==3.0.1,google-cloud-storage==1.38.0'