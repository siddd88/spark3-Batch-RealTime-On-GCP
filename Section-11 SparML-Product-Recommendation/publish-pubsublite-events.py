import json
from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import MessageMetadata
from google.cloud import storage
from random import randrange
import time,datetime
bucket_name = "bucket-name"
topic = "your-topic-path"

storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob("events.json")
data = json.loads(blob.download_as_string(client=None))

with PublisherClient() as publisher_client:
    for row in data:
        now = datetime.datetime.now()
        row['event_date']= now.strftime("%Y-%m-%d %H:%M:%S")
        data = json.dumps(row).encode("utf-8")
        api_future = publisher_client.publish(
            topic,
            data=data
        )
        message_id = api_future.result()
        message_metadata = MessageMetadata.decode(message_id)
        print(
            f"Published {data} to partition {message_metadata.partition.value} and offset {message_metadata.cursor.offset}."
        )
        time.sleep(randrange(1,10))
