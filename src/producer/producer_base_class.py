import json

from kafka import KafkaProducer
import ray
from ray.data.block import Block
import pyarrow
from ray.types import ObjectRef


class ProducerBaseClass:
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, **kwargs)

    def send_messages(self, message_refs: ObjectRef["pyarrow.Table"]):
        messages = ray.data.from_arrow_refs([message_refs])
        for message in messages.iter_rows():
            self.producer.send(self.topic, json.dumps(message).encode("utf-8"))

    def close(self):
        self.producer.close()
