import json
from typing import List

from kafka import KafkaProducer
import ray
import pyarrow


class ProducerBaseClass:
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, **kwargs)

    def send_messages(self, messages: List[dict]):
        for message in messages:
            self.producer.send(self.topic, json.dumps(message).encode("utf-8"))

    def close(self):
        self.producer.close()
