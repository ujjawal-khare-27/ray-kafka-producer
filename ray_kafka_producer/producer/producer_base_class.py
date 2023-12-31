import json
import time
from typing import List
import random

from kafka import KafkaProducer


class ProducerBaseClass:
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, **kwargs)

    def send_messages(self, messages: List[dict]):
        for message in messages:
            self.send_message(message)

    def send_message(self, message: dict):
        self.producer.send(self.topic, json.dumps(message).encode("utf-8"))

    def close(self):
        self.producer.close()
