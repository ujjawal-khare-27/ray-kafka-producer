from ray.data import Dataset

from src.producer.producer import Producer
from src.producer.producer_actor import ProducerActorClass


class KafkaProducerManager:
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer_actor = ProducerActorClass.remote(
            bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
        )
        self._producer = Producer(
            bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
        )

    def send_messages(self, df: Dataset, is_actor=True):
        blocks = df.to_arrow_refs()
        responses = []
        for block in blocks:
            if is_actor:
                resp = self._producer_actor.send_messages.remote(block)
            else:
                resp = self._producer.send_messages(block)
            responses.append(resp)

    def close(self):
        self._producer_actor.close.remote()
        self._producer.close()
