from src.producer.producer_base_class import ProducerBaseClass
from ray.data.block import Block
from ray.types import ObjectRef


class Producer(ProducerBaseClass):
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        super().__init__(bootstrap_servers=bootstrap_servers, topic=topic, **kwargs)

    def send_messages(self, messages: ObjectRef["pyarrow.Table"]):
        return super().send_messages(messages)

    def close(self):
        return super().close()
