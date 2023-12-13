from typing import List

import ray

from src.producer.producer_base_class import ProducerBaseClass
import random


@ray.remote(num_cpus=0.5)
class ProducerActorClass(ProducerBaseClass):
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        print("ProducerActorClass init", random.randint(1, 1000))
        super().__init__(bootstrap_servers, topic, **kwargs)

    def send_messages(self, messages: List[dict]):
        try:
            return super().send_messages(messages)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("Exception", e)

    def close(self):
        return super().close()
