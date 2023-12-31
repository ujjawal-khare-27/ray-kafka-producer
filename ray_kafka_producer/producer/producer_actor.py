from typing import List

import ray

from ray_kafka_producer.producer.producer_base_class import ProducerBaseClass
import random


@ray.remote(num_cpus=0.5)
class ProducerActorClass(ProducerBaseClass):
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        self._id = random.randint(1, 1000)
        print("ProducerActorClass init", self._id)
        super().__init__(bootstrap_servers, topic, **kwargs)

    def send_messages(self, messages):
        try:
            print("id of actor", self._id)
            print("messages df", messages)
            messages = messages.to_dict(orient='list')
            print("messages ->>>", messages)
            return super().send_messages(messages)
        except Exception as e:
            import traceback

            traceback.print_exc()
            print("Exception", e)

    def close(self):
        return super().close()
