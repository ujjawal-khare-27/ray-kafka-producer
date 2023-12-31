import time
from typing import List

import ray
from ray.data import Dataset

from ray_kafka_producer.producer.producer_base_class import ProducerBaseClass
import random


@ray.remote(num_cpus=0.5)
class ProducerActorClass(ProducerBaseClass):
    def __init__(self, bootstrap_servers: str, topic: str, **kwargs):
        self._id = random.randint(1, 1000)
        print("ProducerActorClass init", self._id)
        super().__init__(bootstrap_servers, topic, **kwargs)

    def send_messages(self, messages: Dataset):
        try:
            count = 0
            t1 = time.time()
            for row in messages.iter_rows():
                super().send_message(row)
                count += 1
            t2 = time.time()
            print(
                f"Time taken in sending messages in actor {self._id}: {t2 - t1} of length {count}"
            )
        except Exception as e:
            import traceback

            traceback.print_exc()
            print("Exception", e)

    def close(self):
        return super().close()
