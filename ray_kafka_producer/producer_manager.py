import time

import pandas
import ray
from ray.data import Dataset

from ray_kafka_producer.producer.producer_actor import ProducerActorClass
from ray_kafka_producer.producer.producer_base_class import ProducerBaseClass


class KafkaProducerManager:
    def __init__(
            self,
            bootstrap_servers: str,
            topic: str,
            actor_pool_size: int,
            num_cpu=0.25,
            **kwargs
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.actor_pool_size = actor_pool_size

        self._producer_actors = [
            ProducerActorClass.options(num_cpus=num_cpu).remote(
                bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
            )
            for _ in range(self.actor_pool_size)
        ]

        self._producer = ProducerBaseClass(
            bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
        )

    def get_proportions(self):
        prop_sum = 0
        proportions = []
        for i in range(self.actor_pool_size - 1):
            prop = 1 / self.actor_pool_size
            proportions.append(prop)
            prop_sum += prop
        proportions.append(1 - prop_sum - 0.00000000001)
        return proportions

    def _send_messages(self, df: Dataset, is_actor=True):
        try:
            if is_actor:
                shards = df.split_proportionately(self.get_proportions())
                ray.get([actor.send_messages.remote(shard) for actor, shard in zip(self._producer_actors, shards)])
            else:
                rows = []
                for row in df.iter_rows():
                    rows.append(row)
                self._producer.send_messages(rows)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("Exception occurred in send_messages in producer manager", e)

    def flush_pandas_df(self, df: pandas.DataFrame):
        try:
            ray_df = ray.data.from_pandas(df)
            self._send_messages(ray_df, is_actor=True)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("Exception occurred in flush_pandas_df in producer manager", e)

    def flush_ray_df(self, df: Dataset, is_actor=True):
        try:
            self._send_messages(df, is_actor=is_actor)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("Exception occurred in flush_ray_df in producer manager", e)

    def close(self):
        for actor in self._producer_actors:
            actor.close.remote()

        self._producer.close()
