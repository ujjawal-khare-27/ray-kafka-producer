import time
import unittest

import ray
import pandas as pd

from ray_kafka_producer.producer_manager import KafkaProducerManager


class TestProducerManager(unittest.TestCase):
    def setUp(self):
        self.KafkaProducerManager = KafkaProducerManager(
            bootstrap_servers="localhost:9092",
            topic="test",
            actor_pool_size=4,
            num_cpu=0.25,
        )

        data = {}
        for i in range(100):
            temp = []
            for j in range(40000):
                temp.append(j)
            data[str(i)] = temp
        ray.init(ignore_reinit_error=True)
        print("Available resources", ray.available_resources())
        pd_df = pd.DataFrame(data)
        self.ray_df = ray.data.from_pandas(pd_df)
        print("len data", self.ray_df.count())

    def test_producer_manager_serial_producer(self):
        t1 = time.time()
        self.KafkaProducerManager.flush_ray_df(self.ray_df, is_actor=False)
        t2 = time.time()
        print(f"Time taken in test_producer_manager_serial_producer: {t2 - t1}")

    def test_producer_manager_actor_producer(self):
        t1 = time.time()
        self.KafkaProducerManager.flush_ray_df(self.ray_df, is_actor=True)
        t2 = time.time()
        print(f"Time taken in test_producer_manager_actor_producer: {t2 - t1}")

    def tearDown(self):
        self.KafkaProducerManager.close()
        ray.shutdown()
