import time
import unittest

import ray
import pandas as pd

from src.producer_manager import KafkaProducerManager


class TestProducerManager(unittest.TestCase):
    def setUp(self):
        self.KafkaProducerManager = KafkaProducerManager(
            bootstrap_servers="localhost:9092", topic="test", batch_size=100
        )

        data = {}
        for i in range(1000):
            temp = []
            for j in range(1000):
                temp.append(j)
            data[str(i)] = temp

        pd_df = pd.DataFrame(data)
        self.ray_df = ray.data.from_pandas(pd_df)
        print("len data", self.ray_df.count())

    def test_producer_manager_serial_producer(self):
        t1 = time.time()
        self.KafkaProducerManager.send_messages(self.ray_df, is_actor=False)
        t2 = time.time()
        print(f"Time taken in test_producer_manager_serial_producer: {t2 - t1}")

    def test_producer_manager_actor_producer(self):
        t1 = time.time()
        self.KafkaProducerManager.send_messages(self.ray_df, is_actor=True)
        t2 = time.time()
        print(f"Time taken in test_producer_manager_actor_producer: {t2 - t1}")

    def tearDown(self):
        self.KafkaProducerManager.close()
        ray.shutdown()
