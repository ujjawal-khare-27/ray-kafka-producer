import time
import unittest

import ray
import pandas as pd

from src.producer_manager import KafkaProducerManager


class TestProducerManager(unittest.TestCase):
    def setUp(self):
        self.KafkaProducerManager = KafkaProducerManager(
            bootstrap_servers="localhost:9092", topic="test"
        )

        data = {}
        for i in range(10000):
            data[str(i)] = [i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7]

        pd_df = pd.DataFrame(data)
        self.ray_df = ray.data.from_pandas(pd_df)

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
