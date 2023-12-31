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
            actor_pool_size=12,
            num_cpu=0.25,
        )
        self.create_ray_df()
        self.create_pandas_df()

    def create_ray_df(self):
        data = {}
        for i in range(100):
            temp = []
            for j in range(40000):
                temp.append(j)
            data[str(i)] = temp
        ray.init(ignore_reinit_error=True)
        pd_df = pd.DataFrame(data)
        self.ray_df = ray.data.from_pandas(pd_df)
        print("Ray df length", self.ray_df.count())

    def create_pandas_df(self):
        data = {}
        for i in range(100):
            temp = []
            for j in range(40000):
                temp.append(j)
            data[str(i)] = temp
        self.pd_df = pd.DataFrame(data)
        print("Pandas df length", len(self.pd_df))

    def test_producer_manager_serial_producer_ray_df(self):
        """
        This test is to check the time taken to send messages using serial producer for ray df
        :return:
        """
        t1 = time.time()
        self.KafkaProducerManager.flush_ray_df(self.ray_df, is_actor=False)
        t2 = time.time()
        print(f"Time taken in test_producer_manager_serial_producer for ray df: {t2 - t1}")

    def test_producer_manager_actor_producer_ray_df(self):
        """
        This test is to check the time taken to send messages using actor producer for ray df
        :return:
        """
        t1 = time.time()
        self.KafkaProducerManager.flush_ray_df(self.ray_df, is_actor=True)
        t2 = time.time()
        print(f"Time taken in test_producer_manager_actor_producer for ray df: {t2 - t1}")

    def test_producer_manager_actor_producer_pandas_df(self):
        """
        This test is to check the time taken to send messages using actor producer for pandas df
        :return:
        """
        t1 = time.time()
        self.KafkaProducerManager.flush_pandas_df(self.pd_df)
        t2 = time.time()
        print(f"Time taken in test_producer_manager_actor_producer for pandas df: {t2 - t1}")

    def tearDown(self):
        self.KafkaProducerManager.close()
        ray.shutdown()
