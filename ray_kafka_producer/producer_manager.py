import pandas
import ray
from ray.data import Dataset
from typeguard import typechecked

from ray_kafka_producer.producer.producer_actor import ProducerActorClass
from ray_kafka_producer.producer.producer_base_class import ProducerBaseClass


def _validate_init_args(
    actor_pool_size: int,
    num_cpu=0.25,
):
    """
    Validates the arguments passed to the class.
    :return:
    """
    available_resources = ray.available_resources()
    if available_resources["CPU"] < actor_pool_size * num_cpu:
        raise ValueError(
            f"Insufficient CPU resources. Available: {available_resources['CPU']}, "
            f"Required: {actor_pool_size * num_cpu}"
        )


@typechecked
class KafkaProducerManager:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        actor_pool_size: int,
        num_cpu=0.25,
        **kwargs,
    ):
        """
        :param bootstrap_servers: Kafka broker. e.g. "localhost:9092"
        :param topic: Kafka topic to write to.
        :param actor_pool_size: Number of actors to create for parallel writing.
        :param num_cpu: Number of CPUs to allocate to each actor.
        """
        ray.init(ignore_reinit_error=True)
        _validate_init_args(actor_pool_size=actor_pool_size, num_cpu=num_cpu)
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
                ray.get(
                    [
                        actor.send_messages.remote(shard)
                        for actor, shard in zip(self._producer_actors, shards)
                    ]
                )
            else:
                for row in df.iter_rows():
                    self._producer.send_message(row)
        except Exception as e:
            import traceback

            traceback.print_exc()
            print("Exception occurred in send_messages in producer manager", e)

    def flush_pandas_df(self, df: pandas.DataFrame):
        """
        :param df: Pandas DataFrame to write to Kafka.
        :return:
        """
        try:
            ray_df = ray.data.from_pandas(df)
            self._send_messages(ray_df, is_actor=True)
        except Exception as e:
            import traceback

            traceback.print_exc()
            print("Exception occurred in flush_pandas_df in producer manager", e)

    def flush_ray_df(self, df: Dataset, is_actor=True):
        """
        :param df: Ray DataFrame to write to Kafka.
        :param is_actor: If True, uses actors to write to Kafka. Else, uses serial producer.
        :return:
        """
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
