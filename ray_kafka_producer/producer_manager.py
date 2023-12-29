import ray
from ray.data import Dataset

from ray_kafka_producer.producer.producer import Producer
from ray_kafka_producer.producer.producer_actor import ProducerActorClass


class KafkaProducerManager:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        batch_size: int,
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
            for i in range(self.actor_pool_size)
        ]

        self.batch_size = batch_size
        self._producer = Producer(
            bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
        )

    def send_messages(self, df: Dataset, is_actor=True):
        try:
            responses = []
            rows = []
            i = 0

            messages_batch = []

            def flush_to_kafka(messages, counter) -> int:
                if is_actor:
                    counter = counter % self.actor_pool_size
                    messages_batch.append(messages)
                else:
                    resp = self._producer.send_messages(messages)
                    responses.append(resp)
                return counter + 1

            for row in df.iter_rows():
                rows.append(row)

                if len(rows) == self.batch_size:
                    i = flush_to_kafka(rows, i)
                    rows = []
            i = flush_to_kafka(rows, i)

            print("len(messages_batch)", len(messages_batch))
            if is_actor:
                # Do ray.get in batches
                for i in range(0, len(messages_batch), self.actor_pool_size):
                    print("Sending messages", i)
                    ray.get(
                        [
                            self._producer_actors[
                                i % self.actor_pool_size
                            ].send_messages.remote(messages)
                            for i, messages in enumerate(messages_batch[i : i + self.actor_pool_size])
                        ]
                    )



                # ray.get(
                #     [
                #         self._producer_actors[
                #             i % self.actor_pool_size
                #         ].send_messages.remote(messages)
                #         for i, messages in enumerate(messages_batch)
                #     ]
                # )
                print("Waiting for responses", len(responses), responses)
        except Exception as e:
            import traceback

            traceback.print_exc()
            print("Exception occurred in send_messages in producer manager", e)

    def close(self):
        for actor in self._producer_actors:
            actor.close.remote()

        self._producer.close()
