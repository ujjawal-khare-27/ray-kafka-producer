import ray
from ray.data import Dataset

from src.producer.producer import Producer
from src.producer.producer_actor import ProducerActorClass


class KafkaProducerManager:
    def __init__(self, bootstrap_servers: str, topic: str, batch_size: int, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.actor_pool_size = 6

        self._producer_actors = [ProducerActorClass.remote(
                bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
            ) for i in range(self.actor_pool_size)]

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
                    # _producer_actor = self._producer_actors[counter]
                    # print("Sending messages", len(messages), counter)
                    messages_batch.append(messages)
                    # resp = _producer_actor.send_messages.remote(messages)
                    # responses.append(resp)
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
                ray.get([self._producer_actors[i%self.actor_pool_size].send_messages.remote(messages) for i, messages in enumerate(messages_batch)])
                print("Waiting for responses", len(responses), responses)
                # ray.get(responses)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("Exception occurred in send_messages in producer manager", e)

    def close(self):
        for actor in self._producer_actors:
            actor.close.remote()

        self._producer.close()
