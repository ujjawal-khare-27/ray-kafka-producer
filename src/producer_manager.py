import ray
from ray.data import Dataset


from src.producer.producer import Producer
from src.producer.producer_actor import ProducerActorClass


class KafkaProducerManager:
    def __init__(self, bootstrap_servers: str, topic: str, batch_size: int, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer_actor = ProducerActorClass.remote(
            bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
        )
        self.batch_size = batch_size
        self._producer = Producer(
            bootstrap_servers=self.bootstrap_servers, topic=self.topic, **kwargs
        )

    def send_messages(self, df: Dataset, is_actor=True):
        try:
            responses = []
            rows = []

            def flush_to_kafka(messages):
                if is_actor:
                    resp = self._producer_actor.send_messages.remote(messages)
                    responses.append(resp)
                else:
                    resp = self._producer.send_messages(messages)
                    responses.append(resp)

            for row in df.iter_rows():
                rows.append(row)

                if len(rows) == self.batch_size:
                    flush_to_kafka(rows)
                    rows = []
            flush_to_kafka(rows)

            if is_actor:
                print("Waiting for responses", len(responses), responses)
                ray.get(responses)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("Exception occurred in send_messages in producer manager", e)

    def close(self):
        self._producer_actor.close.remote()
        self._producer.close()
