A python ray Kafka producer for efficient data streaming to Kafka.

```1. pip install https://github.com/ujjawal-khare-27/ray-kafka-producer.git ```

```2. kafkaProducerManager = KafkaProducerManager(bootstrap_servers="localhost:9092", topic="test", batch_size=100)```

```3. kafkaProducerManager.send_messages(ray_df, is_actor=True)```

```4. For debugging purpose, use is_actor=False ```