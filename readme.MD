# Ray Kafka Producer for Efficient Data Streaming to Kafka

This Python Kafka producer facilitates high-performance data streaming from Ray DataFrame and Pandas DataFrame to Kafka. It is optimized to provide approximately 3-4x performance improvement compared to standard Kafka producers.

## Installation

Install the package using pip:

```bash
pip3 install ray_kafka_producer@git+https://github.com/ujjawal-khare-27/ray-kafka-producer@main --force-reinstall
```

## Usage

1. Import the package

```python 
from ray_kafka_producer.producer_manager import KafkaProducerManager

# actor_pool_size is the number of actors that will be created to send data to Kafka
# num_cpu is the number of CPUs that will be allocated to each actor
kafka_producer_manager = KafkaProducerManager(bootstrap_servers="localhost:9092", topic="test", actor_pool_size=12,
            num_cpu=0.25)

# Insert data into Ray DataFrame 
kafka_producer_manager.flush_ray_df(df = ray_df, is_actor=True)

# Insert data into Pandas DataFrame
kafka_producer_manager.flush_pandas_df(df = pandas_df)
```