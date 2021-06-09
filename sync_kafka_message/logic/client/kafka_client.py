from kafka import KafkaProducer, KafkaConsumer
from kafka.consumer import group
from kafka.structs import OffsetAndMetadata, TopicPartition
from sync_kafka_message.setting import (
    KafkaProducerConfig,
    KafkaConsumerConfig,
)
import json


class KafkaSyncProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=KafkaProducerConfig.KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.topic = KafkaProducerConfig.KAFKA_TOPIC

    def send_message(self, user, event, partition):
        self.producer.send(
            topic=self.topic,
            key=bytes(user, "utf-8"),
            value=event,
            partition=partition,
        )
        self.producer.flush()
