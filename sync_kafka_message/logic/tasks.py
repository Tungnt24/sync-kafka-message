from kafka import KafkaConsumer
from sync_kafka_message.logic.client.kafka_client import (
    KafkaSyncProducer,
)
from sync_kafka_message.utils.logger import logger
from sync_kafka_message.setting import (
    KafkaConsumerConfig,
)
from kafka.structs import TopicPartition, OffsetAndMetadata
import json


def get_consumer():
    consumer = KafkaConsumer(
        *KafkaConsumerConfig.KAFKA_TOPIC,
        group_id=KafkaConsumerConfig.KAFKA_CONSUMER_GROUP,
        bootstrap_servers=KafkaConsumerConfig.KAFKA_BROKER,
        auto_offset_reset=KafkaConsumerConfig.KAFKA_AUTO_OFFSET_RESET,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=KafkaConsumerConfig.KAFKA_ENABLE_AUTO_COMMIT,
        max_poll_records=KafkaConsumerConfig.KAFKA_MAX_POLL_RECORDS,
    )
    return consumer


def poll_message():
    consumer = get_consumer()
    while True:
        msg = consumer.poll(1000)
        if not msg:
            logger.info("poll timeout")
            continue
        for event in list(msg.values())[0]:
            data = event.value
            logger.info(f"EVENT:  {data}")
            user = data["user"]
            topic = event.topic
            offset = event.offset
            partition = event.partition
            send_kafka_message(consumer, user, data, partition, offset, topic)


def send_kafka_message(
    consumer, user, event, partition, offset, topic
):
    producer = KafkaSyncProducer()
    logger.info(
        f"SENDING MESSAGE | USER: {user} | TOPIC: {topic} | PARTITION: {partition} | OFFSET: {offset}"
    )
    producer.send_message(user, event, partition)
    tp = TopicPartition(topic, partition)
    consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
    logger.info(f"DONE")
