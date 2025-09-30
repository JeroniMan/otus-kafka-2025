#!/usr/bin/env python3
# test_producer.py

import json
import time
import random
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_topic():
    """Создание топика events если его нет"""
    admin = KafkaAdminClient(
        bootstrap_servers='localhost:9092',
        client_id='test-producer'
    )

    topic = NewTopic(
        name='events',
        num_partitions=1,
        replication_factor=1
    )

    try:
        admin.create_topics([topic])
        logger.info("Topic 'events' created successfully")
    except TopicAlreadyExistsError:
        logger.info("Topic 'events' already exists")

    admin.close()


def send_test_messages():
    """Отправка тестовых сообщений"""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: v.encode('utf-8')
    )

    # Набор ключей для тестирования
    keys = ['key1', 'key2', 'key3', 'key4', 'key5']

    logger.info("Starting to send test messages...")
    logger.info("Press Ctrl+C to stop")

    try:
        message_count = 0
        while True:
            # Случайный выбор ключа
            key = random.choice(keys)

            # Создаем сообщение
            value = f"event_{message_count}"

            # Отправляем в Kafka
            producer.send('events', key=key, value=value)

            logger.info(f"Sent: key={key}, value={value}")
            message_count += 1

            # Пауза между сообщениями
            time.sleep(random.uniform(0.5, 2.0))

            # Каждые 10 сообщений делаем flush
            if message_count % 10 == 0:
                producer.flush()

    except KeyboardInterrupt:
        logger.info("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Total messages sent: {message_count}")


def send_batch_messages():
    """Отправка пачки сообщений для теста окна"""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: v.encode('utf-8')
    )

    logger.info("Sending batch of messages...")

    # Отправляем несколько сообщений с одинаковыми ключами
    test_data = [
        ('key1', 'value1'),
        ('key2', 'value2'),
        ('key1', 'value3'),
        ('key3', 'value4'),
        ('key2', 'value5'),
        ('key1', 'value6'),
        ('key1', 'value7'),
        ('key2', 'value8'),
    ]

    for key, value in test_data:
        producer.send('events', key=key, value=value)
        logger.info(f"Sent: key={key}, value={value}")

    producer.flush()
    producer.close()
    logger.info(f"Batch complete! Sent {len(test_data)} messages")


if __name__ == '__main__':
    import sys

    # Создаем топик
    create_topic()

    # Выбираем режим отправки
    if len(sys.argv) > 1 and sys.argv[1] == 'batch':
        send_batch_messages()
    else:
        send_test_messages()