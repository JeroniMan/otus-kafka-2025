#!/usr/bin/env python3
"""
Kafka Consumer with Read Committed Isolation Level
Reads only messages from committed transactions

Usage:
    python consumer.py                    # Read committed only (default)
    python consumer.py --read-uncommitted  # Read all messages including aborted
"""

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json
import logging
import signal
import sys
import argparse
from datetime import datetime
from typing import Dict, List


# Настройка логирования с цветным выводом
class ColoredFormatter(logging.Formatter):
    """Colored log formatter"""

    grey = "\x1b[38;21m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
    cyan = "\x1b[36m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: grey + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.INFO: green + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.WARNING: yellow + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.ERROR: red + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.CRITICAL: bold_red + "%(asctime)s - %(levelname)s - %(message)s" + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)


# Настройка логера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter())
logger.addHandler(handler)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = ['topic1', 'topic2']
# Используем фиксированный consumer group для домашнего задания
CONSUMER_GROUP = 'homework-transactional-consumer'


class TransactionalConsumer:
    """Kafka Consumer для чтения транзакционных сообщений"""

    def __init__(self, isolation_level='read_committed'):
        self.isolation_level = isolation_level
        self.consumer = None
        self.running = True
        self.stats: Dict[str, int] = {
            'total_messages': 0,
            'topic1_messages': 0,
            'topic2_messages': 0,
            'committed_messages': 0,
            'aborted_messages': 0
        }

        # Обработчик сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения"""
        logger.info("\n⚠️  Received interrupt signal, shutting down gracefully...")
        self.running = False

    def create_consumer(self):
        """Создание Kafka Consumer с нужным уровнем изоляции"""
        logger.info(f"Creating consumer with isolation_level='{self.isolation_level}'")

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='earliest',  # Читаем с начала топика
            enable_auto_commit=False,  # Ручной коммит для большего контроля
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            isolation_level=self.isolation_level,  # КЛЮЧЕВОЙ ПАРАМЕТР!
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
            consumer_timeout_ms=1000  # Таймаут для poll() в миллисекундах
        )

        # Подписываемся на топики
        consumer.subscribe(topics=TOPICS)

        # Важно: делаем первый poll для инициализации партиций
        logger.info("Initializing consumer partitions...")
        consumer.poll(timeout_ms=1000)

        # Теперь проверяем назначенные партиции
        partitions = consumer.assignment()
        if partitions:
            logger.info(f"Seeking to beginning for {len(partitions)} partitions")
            consumer.seek_to_beginning(*partitions)
        else:
            logger.info("No partitions assigned yet, will read from auto_offset_reset position")

        return consumer

    def print_message(self, message, index):
        """Красивый вывод сообщения"""
        value = message.value
        headers = dict(message.headers) if message.headers else {}

        # Определяем статус транзакции из сообщения
        tx_status = value.get('transaction_status', 'UNKNOWN')
        tx_id = value.get('transaction_id', 'N/A')

        # Цветовая индикация
        if tx_status == 'COMMITTED':
            status_color = "\x1b[32m"  # Зеленый
            self.stats['committed_messages'] += 1
        elif tx_status == 'ABORTED':
            status_color = "\x1b[31m"  # Красный
            self.stats['aborted_messages'] += 1
        else:
            status_color = "\x1b[33m"  # Желтый

        reset = "\x1b[0m"
        cyan = "\x1b[36m"

        print(f"\n{cyan}╔══════════════════════════════════════════════════════════════╗{reset}")
        print(f"{cyan}║{reset} Message #{index:<4} {status_color}[{tx_status}]{reset}")
        print(f"{cyan}╟──────────────────────────────────────────────────────────────╢{reset}")
        print(f"{cyan}║{reset} Topic:        {message.topic}")
        print(f"{cyan}║{reset} Partition:    {message.partition}")
        print(f"{cyan}║{reset} Offset:       {message.offset}")
        print(f"{cyan}║{reset} Key:          {message.key}")
        print(f"{cyan}║{reset} Timestamp:    {datetime.fromtimestamp(message.timestamp / 1000).isoformat()}")
        print(f"{cyan}║{reset} Transaction:  ID={tx_id}, Status={tx_status}")

        if headers:
            print(f"{cyan}║{reset} Headers:      {headers}")

        if 'data' in value:
            data = value['data']
            print(f"{cyan}║{reset} Data:")
            print(f"{cyan}║{reset}   - Content:  {data.get('content')}")
            print(f"{cyan}║{reset}   - Value:    {data.get('value')}")
            if 'note' in data:
                print(f"{cyan}║{reset}   - Note:     {data.get('note')}")

        print(f"{cyan}╚══════════════════════════════════════════════════════════════╝{reset}")

        # Обновляем статистику по топикам
        if message.topic == 'topic1':
            self.stats['topic1_messages'] += 1
        elif message.topic == 'topic2':
            self.stats['topic2_messages'] += 1

    def consume_messages(self):
        """Основной цикл чтения сообщений"""
        try:
            logger.info("=" * 70)
            logger.info("KAFKA TRANSACTIONAL CONSUMER")
            logger.info(f"Consumer Group: {CONSUMER_GROUP}")
            logger.info(f"Isolation Level: {self.isolation_level}")
            logger.info(f"Topics: {', '.join(TOPICS)}")
            logger.info("=" * 70)

            self.consumer = self.create_consumer()

            logger.info(f"\n📡 Consumer started. Listening to topics: {TOPICS}")
            logger.info(f"🔍 Isolation level: '{self.isolation_level}'")

            if self.isolation_level == 'read_committed':
                logger.info("✅ Will read ONLY messages from COMMITTED transactions")
            else:
                logger.info("⚠️  Will read ALL messages including ABORTED transactions")

            logger.info("\n" + "=" * 70)
            logger.info("CONSUMING MESSAGES...")
            logger.info("=" * 70)

            message_count = 0
            empty_polls = 0
            max_empty_polls = 5  # После 5 пустых poll'ов завершаем

            while self.running:
                try:
                    # Читаем сообщения
                    messages = self.consumer.poll(timeout_ms=1000, max_records=10)

                    if not messages:
                        empty_polls += 1
                        if empty_polls >= max_empty_polls:
                            logger.info("\n📭 No more messages to consume")
                            break
                        continue

                    empty_polls = 0  # Сбрасываем счетчик

                    # Обрабатываем сообщения
                    for topic_partition, records in messages.items():
                        for message in records:
                            message_count += 1
                            self.stats['total_messages'] += 1
                            self.print_message(message, message_count)

                    # Коммитим offset'ы
                    self.consumer.commit()

                except Exception as e:
                    logger.error(f"Error processing messages: {e}")
                    continue

            # Выводим финальную статистику
            self.print_statistics()

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("🔒 Consumer closed")

    def print_statistics(self):
        """Вывод статистики потребления"""
        logger.info("\n" + "=" * 70)
        logger.info("📊 CONSUMPTION STATISTICS")
        logger.info("=" * 70)
        logger.info(f"Total messages consumed: {self.stats['total_messages']}")
        logger.info(f"  → From topic1: {self.stats['topic1_messages']}")
        logger.info(f"  → From topic2: {self.stats['topic2_messages']}")

        if self.isolation_level == 'read_committed':
            logger.info(f"\n✅ Messages from COMMITTED transactions: {self.stats['committed_messages']}")
            logger.info(f"❌ Messages from ABORTED transactions: {self.stats['aborted_messages']}")

            if self.stats['aborted_messages'] > 0:
                logger.warning("⚠️  Warning: Found aborted messages with read_committed!")
                logger.warning("    This should not happen. Check your Kafka configuration.")
            elif self.stats['committed_messages'] == 10:
                logger.info("\n🎉 SUCCESS! Consumer correctly read only committed messages!")
                logger.info("   Expected: 10 messages (5 from each topic)")
                logger.info("   Received: 10 messages ✓")
        else:
            logger.info(f"\nMessages breakdown:")
            logger.info(f"  → From committed transactions: {self.stats['committed_messages']}")
            logger.info(f"  → From aborted transactions: {self.stats['aborted_messages']}")

        logger.info("=" * 70)


def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description='Kafka Consumer for reading transactional messages'
    )
    parser.add_argument(
        '--read-uncommitted',
        action='store_true',
        help='Read all messages including aborted transactions (default: read only committed)'
    )
    parser.add_argument(
        '--group-id',
        type=str,
        help='Custom consumer group ID (default: homework-transactional-consumer)'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset consumer group to read from beginning'
    )
    return parser.parse_args()


def main():
    """Главная функция"""
    args = parse_arguments()

    # Определяем уровень изоляции
    if args.read_uncommitted:
        isolation_level = 'read_uncommitted'
        logger.warning("⚠️  Running with 'read_uncommitted' - will see aborted transactions!")
    else:
        isolation_level = 'read_committed'
        logger.info("✅ Running with 'read_committed' - will see only committed transactions")

    # Переопределяем group_id если указан
    global CONSUMER_GROUP
    if args.group_id:
        CONSUMER_GROUP = args.group_id

    # Если флаг --reset, используем новый group_id для чтения с начала
    if args.reset:
        CONSUMER_GROUP = f'reset-consumer-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
        logger.info(f"🔄 Reset mode: using new consumer group '{CONSUMER_GROUP}'")

    try:
        consumer = TransactionalConsumer(isolation_level=isolation_level)
        consumer.consume_messages()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Failed to run consumer: {e}")
        raise


if __name__ == "__main__":
    main()