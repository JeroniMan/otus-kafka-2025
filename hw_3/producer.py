#!/usr/bin/env python3
"""
Kafka Producer with Transactions
Homework: Transactional message sending with commit and abort examples

Usage:
    python producer.py
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
import time
from datetime import datetime


# Настройка логирования с цветным выводом
class ColoredFormatter(logging.Formatter):
    """Colored log formatter"""

    grey = "\x1b[38;21m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
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
TOPIC1 = 'topic1'
TOPIC2 = 'topic2'
TRANSACTIONAL_ID = f'homework-producer-{datetime.now().strftime("%Y%m%d-%H%M%S")}'


def create_producer():
    """Создание Kafka Producer с поддержкой транзакций"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        transactional_id=TRANSACTIONAL_ID,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # Ждем подтверждения от всех in-sync реплик
        enable_idempotence=True,  # Включаем идемпотентность для exactly-once
        max_in_flight_requests_per_connection=1,  # Для kafka-python 2.0.x должно быть 1
        retries=3,
        retry_backoff_ms=100,
        compression_type='snappy',  # Сжатие для эффективности
        linger_ms=10  # Небольшая задержка для батчинга
    )


def send_transactional_messages():
    """Отправка сообщений с использованием транзакций"""

    producer = None
    try:
        logger.info("=" * 70)
        logger.info("KAFKA TRANSACTIONAL PRODUCER")
        logger.info(f"Transactional ID: {TRANSACTIONAL_ID}")
        logger.info("=" * 70)

        # Создаем producer
        producer = create_producer()

        # Инициализация транзакций
        logger.info("\n🔧 Initializing transactions...")
        producer.init_transactions()
        logger.info("✅ Transactions initialized successfully")

        # ============================================================
        # ТРАНЗАКЦИЯ 1: Будет подтверждена (COMMIT)
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("📤 TRANSACTION 1: Sending 5 messages to each topic (WILL COMMIT)")
        logger.info("=" * 70)

        producer.begin_transaction()

        for i in range(5):
            # Подготовка сообщений
            timestamp = datetime.now().isoformat()

            msg1 = {
                'transaction_id': 1,
                'transaction_status': 'COMMITTED',
                'message_number': i + 1,
                'topic': TOPIC1,
                'timestamp': timestamp,
                'data': {
                    'content': f'Transaction 1 - Message {i + 1} for {TOPIC1}',
                    'value': (i + 1) * 10,
                    'important': True
                }
            }

            msg2 = {
                'transaction_id': 1,
                'transaction_status': 'COMMITTED',
                'message_number': i + 1,
                'topic': TOPIC2,
                'timestamp': timestamp,
                'data': {
                    'content': f'Transaction 1 - Message {i + 1} for {TOPIC2}',
                    'value': (i + 1) * 20,
                    'important': True
                }
            }

            # Отправка с ключами для лучшего партиционирования
            future1 = producer.send(
                TOPIC1,
                key=f'tx1_msg{i + 1}',
                value=msg1,
                headers=[('transaction', b'1'), ('status', b'committed')]
            )

            future2 = producer.send(
                TOPIC2,
                key=f'tx1_msg{i + 1}',
                value=msg2,
                headers=[('transaction', b'1'), ('status', b'committed')]
            )

            # Получаем метаданные отправленных сообщений
            metadata1 = future1.get(timeout=10)
            metadata2 = future2.get(timeout=10)

            logger.info(f"  ✓ Sent message {i + 1}:")
            logger.info(f"    → {TOPIC1}: partition={metadata1.partition}, offset={metadata1.offset}")
            logger.info(f"    → {TOPIC2}: partition={metadata2.partition}, offset={metadata2.offset}")

        # Коммитим транзакцию
        producer.commit_transaction()
        logger.info("\n🎉 TRANSACTION 1 COMMITTED SUCCESSFULLY!")
        logger.info(f"   Total messages sent: 10 (5 to {TOPIC1} + 5 to {TOPIC2})")

        # Пауза между транзакциями
        logger.info("\n⏳ Waiting 2 seconds before next transaction...")
        time.sleep(2)

        # ============================================================
        # ТРАНЗАКЦИЯ 2: Будет отменена (ABORT)
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("📤 TRANSACTION 2: Sending 2 messages to each topic (WILL ABORT)")
        logger.info("=" * 70)

        producer.begin_transaction()

        for i in range(2):
            timestamp = datetime.now().isoformat()

            msg1 = {
                'transaction_id': 2,
                'transaction_status': 'ABORTED',
                'message_number': i + 1,
                'topic': TOPIC1,
                'timestamp': timestamp,
                'data': {
                    'content': f'Transaction 2 - Message {i + 1} for {TOPIC1}',
                    'value': (i + 1) * 100,
                    'important': False,
                    'note': 'This message should NOT be visible to read_committed consumers'
                }
            }

            msg2 = {
                'transaction_id': 2,
                'transaction_status': 'ABORTED',
                'message_number': i + 1,
                'topic': TOPIC2,
                'timestamp': timestamp,
                'data': {
                    'content': f'Transaction 2 - Message {i + 1} for {TOPIC2}',
                    'value': (i + 1) * 200,
                    'important': False,
                    'note': 'This message should NOT be visible to read_committed consumers'
                }
            }

            future1 = producer.send(
                TOPIC1,
                key=f'tx2_msg{i + 1}',
                value=msg1,
                headers=[('transaction', b'2'), ('status', b'aborted')]
            )

            future2 = producer.send(
                TOPIC2,
                key=f'tx2_msg{i + 1}',
                value=msg2,
                headers=[('transaction', b'2'), ('status', b'aborted')]
            )

            metadata1 = future1.get(timeout=10)
            metadata2 = future2.get(timeout=10)

            logger.info(f"  ✓ Sent message {i + 1}:")
            logger.info(f"    → {TOPIC1}: partition={metadata1.partition}, offset={metadata1.offset}")
            logger.info(f"    → {TOPIC2}: partition={metadata2.partition}, offset={metadata2.offset}")

        # Откатываем транзакцию
        producer.abort_transaction()
        logger.info("\n❌ TRANSACTION 2 ABORTED INTENTIONALLY!")
        logger.info(f"   Messages sent but NOT committed: 4 (2 to {TOPIC1} + 2 to {TOPIC2})")

        # Итоговая статистика
        logger.info("\n" + "=" * 70)
        logger.info("📊 FINAL STATISTICS:")
        logger.info("=" * 70)
        logger.info("✅ Transaction 1: COMMITTED - 10 messages (visible to consumers)")
        logger.info("❌ Transaction 2: ABORTED   - 4 messages (NOT visible to read_committed)")
        logger.info("=" * 70)
        logger.info("\n✨ Producer finished successfully!")
        logger.info("💡 Run consumer.py with isolation_level='read_committed' to see only committed messages")

    except KafkaError as e:
        logger.error(f"Kafka error occurred: {e}")
        if producer and producer._transaction_manager:
            try:
                producer.abort_transaction()
                logger.info("Transaction aborted due to error")
            except:
                pass
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if producer and producer._transaction_manager:
            try:
                producer.abort_transaction()
                logger.info("Transaction aborted due to error")
            except:
                pass
    finally:
        if producer:
            producer.close()
            logger.info("\n🔒 Producer closed")


def main():
    """Главная функция"""
    try:
        send_transactional_messages()
    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Producer interrupted by user")
    except Exception as e:
        logger.error(f"Failed to run producer: {e}")
        raise


if __name__ == "__main__":
    main()