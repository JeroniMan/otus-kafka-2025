"""
Kafka implementation of QueueAdapter
"""

import json
import asyncio
import os
from typing import Dict, Any, Optional, Callable, List
import structlog
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError

from shared.queue import QueueAdapter

logger = structlog.get_logger()


class KafkaQueueAdapter(QueueAdapter):
    """Kafka queue adapter implementation"""

    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "analytics-workers")

        # Topic mapping
        self.topics = {
            "pending": os.getenv("KAFKA_TOPIC_PENDING", "analytics.tasks.pending"),
            "retry": os.getenv("KAFKA_TOPIC_RETRY", "analytics.tasks.retry"),
            "dlq": os.getenv("KAFKA_TOPIC_DLQ", "analytics.tasks.dlq"),
            "updates": os.getenv("KAFKA_TOPIC_UPDATES", "analytics.updates")
        }

    async def connect(self) -> None:
        """Initialize Kafka connections"""
        try:
            # Initialize producer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type="gzip",
                acks='all',  # Wait for all replicas
                retry_backoff_ms=100,
                request_timeout_ms=30000
            )
            await self.producer.start()
            logger.info("kafka_producer_connected",
                        servers=self.bootstrap_servers)

        except Exception as e:
            logger.error("kafka_connection_failed",
                         error=str(e),
                         servers=self.bootstrap_servers)
            raise

    async def disconnect(self) -> None:
        """Close Kafka connections"""
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()

    async def push_task(self, task: Dict[str, Any], topic: str = "pending") -> None:
        """Push task to Kafka topic"""
        if not self.producer:
            await self.connect()

        kafka_topic = self.topics.get(topic, self.topics["pending"])

        try:
            # Add Kafka metadata
            task["_kafka_metadata"] = {
                "topic": kafka_topic,
                "timestamp": asyncio.get_event_loop().time(),
                "producer_id": os.getenv("HOSTNAME", "unknown")
            }

            # Send to Kafka with task_id as key for ordering
            await self.producer.send(
                kafka_topic,
                value=task,
                key=task.get("id", "").encode('utf-8')
            )

            logger.info("task_pushed_to_kafka",
                        task_id=task.get("id"),
                        topic=kafka_topic)

        except KafkaError as e:
            logger.error("kafka_push_failed",
                         error=str(e),
                         task_id=task.get("id"))
            raise

    async def consume_tasks(self, callback: Callable, topics: List[str] = None) -> None:
        """Start consuming tasks from Kafka"""
        if not topics:
            topics = ["pending", "retry"]

        kafka_topics = [self.topics[t] for t in topics if t in self.topics]

        # Initialize consumer с увеличенными таймаутами
        self.consumer = AIOKafkaConsumer(
            *kafka_topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.consumer_group,
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=False,
            max_poll_records=int(os.getenv("KAFKA_MAX_POLL_RECORDS", "1")),  # Уменьшено до 1
            max_poll_interval_ms=300000,  # 5 минут на обработку сообщения
            session_timeout_ms=30000,  # 30 секунд timeout сессии
            heartbeat_interval_ms=3000,  # Heartbeat каждые 3 секунды
            request_timeout_ms=60000,  # 60 секунд на запросы
        )

        await self.consumer.start()
        logger.info("kafka_consumer_started",
                    topics=kafka_topics,
                    group=self.consumer_group)

        try:
            async for message in self.consumer:
                try:
                    if message.value:
                        raw_value = message.value.decode('utf-8') if isinstance(message.value, bytes) else message.value

                        logger.debug("kafka_raw_message",
                                     topic=message.topic,
                                     raw_value=raw_value[:100])

                        try:
                            task = json.loads(raw_value)
                        except json.JSONDecodeError as e:
                            logger.error("kafka_invalid_json",
                                         error=str(e),
                                         raw_value=raw_value[:200])
                            await self.consumer.commit()
                            continue

                        logger.info("kafka_message_received",
                                    topic=message.topic,
                                    partition=message.partition,
                                    offset=message.offset,
                                    task_id=task.get("id"))

                        # Process task
                        await callback(task)

                        # Commit сразу после обработки
                        try:
                            await self.consumer.commit()
                            logger.debug("kafka_commit_successful",
                                         topic=message.topic,
                                         offset=message.offset)
                        except Exception as commit_error:
                            logger.error("kafka_commit_failed",
                                         error=str(commit_error),
                                         topic=message.topic)
                            # Продолжаем работу, сообщение будет переобработано
                    else:
                        logger.warning("kafka_empty_message",
                                       topic=message.topic,
                                       offset=message.offset)
                        await self.consumer.commit()

                except Exception as e:
                    logger.error("task_processing_failed",
                                 error=str(e),
                                 error_type=type(e).__name__,
                                 topic=message.topic)
                    # Не коммитим - сообщение будет переобработано

        except asyncio.CancelledError:
            logger.info("kafka_consumer_cancelled")
            raise
        finally:
            await self.consumer.stop()

    async def acknowledge(self, message: Any) -> None:
        """Acknowledge message (commit offset)"""
        if self.consumer:
            await self.consumer.commit()