"""
Queue abstraction layer for switching between Redis and Kafka
"""

from enum import Enum
from typing import Protocol, Optional, Dict, Any
import os


class QueueBackend(str, Enum):
    """Supported queue backends"""
    REDIS = "redis"
    KAFKA = "kafka"


class QueueAdapter(Protocol):
    """Protocol for queue operations"""

    async def connect(self) -> None:
        """Initialize connection to queue backend"""
        ...

    async def disconnect(self) -> None:
        """Close connection to queue backend"""
        ...

    async def push_task(self, task: Dict[str, Any], topic: str = None) -> None:
        """Push task to queue"""
        ...

    async def consume_tasks(self, callback, topics: list = None) -> None:
        """Start consuming tasks from queue"""
        ...

    async def acknowledge(self, message: Any) -> None:
        """Acknowledge message processing"""
        ...


def get_queue_adapter() -> QueueAdapter:
    """Factory to get appropriate queue adapter based on config"""
    backend = os.getenv("QUEUE_BACKEND", "redis").lower()

    if backend == "kafka":
        from shared.queue.kafka_adapter import KafkaQueueAdapter
        return KafkaQueueAdapter()
    else:
        from shared.queue.redis_adapter import RedisQueueAdapter
        return RedisQueueAdapter()


__all__ = ['QueueBackend', 'QueueAdapter', 'get_queue_adapter']