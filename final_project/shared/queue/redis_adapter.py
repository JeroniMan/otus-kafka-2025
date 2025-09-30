"""
Redis implementation of QueueAdapter (wrapper for existing functionality)
"""

import json
import os
from typing import Dict, Any, Optional, Callable, List
import redis.asyncio as redis
import structlog

from shared.queue import QueueAdapter
from workers.config import settings

logger = structlog.get_logger()


class RedisQueueAdapter(QueueAdapter):
    """Redis queue adapter - wrapper for existing Redis functionality"""

    def __init__(self):
        self.redis_url = settings.redis_url
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        """Initialize Redis connection"""
        self.client = redis.from_url(self.redis_url, decode_responses=True)
        await self.client.ping()
        logger.info("redis_queue_connected", url=self.redis_url)

    async def disconnect(self) -> None:
        """Close Redis connection"""
        if self.client:
            await self.client.close()

    async def push_task(self, task: Dict[str, Any], topic: str = "pending") -> None:
        """Push task to Redis queue (backward compatible)"""
        if not self.client:
            await self.connect()

        queue_map = {
            "pending": settings.queue_pending,
            "retry": settings.queue_retry,
            "dlq": settings.queue_dead_letter
        }

        queue_key = queue_map.get(topic, settings.queue_pending)
        await self.client.lpush(queue_key, json.dumps(task))

        logger.info("task_pushed_to_redis",
                    task_id=task.get("id"),
                    queue=queue_key)

    async def consume_tasks(self, callback: Callable, topics: List[str] = None) -> None:
        """Consume tasks from Redis (backward compatible)"""
        if not self.client:
            await self.connect()

        if not topics:
            topics = ["pending", "retry"]

        queue_map = {
            "pending": settings.queue_pending,
            "retry": settings.queue_retry
        }

        queues = [queue_map[t] for t in topics if t in queue_map]

        while True:
            result = await self.client.brpop(queues, timeout=5)
            if result:
                queue_name, task_json = result
                task = json.loads(task_json)

                logger.debug("redis_message_received",
                             queue=queue_name,
                             task_id=task.get("id"))

                await callback(task)

    async def acknowledge(self, message: Any) -> None:
        """No-op for Redis (already removed from queue)"""
        pass