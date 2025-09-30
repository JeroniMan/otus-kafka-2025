"""
Redis client wrapper for Slack bot
"""

import json
import time
from datetime import datetime
from typing import Dict, Tuple, Optional, Any
import os
import redis.asyncio as redis
import structlog

from slack_bot.config import settings

logger = structlog.get_logger()
from shared.queue import get_queue_adapter


class RedisClient:
    """Redis client with Kafka support"""

    def __init__(self):
        """Initialize Redis client and queue adapter"""
        self.redis = redis.Redis(connection_pool=self.get_pool())
        self.queue_adapter = None  # Инициализируется при первом использовании

    async def queue_task(self, task: Dict, priority: str = "normal") -> None:
        """Add task to queue (Redis or Kafka based on config)"""

        # Проверяем какой backend используется
        if settings.queue_backend == "kafka":
            # Используем Kafka
            if not self.queue_adapter:
                from shared.queue import get_queue_adapter
                self.queue_adapter = get_queue_adapter()
                await self.queue_adapter.connect()

            # Отправляем в соответствующий топик
            topic_map = {
                "high": "pending",
                "normal": "pending",
                "low": "retry"
            }
            topic = topic_map.get(priority, "pending")

            await self.queue_adapter.push_task(task, topic=topic)

            logger.info("task_queued_to_kafka",
                        task_id=task["id"],
                        topic=topic,
                        priority=priority)
        else:
            # Используем Redis (существующий код)
            queue_key = {
                "high": settings.queue_pending,
                "normal": settings.queue_pending,
                "low": settings.queue_retry,
            }.get(priority, settings.queue_pending)

            await self.redis.lpush(queue_key, json.dumps(task))

            logger.info("task_queued_to_redis",
                        task_id=task["id"],
                        queue=queue_key,
                        priority=priority)

class RedisClient:
    """Redis client with connection pooling"""

    _pool = None

    @classmethod
    def get_pool(cls):
        """Get or create connection pool"""
        if cls._pool is None:
            cls._pool = redis.ConnectionPool.from_url(
                settings.redis_url,
                max_connections=settings.redis_max_connections,
                decode_responses=settings.redis_decode_responses
            )
        return cls._pool

    def __init__(self):
        """Initialize Redis client"""
        self.redis = redis.Redis(connection_pool=self.get_pool())

    async def queue_task(self, task: Dict, priority: str = "normal") -> None:
        """Add task to queue (Redis or Kafka based on config)"""

        # Проверяем настройку
        backend = os.getenv("QUEUE_BACKEND", "redis")

        if backend == "kafka":
            # Временное решение - используем kafka-python для простоты
            from kafka import KafkaProducer
            import json

            producer = KafkaProducer(
                bootstrap_servers='kafka:29092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            # Отправляем в Kafka
            producer.send('analytics.tasks.pending', task)
            producer.flush()

            logger.info("task_queued_to_kafka",
                        task_id=task["id"],
                        backend="kafka")
        else:
            # Существующий Redis код
            queue_key = settings.queue_pending
            await self.redis.lpush(queue_key, json.dumps(task))

            logger.info("task_queued_to_redis",
                        task_id=task["id"],
                        queue=queue_key)

    async def set_task_status(self, task_id: str, status: str, metadata: Dict = None) -> None:
        """Update task status and metadata"""

        key = f"task:{task_id}"
        data = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat()
        }

        if metadata:
            # Store full task data
            data["metadata"] = json.dumps(metadata)

        await self.redis.hset(key, mapping=data)
        await self.redis.expire(key, 3600)  # 1 hour TTL

        logger.debug("task_status_updated",
                     task_id=task_id,
                     status=status)

    async def get_task_status(self, task_id: str) -> Optional[Dict]:
        """Get task status and metadata"""

        key = f"task:{task_id}"
        data = await self.redis.hgetall(key)

        if data and "metadata" in data:
            data["metadata"] = json.loads(data["metadata"])

        return data

    async def check_rate_limit(self, user_id: str) -> Tuple[bool, str]:
        """Check if user is within rate limits"""

        now = int(time.time())

        # Per-minute check
        minute_key = f"rate:{user_id}:minute:{now // 60}"
        minute_count = await self.redis.incr(minute_key)
        await self.redis.expire(minute_key, 60)

        if minute_count > settings.rate_limit_per_minute:
            return False, f"Rate limit exceeded: max {settings.rate_limit_per_minute} requests per minute"

        # Per-day check
        today = datetime.now().strftime("%Y-%m-%d")
        day_key = f"rate:{user_id}:day:{today}"
        day_count = await self.redis.incr(day_key)
        await self.redis.expire(day_key, 86400)

        if day_count > settings.rate_limit_per_day:
            return False, f"Daily limit exceeded: max {settings.rate_limit_per_day} requests per day"

        return True, "OK"

    async def save_session(self, interaction_id: str, data: Dict) -> None:
        """Save interaction session data"""

        key = f"session:{interaction_id}"

        # Convert nested dicts to JSON strings
        formatted_data = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                formatted_data[k] = json.dumps(v)
            else:
                formatted_data[k] = v

        await self.redis.hset(key, mapping=formatted_data)
        await self.redis.expire(key, 3600)  # 1 hour TTL

        logger.debug("session_saved", interaction_id=interaction_id)

    async def get_session(self, interaction_id: str) -> Optional[Dict]:
        """Get interaction session data"""

        key = f"session:{interaction_id}"
        data = await self.redis.hgetall(key)

        # Parse JSON fields
        if data:
            for k, v in data.items():
                if k in ["slack_context", "dry_run_result"]:
                    try:
                        data[k] = json.loads(v)
                    except:
                        pass

        return data

    async def publish_update(self, update: Dict) -> None:
        """Publish update to Slack bot via pub/sub"""

        await self.redis.publish(
            settings.pubsub_channel,
            json.dumps(update)
        )

        logger.debug("update_published",
                     type=update.get("type"),
                     task_id=update.get("task_id"))

    async def get_queue_stats(self) -> Dict:
        """Get queue statistics"""

        stats = {
            "pending": await self.redis.llen(settings.queue_pending),
            "processing": await self.redis.llen(settings.queue_processing),
            "retry": await self.redis.llen(settings.queue_retry),
            "dlq": await self.redis.llen(settings.queue_dlq),
            "timestamp": datetime.utcnow().isoformat()
        }

        return stats

    async def close(self):
        """Close Redis connection"""
        await self.redis.close()