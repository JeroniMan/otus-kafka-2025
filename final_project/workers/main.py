# File: workers/main.py - UPDATED VERSION with thread support

# !/usr/bin/env python3
"""
Worker - Consumer for AI Analytics Assistant
Processes tasks from queue and executes queries

UPDATED: Thread-aware session support
"""
import os

if os.getenv("LANGSMITH_TRACING", "").lower() == "true":
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY", "")
    os.environ["LANGCHAIN_PROJECT"] = os.getenv("LANGSMITH_PROJECT", "ai-analytics-assistant")
    os.environ["LANGCHAIN_ENDPOINT"] = os.getenv("LANGSMITH_ENDPOINT", "https://api.smith.langchain.com")
    print(f"✅ LangSmith enabled for project: {os.environ['LANGCHAIN_PROJECT']}")

import asyncio
import signal
import sys
import json
import traceback
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

import structlog

# Add shared module to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from workers.config import settings
from workers.orchestrator import TaskOrchestrator
from workers.communicator import SlackCommunicator

from typing import Dict, Optional
from shared.queue import get_queue_adapter, QueueBackend

# IMPORTANT: Set Python logging level based on config
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(message)s',
    force=True  # Override any existing configuration
)

# Configure structlog with proper processors
processors = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
]

# Add appropriate renderer based on config
if settings.log_json:
    processors.append(structlog.processors.JSONRenderer())
else:
    processors.append(structlog.dev.ConsoleRenderer(colors=True))

structlog.configure(
    processors=processors,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Log startup configuration
logger.info("=" * 60)
logger.info("AI Analytics Assistant Worker Starting")
logger.info("=" * 60)
logger.info("Configuration:",
            log_level=settings.log_level,
            log_json=settings.log_json,
            monitoring_backend=settings.monitoring_backend,
            openai_model=settings.openai_model,
            bq_project=settings.bq_source_project,
            qdrant_enabled=settings.qdrant_enabled)
logger.info("=" * 60)


class Worker:
    """Main worker class that processes tasks from queue"""

    def __init__(self, worker_id: Optional[str] = None):
        """Initialize worker with queue adapter support"""
        self.worker_id = worker_id or settings.worker_id or f"worker-{datetime.now().timestamp()}"
        self.redis_client = None
        self.orchestrator = None
        self.communicator = None
        self.running = True
        self.queue_adapter = None

        # Log worker initialization
        logger.info("🔧 Initializing worker",
                    worker_id=self.worker_id,
                    max_retries=settings.max_retries,
                    queue_backend=settings.queue_backend)

    async def start(self):
        """Start the worker with queue adapter support"""
        logger.info("🚀 Starting worker",
                    worker_id=self.worker_id,
                    queue_backend=settings.queue_backend)

        try:
            # Initialize Queue Adapter
            from shared.queue import get_queue_adapter
            self.queue_adapter = get_queue_adapter()
            await self.queue_adapter.connect()
            logger.info("✅ Queue adapter connected",
                        backend=settings.queue_backend)

            # Initialize Redis for non-queue operations (status updates, sessions)
            import redis.asyncio as redis
            self.redis_client = redis.Redis.from_url(
                settings.redis_url,
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info("✅ Redis connected for pub/sub and sessions")

            # Initialize components
            logger.info("🔧 Initializing orchestrator...")
            self.orchestrator = TaskOrchestrator()
            logger.info("✅ Orchestrator initialized")

            logger.info("🔧 Initializing Slack communicator...")
            self.communicator = SlackCommunicator(self.redis_client)
            logger.info("✅ Slack communicator initialized")

            # Setup signal handlers
            self._setup_signal_handlers()

            # Start processing loop
            logger.info("=" * 60)
            logger.info("✅ Worker ready! Waiting for tasks...")
            logger.info(f"📡 Queue backend: {settings.queue_backend}")
            logger.info("=" * 60)

            if settings.queue_backend == "kafka":
                await self.process_with_adapter()
            else:
                await self.process_loop()

        except Exception as e:
            logger.error("❌ Failed to start worker",
                         error=str(e),
                         error_type=type(e).__name__,
                         traceback=traceback.format_exc())
            raise

    async def process_with_adapter(self):
        """Process tasks using queue adapter (Kafka support)"""
        logger.info("🔄 Starting Kafka consumer loop",
                    worker_id=self.worker_id)

        async def process_callback(task: Dict):
            """Callback for processing each task from Kafka"""
            try:
                # Конвертируем task обратно в JSON строку для совместимости
                # с существующим методом process_task
                task_json = json.dumps(task)

                logger.info("📥 Task received from Kafka",
                            task_id=task.get("id"),
                            task_size=len(task_json))

                # Используем существующий метод process_task
                await self.process_task(task_json)

                # Подтверждаем обработку сообщения в Kafka
                await self.queue_adapter.acknowledge(task)

            except Exception as e:
                logger.error("❌ Error processing Kafka message",
                             error=str(e),
                             error_type=type(e).__name__,
                             task_id=task.get("id"),
                             traceback=traceback.format_exc())

                # В случае ошибки не подтверждаем сообщение,
                # оно будет переобработано
                if task:
                    # Увеличиваем счетчик попыток
                    retry_count = task.get("retry_count", 0)
                    if retry_count < settings.max_retries:
                        task["retry_count"] = retry_count + 1
                        # Отправляем в retry топик
                        await self.queue_adapter.push_task(task, topic="retry")
                    else:
                        # Отправляем в DLQ
                        await self.queue_adapter.push_task(task, topic="dlq")

        try:
            # Запускаем потребление сообщений из Kafka
            while self.running:
                try:
                    # consume_tasks будет блокировать и вызывать callback для каждого сообщения
                    await self.queue_adapter.consume_tasks(
                        callback=process_callback,
                        topics=["pending", "retry"]
                    )
                except asyncio.CancelledError:
                    logger.info("🛑 Kafka consumer cancelled")
                    break
                except Exception as e:
                    logger.error("❌ Kafka consumer error, restarting...",
                                 error=str(e),
                                 error_type=type(e).__name__)
                    await asyncio.sleep(5)  # Пауза перед переподключением

        except Exception as e:
            logger.error("❌ Fatal error in Kafka consumer",
                         error=str(e),
                         traceback=traceback.format_exc())
            raise

    async def process_loop(self):
        """Main processing loop"""

        while self.running:
            try:
                # Log every 10th iteration to show we're alive
                if hasattr(self, '_loop_count'):
                    self._loop_count += 1
                else:
                    self._loop_count = 0

                if self._loop_count % 10 == 0:
                    logger.debug("🔄 Polling for tasks...",
                                 worker_id=self.worker_id,
                                 queues=[settings.queue_pending, settings.queue_retry])

                # Block waiting for task (with timeout for graceful shutdown)
                result = await self.redis_client.brpop(
                    [settings.queue_pending, settings.queue_retry],
                    timeout=5
                )

                if result:
                    queue_name, task_json = result
                    logger.info("📥 Task received from queue",
                                queue=queue_name,
                                task_size=len(task_json))
                    await self.process_task(task_json)

            except asyncio.CancelledError:
                logger.info("🛑 Processing loop cancelled")
                break

            except Exception as e:
                logger.error("❌ Error in process loop",
                             error=str(e),
                             error_type=type(e).__name__,
                             worker_id=self.worker_id)
                await asyncio.sleep(1)

    async def process_task(self, task_json: str):
        """Process a single task"""
        task = None
        try:
            # Parse task
            task = json.loads(task_json)
            task_id = task.get("id", "unknown")

            logger.info("=" * 50)
            logger.info("📋 Processing task",
                        task_id=task_id,
                        type=task.get("type"),
                        question_preview=task.get("question", "")[:100],
                        worker_id=self.worker_id)
            logger.info("=" * 50)

            # Update task status
            await self.update_task_status(task_id, "processing")

            # Notify Slack that we're processing
            await self.communicator.send_status_update(
                task,
                "⚙️ Processing your request..."
            )

            # Process the task based on type
            if task["type"] == "analytics_query":
                await self.process_analytics_query(task)
            elif task["type"] == "execute_confirmed":
                await self.process_execute_confirmed(task)
            else:
                logger.warning("⚠️ Unknown task type",
                               type=task.get("type"),
                               task_id=task_id)
                await self.communicator.send_error(
                    task,
                    f"Unknown task type: {task.get('type')}"
                )

            # Mark as completed
            await self.update_task_status(task_id, "completed")

            logger.info("✅ Task completed successfully",
                        task_id=task_id)

        except Exception as e:
            logger.error("❌ Task processing error",
                         error=str(e),
                         error_type=type(e).__name__,
                         task=task,
                         worker_id=self.worker_id,
                         traceback=traceback.format_exc())

            if task:
                await self.handle_task_error(task, str(e))

    async def process_analytics_query(self, task: Dict):
        """
        Process an analytics query task

        UPDATED: Added thread_ts to metadata for thread-aware sessions
        """
        try:
            logger.info("🔍 Starting analytics query processing",
                        task_id=task["id"],
                        question=task["question"])

            # Update status
            await self.communicator.send_status_update(
                task,
                "🔍 Analyzing your question and generating SQL..."
            )

            # Run orchestration
            logger.info("🧠 Running orchestrator...", task_id=task["id"])

            # UPDATED: Extract slack_context and include thread_ts
            slack_context = task.get("slack_context", {})

            # Build metadata with thread awareness
            metadata = {
                "user_id": slack_context.get("user", "unknown"),
                "team_id": slack_context.get("team", "unknown"),
                "channel_id": slack_context.get("channel", "unknown"),
                "thread_ts": slack_context.get("thread_ts"),  # ADDED: thread timestamp
                "slack_context": slack_context  # Include full context
            }

            # Log if we're in a thread
            if metadata["thread_ts"]:
                logger.info("📌 Processing query in thread",
                            task_id=task["id"],
                            thread_ts=metadata["thread_ts"])

            result = await self.orchestrator.process_query(
                task["question"],  # query
                task["id"],  # task_id
                metadata=metadata  # UPDATED: includes thread_ts
            )

            # Check if result is None
            if result is None:
                logger.error("orchestrator_returned_none", task_id=task["id"])
                await self.communicator.send_error(
                    task,
                    "Query processing failed: No result from orchestrator",
                    details="The query processor did not return any results. Please try again."
                )
                return

            # Check for errors
            if result.get("error"):
                error_msg = result.get("error", "Unknown error")
                logger.error("orchestrator_error",
                             task_id=task["id"],
                             error=error_msg,
                             error_details=result.get("error_details"))

                # Provide user-friendly error messages
                if "permission" in error_msg.lower():
                    await self.communicator.send_error(
                        task,
                        "Permission denied. The service account might not have access to the requested table.",
                        details=error_msg if settings.log_level == "DEBUG" else None
                    )
                elif "not found" in error_msg.lower():
                    await self.communicator.send_error(
                        task,
                        "Table or dataset not found. Please check if the table exists.",
                        details=error_msg if settings.log_level == "DEBUG" else None
                    )
                elif "timeout" in error_msg.lower():
                    await self.communicator.send_error(
                        task,
                        "Query took too long to execute. Try simplifying your question or adding specific filters.",
                        details=error_msg if settings.log_level == "DEBUG" else None
                    )
                else:
                    await self.communicator.send_error(
                        task,
                        f"Query failed: {error_msg}",
                        details=result.get("error_details") if settings.log_level == "DEBUG" else None
                    )
                return

            # Check if confirmation is needed
            if result.get("needs_confirmation"):
                logger.info("💰 Confirmation needed for expensive query",
                            task_id=task["id"],
                            estimated_cost=result.get("estimated_cost"),
                            reason=result.get("confirmation_reason"))

                await self.communicator.send_confirmation_request(
                    task,
                    result.get("dry_run_result", {}),
                    result.get("sql", ""),
                    result.get("confirmation_reason", "Query requires confirmation")
                )
                return

            # Send successful result
            logger.info("📊 Sending query results",
                        task_id=task["id"],
                        total_rows=result.get("total_rows", 0))

            await self.communicator.send_query_result(task, result)

        except Exception as e:
            logger.error("process_analytics_error",
                         error=str(e),
                         error_type=type(e).__name__,
                         task_id=task.get("id"),
                         traceback=traceback.format_exc())

            await self.communicator.send_error(
                task,
                "An unexpected error occurred",
                details=str(e) if settings.log_level == "DEBUG" else None
            )

    async def process_execute_confirmed(self, task: Dict):
        """Process a confirmed query execution"""
        try:
            logger.info("⚡ Executing confirmed query",
                        task_id=task["id"])

            # Execute directly
            result = await self.orchestrator.execute_sql(
                task["sql"],
                task["id"]
            )

            # Send results
            await self.communicator.send_query_result(task, result)

        except Exception as e:
            logger.error("execute_confirmed_error",
                         error=str(e),
                         task_id=task.get("id"))

            await self.communicator.send_error(
                task,
                f"Execution failed: {str(e)}"
            )

    async def handle_task_error(self, task: Dict, error: str):
        """Handle task processing error"""
        task_id = task.get("id", "unknown")

        # Update status
        await self.update_task_status(task_id, "failed", {"error": error})

        # Should we retry?
        retry_count = task.get("retry_count", 0)

        if retry_count < settings.max_retries:
            logger.info("🔄 Retrying task",
                        task_id=task_id,
                        retry_count=retry_count + 1)

            task["retry_count"] = retry_count + 1
            await self.redis_client.lpush(
                settings.queue_retry,
                json.dumps(task)
            )
        else:
            logger.error("☠️ Task failed after max retries",
                         task_id=task_id,
                         retries=retry_count)

            # Move to dead letter queue
            await self.redis_client.lpush(
                settings.queue_dead_letter,
                json.dumps(task)
            )

            # Notify user of failure
            await self.communicator.send_error(
                task,
                "Query failed after multiple attempts. Please try again later or contact support.",
                details=error if settings.log_level == "DEBUG" else None
            )

    async def update_task_status(self, task_id: str, status: str, metadata: Optional[Dict] = None):
        """Update task status in Redis"""
        key = f"task:{task_id}:status"
        value = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat(),
            "worker_id": self.worker_id
        }

        if metadata:
            value.update(metadata)

        await self.redis_client.setex(
            key,
            3600,  # 1 hour TTL
            json.dumps(value)
        )

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""

        def signal_handler(sig, frame):
            logger.info("🛑 Shutdown signal received",
                        signal=sig)
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def stop(self):
        """Stop the worker gracefully"""
        logger.info("🛑 Stopping worker...",
                    worker_id=self.worker_id)

        self.running = False

        # Close queue adapter connection
        if self.queue_adapter:
            await self.queue_adapter.disconnect()
            logger.info("✅ Queue adapter disconnected")

        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()
            logger.info("✅ Redis disconnected")

        logger.info("✅ Worker stopped",
                    worker_id=self.worker_id)


async def main():
    """Main entry point"""
    worker = Worker()

    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("⌨️ Keyboard interrupt received")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())