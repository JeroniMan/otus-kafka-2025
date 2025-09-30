"""
Redis Pub/Sub listener for worker updates
"""
import base64
import io
import traceback  # ДОБАВЛЕНО: импорт traceback
from datetime import datetime

import asyncio
import json
from typing import Optional

import redis.asyncio as redis
import structlog
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

from slack_bot.config import settings
from slack_bot.formatters import (
    format_status_update,
    format_query_result,
    format_confirmation_request,
    format_error_message
)

logger = structlog.get_logger()


class PubSubListener:
    """Listens to Redis pub/sub for worker updates"""

    def __init__(self, slack_client: AsyncWebClient):
        """Initialize listener"""
        self.slack_client = slack_client
        self.redis_client = None
        self.pubsub = None
        self.running = False
        self.task = None

    async def start(self):
        """Start listening to Redis pub/sub"""

        logger.info("starting_pubsub_listener",
                    channel=settings.pubsub_channel)

        self.running = True
        self.redis_client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()

        # Subscribe to channel
        await self.pubsub.subscribe(settings.pubsub_channel)

        # Start listener task
        self.task = asyncio.create_task(self._listen())

        logger.info("pubsub_listener_started")

    async def stop(self):
        """Stop listening"""

        logger.info("stopping_pubsub_listener")

        self.running = False

        if self.pubsub:
            await self.pubsub.unsubscribe()
            await self.pubsub.close()

        if self.redis_client:
            await self.redis_client.close()

        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

        logger.info("pubsub_listener_stopped")

    async def _listen(self):
        """Main listener loop"""

        while self.running:
            try:
                # Listen for messages with timeout
                message = await asyncio.wait_for(
                    self.pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=1.0
                )

                if message and message["type"] == "message":
                    await self._handle_message(message["data"])

            except asyncio.TimeoutError:
                # Normal timeout, continue
                continue
            except Exception as e:
                logger.error("pubsub_listener_error",
                             error=str(e))

                # Reconnect on error
                if self.running:
                    await asyncio.sleep(5)
                    await self._reconnect()

    async def _reconnect(self):
        """Reconnect to Redis"""

        logger.info("reconnecting_pubsub")

        try:
            # Close existing connections
            if self.pubsub:
                await self.pubsub.close()
            if self.redis_client:
                await self.redis_client.close()

            # Create new connections
            self.redis_client = redis.Redis.from_url(
                settings.redis_url,
                decode_responses=True
            )
            self.pubsub = self.redis_client.pubsub()
            await self.pubsub.subscribe(settings.pubsub_channel)

            logger.info("pubsub_reconnected")

        except Exception as e:
            logger.error("pubsub_reconnect_failed", error=str(e))

    async def _handle_message(self, data: str):
        """Handle incoming message from workers"""

        try:
            update = json.loads(data)

            logger.debug("handling_worker_update",
                         type=update.get("type"),
                         task_id=update.get("task_id"))

            # Route to appropriate handler
            handlers = {
                "status_update": self._handle_status_update,
                "query_result": self._handle_query_result,
                "request_confirmation": self._handle_confirmation_request,
                "error": self._handle_error,
            }

            handler = handlers.get(update["type"])
            if handler:
                await handler(update)
            else:
                logger.warning("unknown_update_type",
                               type=update["type"])

        except json.JSONDecodeError as e:
            logger.error("invalid_json_in_pubsub", error=str(e))
        except Exception as e:
            logger.error("handle_message_error", error=str(e))

    async def _handle_status_update(self, update: dict):
        """Handle status update from worker"""

        try:
            slack_context = update["slack_context"]

            await self.slack_client.chat_update(
                channel=slack_context["channel"],
                ts=slack_context["message_ts"],
                text=update["message"],
                blocks=format_status_update(update["message"])
            )

        except SlackApiError as e:
            logger.error("slack_update_failed",
                         error=str(e),
                         response=e.response)

    async def _handle_query_result(self, update: dict):
        """Handle query execution result"""

        try:
            slack_context = update["slack_context"]
            result = update["result"]


            # Format result message
            blocks = format_query_result(result)
            html_report_encoded = update.get("html_report_encoded")



            # Post result in thread
            await self.slack_client.chat_postMessage(
                channel=slack_context["channel"],
                thread_ts=slack_context["thread_ts"],
                text=update.get("message", "Query completed"),
                blocks=blocks
            )

            # Update original message
            await self.slack_client.chat_update(
                channel=slack_context["channel"],
                ts=slack_context["message_ts"],
                text="✅ Query completed - see results below"
            )

            # Если есть HTML отчет, загружаем его как файл
            if html_report_encoded:
                try:
                    # Декодируем HTML из base64
                    html_bytes = base64.b64decode(html_report_encoded)

                    # Создаем файл в памяти
                    file_buffer = io.BytesIO(html_bytes)

                    # Формируем имя файла
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"query_report_{timestamp}.html"

                    file_response = await self.slack_client.files_upload_v2(
                        channel=slack_context["channel"],
                        file=file_buffer,
                        filename=filename,
                        title=f"Query Report - {update.get('task_id', '')[:8]}",
                        initial_comment=None,  # Убираем initial_comment
                        thread_ts=slack_context.get("thread_ts") or slack_context.get("message_ts")
                    )

                    if file_response["ok"]:
                        # Добавляем блок со ссылкой на файл в конец
                        file_url = file_response["file"].get("permalink")

                        # ОБНОВЛЕНО: Форматируем ссылку как требуется
                        blocks.append({
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Full Report:* <{file_url}|{filename}>"
                            }
                        })

                except Exception as e:
                    logger.error("html_upload_error",
                                 task_id=update.get("task_id"),
                                 error=str(e),
                                 traceback=traceback.format_exc())

        except SlackApiError as e:
            logger.error("slack_result_failed",
                         error=str(e),
                         response=e.response)

    async def _handle_confirmation_request(self, update: dict):
        """Handle confirmation request from worker"""

        try:
            slack_context = update["slack_context"]

            # Format confirmation message with buttons
            blocks = format_confirmation_request(
                update["dry_run_result"],
                update["interaction_id"]
            )

            # Post confirmation request
            await self.slack_client.chat_postMessage(
                channel=slack_context["channel"],
                thread_ts=slack_context["thread_ts"],
                text=update.get("message", "Query validation complete"),
                blocks=blocks
            )

        except SlackApiError as e:
            logger.error("slack_confirmation_failed",
                         error=str(e),
                         response=e.response)

    async def _handle_error(self, update: dict):
        """Handle error from worker"""

        try:
            slack_context = update["slack_context"]

            # Update message with error
            await self.slack_client.chat_update(
                channel=slack_context["channel"],
                ts=slack_context["message_ts"],
                text=format_error_message(update["error"])
            )

            # Post detailed error in thread if available
            if update.get("details"):
                await self.slack_client.chat_postMessage(
                    channel=slack_context["channel"],
                    thread_ts=slack_context["thread_ts"],
                    text=f"```{update['details']}```"
                )

        except SlackApiError as e:
            logger.error("slack_error_failed",
                         error=str(e),
                         response=e.response)