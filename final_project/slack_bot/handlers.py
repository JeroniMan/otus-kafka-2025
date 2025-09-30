# File: slack_bot/handlers.py - ПОЛНЫЙ ФАЙЛ

"""
Slack event handlers
"""

import json
import time
from datetime import datetime
from uuid import uuid4

import structlog
from slack_bolt.app.async_app import AsyncApp
from slack_sdk.errors import SlackApiError

from slack_bot.config import settings
from slack_bot.redis_client import RedisClient
from slack_bot.formatters import format_initial_response, format_error_response

logger = structlog.get_logger()


def register_handlers(app: AsyncApp):
    """Register all Slack event handlers"""

    # Message handlers
    register_message_handlers(app)

    # Command handlers
    register_command_handlers(app)

    # Action handlers (buttons, etc)
    register_action_handlers(app)

    logger.info("handlers_registered")


def register_message_handlers(app: AsyncApp):
    """Register message event handlers"""

    @app.event("app_mention")
    async def handle_app_mention(event, say, ack):
        """Handle @mentions of the bot"""
        await ack()
        await process_message(event, say, "mention")

    @app.event("message")
    async def handle_direct_message(event, say, ack):
        """Handle direct messages to the bot"""
        await ack()

        # Only process DMs (no channel_type means DM)
        if "channel_type" not in event or event["channel_type"] == "im":
            await process_message(event, say, "direct_message")


def register_command_handlers(app: AsyncApp):
    """Register slash command handlers"""

    @app.command("/ask")
    async def handle_ask_command(ack, command, say):
        """Handle /ask command"""
        await ack()

        # Convert command to event-like structure
        event = {
            "text": command["text"],
            "user": command["user_id"],
            "channel": command["channel_id"],
            "ts": str(time.time()),
            "team": command["team_id"],
        }

        await process_message(event, say, "slash_command")

    @app.command("/context")
    async def handle_context_command(ack, command, client):  # ИСПРАВЛЕНО: client отдельный параметр
        """Show current session context"""
        await ack()

        redis_client = RedisClient()

        team_id = command["team_id"]
        channel_id = command["channel_id"]
        user_id = command["user_id"]

        # Ищем все сессии
        patterns = [
            f"session:thread:{team_id}:{channel_id}:*",
            f"session:dm:{team_id}:{user_id}",
            f"session:ephemeral:{team_id}:{channel_id}:{user_id}"
        ]

        response = "*📋 Your Active Sessions:*\n\n"
        found = False

        for pattern in patterns:
            keys = await redis_client.redis.keys(pattern)
            for key in keys[:3]:
                session_data = await redis_client.redis.get(key)
                if session_data:
                    try:
                        session = json.loads(session_data)
                        found = True

                        response += f"*Type:* `{session.get('session_type', 'unknown')}`\n"

                        if session.get('thread_ts'):
                            response += f"*Thread:* {session['thread_ts']}\n"

                        queries = session.get('queries', [])
                        response += f"*Queries:* {len(queries)}\n"

                        context = session.get('context', {})
                        if context.get('last_dataset'):
                            response += f"*Dataset:* `{context['last_dataset']}`\n"
                        if context.get('last_tables'):
                            tables = context['last_tables'][:3]
                            response += f"*Tables:* `{', '.join(tables)}`\n"

                        if queries:
                            last = queries[-1]
                            response += f"*Last Query:* _{last['query'][:50]}..._\n"
                            response += f"*Result:* {last['row_count']} rows\n"

                        response += "\n"
                    except Exception as e:
                        logger.error("error_parsing_session", error=str(e), key=key)

        if not found:
            response = "📭 No active sessions found. Start asking questions to create one!"

        # ИСПРАВЛЕНО: используем client напрямую
        await client.chat_postMessage(
            channel=channel_id,
            text=response
        )

    @app.command("/clear-context")
    async def handle_clear_context_command(ack, command, client):  # ИСПРАВЛЕНО
        """Clear session context"""
        await ack()

        redis_client = RedisClient()

        team_id = command["team_id"]
        channel_id = command["channel_id"]
        user_id = command["user_id"]

        patterns = [
            f"session:thread:{team_id}:{channel_id}:*",
            f"session:thread:{team_id}:*:*",
            f"session:dm:{team_id}:{user_id}",
            f"session:ephemeral:{team_id}:*:{user_id}"
        ]

        cleared = 0
        for pattern in patterns:
            keys = await redis_client.redis.keys(pattern)
            for key in keys:
                if user_id in key or channel_id in key:
                    await redis_client.redis.delete(key)
                    cleared += 1

        # ИСПРАВЛЕНО: используем client напрямую
        await client.chat_postMessage(
            channel=channel_id,
            text=f"✅ Cleared {cleared} session(s). Starting fresh!"
        )

    @app.command("/debug")
    async def handle_debug_command(ack, command, client):  # ИСПРАВЛЕНО
        """Show debug info"""
        await ack()

        redis_client = RedisClient()
        channel_id = command["channel_id"]

        # Показываем последние задачи
        task_keys = await redis_client.redis.keys("task:*:status")

        response = "*📊 Debug Information:*\n\n"

        if task_keys:
            latest_key = sorted(task_keys)[-1] if task_keys else None
            if latest_key:
                task_data = await redis_client.redis.get(latest_key)

                if task_data:
                    try:
                        task = json.loads(task_data)

                        response += "*Last Task:*\n"
                        response += f"• Status: `{task.get('status')}`\n"
                        response += f"• Task ID: `{latest_key.split(':')[1]}`\n"
                        response += f"• Worker: `{task.get('worker_id')}`\n"
                        response += f"• Updated: {task.get('updated_at', 'unknown')}\n"

                        if task.get('error'):
                            response += f"• Error: _{task['error'][:200]}..._\n"
                    except Exception as e:
                        response += f"Error parsing task data: {str(e)}\n"
                else:
                    response += "No task data found\n"
        else:
            response += "No recent tasks found\n"

        # ИСПРАВЛЕНО: используем client напрямую
        await client.chat_postMessage(
            channel=channel_id,
            text=response
        )

def register_action_handlers(app: AsyncApp):
    """Register interactive component handlers"""

    @app.action("execute_query")
    async def handle_execute_button(ack, body, client):
        """Handle 'Execute Query' button click"""
        await ack()

        redis_client = RedisClient()

        try:
            # Get interaction context
            interaction_id = body["actions"][0]["value"]
            session_data = await redis_client.get_session(interaction_id)

            if not session_data:
                await client.chat_postMessage(
                    channel=body["channel"]["id"],
                    thread_ts=body["message"]["thread_ts"],
                    text="❌ Session expired. Please submit your query again."
                )
                return

            # Create execution task
            task = {
                "id": str(uuid4()),
                "type": "execute_confirmed",
                "sql": session_data["sql"],
                "original_question": session_data.get("question"),
                "slack_context": json.loads(session_data["slack_context"]),
                "created_at": datetime.utcnow().isoformat(),
                "priority": "high"
            }

            # Queue for execution
            await redis_client.queue_task(task, priority="high")

            # Update message to show processing
            await client.chat_update(
                channel=body["channel"]["id"],
                ts=body["message"]["ts"],
                text="✅ Query execution started...",
                blocks=None  # Clear buttons
            )

            logger.info("query_execution_confirmed",
                        task_id=task["id"],
                        interaction_id=interaction_id)

        except Exception as e:
            logger.error("execute_button_error", error=str(e))
            await client.chat_postMessage(
                channel=body["channel"]["id"],
                thread_ts=body["message"]["thread_ts"],
                text=format_error_response(str(e))
            )

    @app.action("cancel_query")
    async def handle_cancel_button(ack, body, client):
        """Handle 'Cancel Query' button click"""
        await ack()

        # Update message to show cancelled
        await client.chat_update(
            channel=body["channel"]["id"],
            ts=body["message"]["ts"],
            text="❌ Query cancelled",
            blocks=None
        )

        logger.info("query_cancelled",
                    interaction_id=body["actions"][0]["value"])


async def process_message(event: dict, say, source: str):
    """Process incoming message and queue for analysis"""

    redis_client = RedisClient()

    try:
        # Extract question (remove bot mention if present)
        question = event["text"]
        if source == "mention":
            # Remove bot mention from text
            question = question.split(">", 1)[-1].strip()

        if not question:
            await say("Please provide a question for me to analyze.")
            return

        # Check rate limiting
        user_id = event["user"]
        is_allowed, reason = await redis_client.check_rate_limit(user_id)

        if not is_allowed:
            await say(f"⚠️ {reason}")
            return

        # Send initial response
        initial_response = await say(
            text=format_initial_response(),
            thread_ts=event.get("thread_ts", event["ts"])
        )

        # Create task
        task = {
            "id": str(uuid4()),
            "type": "analytics_query",
            "question": question,
            "slack_context": {
                "channel": event["channel"],
                "thread_ts": event.get("thread_ts", event["ts"]),
                "message_ts": initial_response["ts"],
                "user": user_id,
                "team": event.get("team"),
            },
            "created_at": datetime.utcnow().isoformat(),
            "retry_count": 0,
            "source": source,
        }

        # Queue task
        await redis_client.queue_task(task)

        # Store task metadata
        await redis_client.set_task_status(task["id"], "queued", task)

        logger.info("message_queued",
                    task_id=task["id"],
                    source=source,
                    question_length=len(question),
                    user=user_id,
                    channel=event["channel"])

    except Exception as e:
        logger.error("process_message_error",
                     error=str(e),
                     source=source)

        await say(
            text=format_error_response(str(e)),
            thread_ts=event.get("thread_ts", event["ts"])
        )