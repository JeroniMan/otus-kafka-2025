#!/usr/bin/env python3
"""
Slack Bot - Producer for AI Analytics Assistant
Receives messages from Slack and queues them for processing
"""

import asyncio
import signal
import sys
from pathlib import Path

import structlog
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

# Add shared module to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slack_bot.app import create_app
from slack_bot.config import settings
from slack_bot.listeners import PubSubListener
from slack_bot.health import create_health_server

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer() if settings.log_json else structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class SlackBotService:
    """Main service orchestrator for Slack Bot"""

    def __init__(self):
        self.app = None
        self.handler = None
        self.pubsub_listener = None
        self.health_server = None
        self.running = True

    async def start(self):
        """Start all components"""
        logger.info("starting_slack_bot",
                    redis_url=settings.redis_url,
                    allowed_channels=settings.slack_allowed_channels)

        # Create Slack app
        self.app = create_app()

        # Create Socket Mode handler
        self.handler = AsyncSocketModeHandler(
            app=self.app,
            app_token=settings.slack_app_token
        )

        # Create and start PubSub listener
        self.pubsub_listener = PubSubListener(self.app.client)

        # Start health check server
        self.health_server = create_health_server(self.app, self.pubsub_listener)

        # Setup signal handlers
        self._setup_signal_handlers()

        # Start all components concurrently
        await asyncio.gather(
            self.handler.start_async(),
            self.pubsub_listener.start(),
            self._run_health_server(),
            return_exceptions=True
        )

    async def _run_health_server(self):
        """Run health check server"""
        import uvicorn
        config = uvicorn.Config(
            app=self.health_server,
            host="0.0.0.0",
            port=8080,
            log_level="warning"
        )
        server = uvicorn.Server(config)
        await server.serve()

    def _setup_signal_handlers(self):
        """Setup graceful shutdown"""

        def signal_handler(sig, frame):
            logger.info("received_shutdown_signal", signal=sig)
            self.running = False
            asyncio.create_task(self.shutdown())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("shutting_down_slack_bot")

        if self.pubsub_listener:
            await self.pubsub_listener.stop()

        if self.handler:
            await self.handler.close()

        logger.info("slack_bot_shutdown_complete")
        sys.exit(0)


async def main():
    """Main entry point"""
    service = SlackBotService()

    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_received")
    except Exception as e:
        logger.exception("fatal_error", error=str(e))
        sys.exit(1)
    finally:
        await service.shutdown()


if __name__ == "__main__":
    asyncio.run(main())