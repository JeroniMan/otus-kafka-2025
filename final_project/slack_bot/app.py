"""
Slack App initialization and configuration
"""

import structlog
from slack_bolt.app.async_app import AsyncApp

from slack_bot.config import settings
from slack_bot.handlers import register_handlers
from slack_bot.middleware import register_middleware

logger = structlog.get_logger()


def create_app() -> AsyncApp:
    """Create and configure Slack app"""

    logger.info("creating_slack_app")

    # Initialize app
    app = AsyncApp(
        token=settings.slack_bot_token,
        # Socket Mode doesn't require signing secret
        # but we set it for future webhook support
        signing_secret=None,
        # Disable built-in middleware we don't need
        process_before_response=True,
    )

    # Register custom middleware
    register_middleware(app)

    # Register event handlers
    register_handlers(app)

    logger.info("slack_app_created",
                handlers_registered=True,
                middleware_registered=True)

    return app