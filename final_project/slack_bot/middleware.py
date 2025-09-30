"""
Custom middleware for Slack app
"""

import structlog
from slack_bolt.app.async_app import AsyncApp

from slack_bot.config import settings

logger = structlog.get_logger()


def register_middleware(app: AsyncApp):
    """Register all custom middleware"""

    @app.middleware
    async def log_request(body, next, logger):
        """Log all incoming requests"""
        logger.debug("incoming_request",
                    type=body.get("type") if body else None,
                    user=body.get("user") if body else None,
                    channel=body.get("channel") if body else None,
                    team=body.get("team") if body else None)
        await next()

    @app.middleware
    async def check_channel_allowed(body, next, event):
        """Check if channel is in allowed list"""

        # Skip for non-message events
        if not event:
            await next()
            return

        # Get channel from event
        channel = event.get("channel")

        # DMs are always allowed (they don't have channel names)
        if not channel or channel.startswith("D"):
            await next()
            return

        # Get channel name (need to fetch if we only have ID)
        channel_name = await get_channel_name(app.client, channel)

        # Check if channel is allowed
        allowed_channels = settings.get_allowed_channels()
        if allowed_channels:
            if channel_name not in allowed_channels:
                logger.warning("channel_not_allowed",
                             channel=channel_name,
                             allowed=allowed_channels)
                # Don't process events from non-allowed channels
                return

        await next()

    @app.middleware
    async def check_user_allowed(body, next):
        """Check if user is in allowed list"""

        # Skip if no user restrictions
        allowed_users = settings.get_allowed_users()
        if not allowed_users:
            await next()
            return

        # Get user ID from body
        user_id = None
        if body:
            if "event" in body:
                user_id = body["event"].get("user")
            elif "user_id" in body:
                user_id = body["user_id"]
            elif "user" in body:
                user_id = body["user"]

        if user_id and user_id not in allowed_users:
            logger.warning("user_not_allowed",
                         user=user_id,
                         allowed=allowed_users)
            return

        await next()


async def get_channel_name(client, channel_id: str) -> str:
    """Get channel name from ID"""

    try:
        # Try to get channel info
        result = await client.conversations_info(channel=channel_id)
        return f"#{result['channel']['name']}"
    except:
        # Return ID if can't get name
        return channel_id