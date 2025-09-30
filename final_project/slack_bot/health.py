"""
Health check endpoint for Slack bot
"""

from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

from slack_bot.config import settings
from slack_bot.redis_client import RedisClient


class HealthStatus(BaseModel):
    """Health check response model"""
    status: str
    timestamp: str
    checks: dict


def create_health_server(slack_app, pubsub_listener) -> FastAPI:
    """Create FastAPI app for health checks"""

    app = FastAPI(title="Slack Bot Health", version="1.0.0")

    @app.get("/health", response_model=HealthStatus)
    async def health_check():
        """Health check endpoint"""

        checks = {}
        overall_status = "healthy"

        # Check Redis connection
        try:
            redis_client = RedisClient()
            stats = await redis_client.get_queue_stats()
            checks["redis"] = {
                "status": "healthy",
                "queue_stats": stats
            }
        except Exception as e:
            checks["redis"] = {
                "status": "unhealthy",
                "error": str(e)
            }
            overall_status = "unhealthy"

        # Check Slack connection
        try:
            # Simple check if client exists
            if slack_app and slack_app.client:
                checks["slack"] = {
                    "status": "healthy",
                    "connected": True
                }
            else:
                checks["slack"] = {
                    "status": "unhealthy",
                    "connected": False
                }
                overall_status = "unhealthy"
        except Exception as e:
            checks["slack"] = {
                "status": "unhealthy",
                "error": str(e)
            }
            overall_status = "unhealthy"

        # Check PubSub listener
        try:
            if pubsub_listener and pubsub_listener.running:
                checks["pubsub"] = {
                    "status": "healthy",
                    "running": True
                }
            else:
                checks["pubsub"] = {
                    "status": "unhealthy",
                    "running": False
                }
                overall_status = "degraded"
        except Exception as e:
            checks["pubsub"] = {
                "status": "unhealthy",
                "error": str(e)
            }
            overall_status = "degraded"

        return HealthStatus(
            status=overall_status,
            timestamp=datetime.utcnow().isoformat(),
            checks=checks
        )

    @app.get("/ready")
    async def readiness_check():
        """Readiness check endpoint"""

        # Check if we can connect to Redis
        try:
            redis_client = RedisClient()
            await redis_client.get_queue_stats()
            return {"status": "ready"}
        except:
            return {"status": "not_ready"}, 503

    return app