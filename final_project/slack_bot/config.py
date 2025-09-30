"""
Configuration for Slack Bot
"""

from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Settings(BaseSettings):
    """Application settings"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # Игнорируем лишние переменные окружения
    )

    # Slack
    slack_bot_token: str = Field(..., description="Slack Bot OAuth Token")
    slack_app_token: str = Field(..., description="Slack App-Level Token for Socket Mode")
    slack_allowed_channels: Optional[str] = Field(
        default=None,
        description="Comma-separated list of allowed channels"
    )
    slack_allowed_users: Optional[str] = Field(
        default=None,
        description="Comma-separated list of allowed user IDs"
    )

    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL"
    )
    redis_max_connections: int = Field(
        default=50,
        description="Max Redis connections in pool"
    )
    redis_decode_responses: bool = Field(
        default=True,
        description="Decode Redis responses to strings"
    )

    # Queue settings
    queue_pending: str = Field(
        default="analytics:queue:pending",
        description="Redis key for pending tasks queue"
    )
    queue_processing: str = Field(
        default="analytics:queue:processing",
        description="Redis key for processing tasks"
    )
    queue_retry: str = Field(
        default="analytics:queue:retry",
        description="Redis key for retry queue"
    )
    queue_dlq: str = Field(
        default="analytics:queue:dlq",
        description="Dead letter queue"
    )

    # Pub/Sub channels
    pubsub_channel: str = Field(
        default="slack:updates",
        description="Redis pub/sub channel for worker updates"
    )

    # Rate limiting
    rate_limit_per_minute: int = Field(
        default=10,
        description="Max requests per minute per user"
    )
    rate_limit_per_day: int = Field(
        default=100,
        description="Max requests per day per user"
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    log_json: bool = Field(
        default=False,
        description="Output logs in JSON format"
    )

    # Queue Backend
    queue_backend: str = Field(
        default="redis",
        alias="QUEUE_BACKEND",
        description="Queue backend: redis or kafka"
    )

    # Kafka settings (если используется)
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS"
    )

    def get_allowed_channels(self) -> List[str]:
        """Get list of allowed channels"""
        if not self.slack_allowed_channels:
            return []
        return [ch.strip() for ch in self.slack_allowed_channels.split(",") if ch.strip()]

    def get_allowed_users(self) -> Optional[List[str]]:
        """Get list of allowed users"""
        if not self.slack_allowed_users:
            return None
        return [u.strip() for u in self.slack_allowed_users.split(",") if u.strip()]



# Global settings instance
settings = Settings()