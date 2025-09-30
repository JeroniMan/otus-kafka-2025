"""
Configuration for Workers with Monitoring Support
Location: workers/config.py

UPDATED: Added Kafka queue backend support
"""

import os
from typing import Optional, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Settings(BaseSettings):
    """Worker settings with monitoring configuration"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # === Worker Settings ===
    worker_id: Optional[str] = Field(
        default=None,
        description="Unique worker ID"
    )
    worker_concurrency: int = Field(
        default=1,
        description="Number of concurrent tasks"
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts"
    )

    # === Queue Backend Configuration (NEW) ===
    queue_backend: Literal["redis", "kafka"] = Field(
        default="redis",
        alias="QUEUE_BACKEND",
        description="Queue backend to use: redis or kafka"
    )

    # === Redis Configuration ===
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL"
    )

    # Queue names (Redis)
    queue_pending: str = Field(
        default="analytics:queue:pending",
        description="Pending tasks queue"
    )
    queue_processing: str = Field(
        default="analytics:queue:processing",
        description="Processing tasks"
    )
    queue_retry: str = Field(
        default="analytics:queue:retry",
        description="Retry queue"
    )
    queue_dlq: str = Field(
        default="analytics:queue:dlq",
        description="Dead letter queue"
    )
    queue_dead_letter: str = Field(
        default="analytics:queue:dlq",
        description="Dead letter queue (alias)"
    )

    # Pub/Sub
    pubsub_channel: str = Field(
        default="slack:updates",
        description="Channel for Slack updates"
    )

    # === Kafka Configuration (NEW) ===
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
        description="Kafka bootstrap servers (comma-separated)"
    )
    kafka_consumer_group: str = Field(
        default="analytics-workers",
        alias="KAFKA_CONSUMER_GROUP",
        description="Kafka consumer group ID"
    )
    kafka_topic_pending: str = Field(
        default="analytics.tasks.pending",
        alias="KAFKA_TOPIC_PENDING",
        description="Kafka topic for pending tasks"
    )
    kafka_topic_retry: str = Field(
        default="analytics.tasks.retry",
        alias="KAFKA_TOPIC_RETRY",
        description="Kafka topic for retry tasks"
    )
    kafka_topic_dlq: str = Field(
        default="analytics.tasks.dlq",
        alias="KAFKA_TOPIC_DLQ",
        description="Kafka topic for dead letter queue"
    )
    kafka_topic_updates: str = Field(
        default="analytics.updates",
        alias="KAFKA_TOPIC_UPDATES",
        description="Kafka topic for worker updates to Slack"
    )
    kafka_compression_type: str = Field(
        default="gzip",
        alias="KAFKA_COMPRESSION_TYPE",
        description="Kafka compression type: none, gzip, snappy, lz4"
    )
    kafka_max_poll_records: int = Field(
        default=10,
        alias="KAFKA_MAX_POLL_RECORDS",
        description="Maximum records to poll at once"
    )
    kafka_auto_offset_reset: Literal["earliest", "latest"] = Field(
        default="earliest",
        alias="KAFKA_AUTO_OFFSET_RESET",
        description="Where to start reading: earliest or latest"
    )

    # === OpenAI Configuration ===
    openai_api_key: str = Field(
        default="sk-placeholder",
        description="OpenAI API key"
    )
    openai_model: str = Field(
        default="gpt-4o-mini",
        description="OpenAI model to use"
    )
    openai_temperature: float = Field(
        default=0.1,
        description="Temperature for SQL generation"
    )

    # === BigQuery Configuration ===
    bq_source_project: str = Field(
        default="p2p-data-warehouse",
        alias="BQ_SOURCE_PROJECT",
        description="GCP project for reading data"
    )
    bq_results_project: str = Field(
        default="p2p-data-ai",
        alias="BQ_RESULTS_PROJECT",
        description="GCP project for writing results"
    )
    bq_location: str = Field(
        default="US",
        alias="BQ_LOCATION",
        description="BigQuery location"
    )
    bq_results_dataset: str = Field(
        default="ai_analytics_assistant",
        alias="BQ_RESULTS_DATASET",
        description="Dataset for temporary tables"
    )
    bq_max_bytes_billed: int = Field(
        default=20_000_000_000,  # 20GB
        description="Max bytes to bill per query"
    )
    bq_timeout: int = Field(
        default=300,
        description="Query timeout in seconds"
    )
    max_query_cost: float = Field(
        default=5.0,
        description="Max query cost in USD"
    )

    # === Qdrant Configuration ===
    qdrant_enabled: bool = Field(
        default=True,
        description="Enable Qdrant RAG"
    )
    qdrant_url: str = Field(
        default="http://localhost:6333",
        description="Qdrant URL"
    )
    qdrant_api_key: Optional[str] = Field(
        default=None,
        description="Qdrant API key"
    )
    qdrant_collection: str = Field(
        default="sql_examples,dbt_models,table_schemas,business_knowledge",
        description="Qdrant collection names (comma-separated)"
    )

    # === Monitoring Backend Selection ===
    monitoring_backend: Literal["none", "langfuse", "langsmith", "both"] = Field(
        default="langsmith",
        alias="MONITORING_BACKEND",
        description="Which monitoring backend to use"
    )

    # === LangSmith Configuration (CORRECT ENV VARS) ===
    langsmith_tracing: bool = Field(
        default=True,
        alias="LANGSMITH_TRACING",
        description="Enable LangSmith tracing"
    )
    langsmith_api_key: Optional[str] = Field(
        default=None,
        alias="LANGSMITH_API_KEY",
        description="LangSmith API key"
    )
    langsmith_project: str = Field(
        default="ai-analytics-assistant",
        alias="LANGSMITH_PROJECT",
        description="LangSmith project name"
    )
    langsmith_endpoint: str = Field(
        default="https://api.smith.langchain.com",
        alias="LANGSMITH_ENDPOINT",
        description="LangSmith API endpoint"
    )

    # === LangFuse Configuration (optional) ===
    langfuse_enabled: bool = Field(
        default=False,
        description="Enable LangFuse tracing"
    )
    langfuse_public_key: Optional[str] = Field(
        default=None,
        alias="LANGFUSE_PUBLIC_KEY",
        description="LangFuse public key"
    )
    langfuse_secret_key: Optional[str] = Field(
        default=None,
        alias="LANGFUSE_SECRET_KEY",
        description="LangFuse secret key"
    )
    langfuse_host: str = Field(
        default="http://localhost:3000",
        alias="LANGFUSE_HOST",
        description="LangFuse host URL"
    )

    # === Logging Configuration (IMPORTANT FOR YOUR ISSUE) ===
    log_level: str = Field(
        default="INFO",
        alias="LOG_LEVEL",
        description="Log level (DEBUG, INFO, WARNING, ERROR)"
    )
    log_json: bool = Field(
        default=False,
        alias="LOG_JSON",
        description="Output logs as JSON (set to true for production)"
    )
    log_detailed_stages: bool = Field(
        default=True,
        description="Enable detailed stage logging"
    )
    log_sql_queries: bool = Field(
        default=True,
        description="Log generated SQL queries"
    )
    log_execution_time: bool = Field(
        default=True,
        description="Log execution time for each stage"
    )

    # === Advanced Monitoring Options ===
    trace_sample_rate: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Sampling rate for traces (0.0-1.0)"
    )

    slack_bot_token: str = Field(
        default="",
        description="Slack bot token for file uploads"
    )

    # === Validators ===
    @field_validator('queue_backend')
    @classmethod
    def validate_queue_backend(cls, v: str) -> str:
        """Validate queue backend selection"""
        valid_backends = ["redis", "kafka"]
        if v not in valid_backends:
            raise ValueError(f"queue_backend must be one of {valid_backends}, got {v}")
        return v

    @field_validator('monitoring_backend')
    @classmethod
    def validate_monitoring_backend(cls, v: str) -> str:
        """Validate monitoring backend selection"""
        valid_backends = ["none", "langfuse", "langsmith", "both"]
        if v not in valid_backends:
            raise ValueError(f"monitoring_backend must be one of {valid_backends}, got {v}")
        return v

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate and uppercase log level"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}, got {v}")
        return v_upper

    @field_validator('kafka_compression_type')
    @classmethod
    def validate_kafka_compression(cls, v: str) -> str:
        """Validate Kafka compression type"""
        valid_types = ["none", "gzip", "snappy", "lz4"]
        if v not in valid_types:
            raise ValueError(f"kafka_compression_type must be one of {valid_types}, got {v}")
        return v

    # === Properties ===
    @property
    def effective_langsmith_enabled(self) -> bool:
        """Check if LangSmith should be enabled"""
        return (
            self.monitoring_backend in ["langsmith", "both"] and
            self.langsmith_tracing and
            bool(self.langsmith_api_key)
        )

    @property
    def effective_langfuse_enabled(self) -> bool:
        """Check if LangFuse should be enabled"""
        return (
            self.monitoring_backend in ["langfuse", "both"] and
            self.langfuse_enabled and
            bool(self.langfuse_public_key and self.langfuse_secret_key)
        )

    @property
    def is_kafka_enabled(self) -> bool:
        """Check if Kafka queue backend is enabled"""
        return self.queue_backend == "kafka"

    @property
    def is_redis_queue_enabled(self) -> bool:
        """Check if Redis queue backend is enabled"""
        return self.queue_backend == "redis"

    # === Configuration Methods ===
    def get_monitoring_config(self) -> dict:
        """Get monitoring configuration as dict"""
        return {
            "backend": self.monitoring_backend,
            "langsmith": {
                "enabled": self.effective_langsmith_enabled,
                "project": self.langsmith_project,
                "endpoint": self.langsmith_endpoint,
                "has_key": bool(self.langsmith_api_key)
            },
            "langfuse": {
                "enabled": self.effective_langfuse_enabled,
                "host": self.langfuse_host,
                "has_keys": bool(self.langfuse_public_key and self.langfuse_secret_key)
            },
            "logging": {
                "level": self.log_level,
                "json": self.log_json,
                "detailed_stages": self.log_detailed_stages,
                "sql_queries": self.log_sql_queries,
                "execution_time": self.log_execution_time
            }
        }

    def get_queue_config(self) -> dict:
        """Get queue configuration as dict"""
        config = {
            "backend": self.queue_backend,
            "redis": {
                "url": self.redis_url,
                "queues": {
                    "pending": self.queue_pending,
                    "processing": self.queue_processing,
                    "retry": self.queue_retry,
                    "dlq": self.queue_dlq
                },
                "pubsub_channel": self.pubsub_channel
            }
        }

        if self.is_kafka_enabled:
            config["kafka"] = {
                "bootstrap_servers": self.kafka_bootstrap_servers,
                "consumer_group": self.kafka_consumer_group,
                "topics": {
                    "pending": self.kafka_topic_pending,
                    "retry": self.kafka_topic_retry,
                    "dlq": self.kafka_topic_dlq,
                    "updates": self.kafka_topic_updates
                },
                "compression": self.kafka_compression_type,
                "max_poll_records": self.kafka_max_poll_records,
                "auto_offset_reset": self.kafka_auto_offset_reset
            }

        return config


# Singleton instance
settings = Settings()

# Print configuration on import (for debugging)
if settings.log_level == "DEBUG":
    import json
    print("=== Worker Configuration ===")
    print(json.dumps(settings.get_monitoring_config(), indent=2))
    print("=== Queue Configuration ===")
    print(json.dumps(settings.get_queue_config(), indent=2))
    print(f"Log Level: {settings.log_level}")
    print(f"Log JSON: {settings.log_json}")
    print(f"Queue Backend: {settings.queue_backend}")
    print("===========================")