"""
Shared data models for AI Analytics Assistant
"""

from datetime import datetime
from typing import Dict, Optional, List, Any
from enum import Enum

from pydantic import BaseModel, Field


class TaskType(str, Enum):
    """Types of tasks"""
    ANALYTICS_QUERY = "analytics_query"
    EXECUTE_CONFIRMED = "execute_confirmed"
    DBT_RUN = "dbt_run"
    CLARIFICATION = "clarification"


class TaskPriority(str, Enum):
    """Task priority levels"""
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class TaskStatus(str, Enum):
    """Task execution status"""
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SlackContext(BaseModel):
    """Slack message context"""
    channel: str = Field(..., description="Channel ID")
    thread_ts: Optional[str] = Field(None, description="Thread timestamp")
    message_ts: str = Field(..., description="Message timestamp to update")
    user: str = Field(..., description="User ID")
    team: Optional[str] = Field(None, description="Team/Workspace ID")


class Task(BaseModel):
    """Task model for queue"""
    id: str = Field(..., description="Unique task ID")
    type: TaskType = Field(..., description="Type of task")
    question: str = Field(..., description="Original user question")
    slack_context: SlackContext = Field(..., description="Slack context")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    retry_count: int = Field(default=0, description="Number of retries")
    priority: TaskPriority = Field(default=TaskPriority.NORMAL)
    source: str = Field(default="slack", description="Source of the task")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkerUpdate(BaseModel):
    """Update message from worker to Slack bot"""
    type: str = Field(..., description="Update type")
    task_id: str = Field(..., description="Related task ID")
    slack_context: SlackContext = Field(..., description="Slack context")
    message: Optional[str] = Field(None, description="Human-readable message")
    blocks: Optional[List[Dict]] = Field(None, description="Slack blocks")
    result: Optional[Dict[str, Any]] = Field(None, description="Query result")
    error: Optional[str] = Field(None, description="Error message")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DryRunResult(BaseModel):
    """BigQuery dry run result"""
    sql: str = Field(..., description="SQL query")
    bytes_billed: int = Field(..., description="Estimated bytes to be billed")
    bytes_processed: int = Field(..., description="Estimated bytes to be processed")
    referenced_tables: List[str] = Field(default_factory=list)
    cache_hit: bool = Field(default=False)
    slot_millis: Optional[int] = Field(None)


class QueryResult(BaseModel):
    """Query execution result"""
    sql: str = Field(..., description="Executed SQL")
    rows_count: int = Field(..., description="Number of rows returned")
    bytes_processed: int = Field(..., description="Actual bytes processed")
    execution_time: float = Field(..., description="Execution time in seconds")
    cache_hit: bool = Field(default=False)
    preview: Optional[List[Dict]] = Field(None, description="Preview of results")
    table_link: Optional[str] = Field(None, description="Link to result table")
    table_name: Optional[str] = Field(None, description="Result table name")