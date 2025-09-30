"""
Base states and types for all graphs
Location: workers/graphs/base.py
"""

from typing import Dict, Any, List, Optional, TypedDict, Annotated, Literal
from datetime import datetime
from enum import Enum

from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage
from pydantic import BaseModel, Field


# =====================================================
# ENUMS
# =====================================================

class SearchStrategy(str, Enum):
    """Search strategies for context gathering"""
    SQL_HEAVY = "sql_heavy"           # Focus on SQL examples and schemas
    BUSINESS_HEAVY = "business_heavy"  # Focus on business context
    SCHEMA_ONLY = "schema_only"        # Only table schemas
    COMPREHENSIVE = "comprehensive"    # Search everything


class WorkflowStage(str, Enum):
    """Stages of the main workflow"""
    INITIALIZING = "initializing"
    GATHERING_CONTEXT = "gathering_context"
    GENERATING_SQL = "generating_sql"
    VALIDATING = "validating"
    EXECUTING = "executing"
    CREATING_REPORT = "creating_report"
    COMPLETED = "completed"
    FAILED = "failed"


# =====================================================
# STATE DEFINITIONS
# =====================================================

class BaseState(TypedDict):
    """Base state shared across all workflows"""
    # Core identifiers
    task_id: str
    query: str
    user_id: str
    team_id: str
    channel_id: str

    # Message history for LangGraph
    messages: Annotated[List[BaseMessage], add_messages]

    # Error tracking
    error: Optional[str]
    error_details: Optional[Dict[str, Any]]

    # Metadata and timing
    metadata: Dict[str, Any]
    start_time: float
    end_time: Optional[float]


class ContextState(BaseState):
    """State for context gathering subgraph"""
    # Strategy
    search_strategy: SearchStrategy

    # Retrieved context
    sql_examples: List[Dict[str, Any]]
    table_schemas: List[Dict[str, Any]]
    dbt_models: List[Dict[str, Any]]
    business_knowledge: List[Dict[str, Any]]

    # Relevance and ranking
    relevance_scores: Dict[str, float]
    selected_tables: List[str]

    # Cache keys
    cache_keys: Dict[str, str]


class SQLState(BaseState):
    """State for SQL generation subgraph"""
    # Input context
    context: Dict[str, Any]

    # Generated SQL
    generated_sql: str
    sql_params: Dict[str, Any]
    sql_explanation: Optional[str]

    # Validation
    validation_passed: bool
    validation_issues: List[str]
    validation_warnings: List[str]

    # Cost estimation
    estimated_bytes: int
    estimated_cost: float
    estimated_rows: Optional[int]

    # Optimization suggestions
    optimization_hints: List[str]


class ExecutionState(BaseState):
    """State for execution subgraph"""
    # Input
    sql: str
    sql_params: Dict[str, Any]

    # Dry run results
    dry_run_result: Dict[str, Any]

    # Confirmation
    needs_confirmation: bool
    confirmation_reason: Optional[str]
    user_confirmed: Optional[bool]

    # Execution
    execution_result: Optional[Dict[str, Any]]
    result_table: Optional[str]
    rows_affected: Optional[int]

    # Performance metrics
    execution_time_ms: Optional[int]
    bytes_processed: Optional[int]
    cache_hit: Optional[bool]


class MainState(BaseState):
    """Main orchestrator state combining all substates"""
    # Workflow tracking
    workflow_stage: WorkflowStage
    workflow_history: List[Dict[str, Any]]

    # Subgraph states
    context_state: Optional[ContextState]
    sql_state: Optional[SQLState]
    execution_state: Optional[ExecutionState]

    # Performance tracking
    stage_timings: Dict[str, float]
    total_tokens_used: int
    total_cost: float

    # Final output
    final_report: Optional[Dict[str, Any]]
    success: bool


# =====================================================
# MODELS (for structured outputs)
# =====================================================

class TableInfo(BaseModel):
    """Information about a BigQuery table"""
    dataset: str
    table: str
    full_name: str = Field(description="dataset.table")
    description: Optional[str] = None
    columns: List[Dict[str, str]] = Field(default_factory=list)
    partition_field: Optional[str] = None
    clustering_fields: List[str] = Field(default_factory=list)
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None


class SQLValidation(BaseModel):
    """SQL validation result"""
    is_valid: bool
    issues: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)
    estimated_cost: Optional[float] = None
    confidence_score: float = Field(ge=0, le=1)


def create_initial_state(
        task_id: str,
        query: str,
        user_id: str,
        team_id: str,
        channel_id: str,
        metadata: Optional[Dict[str, Any]] = None
) -> MainState:
    """Create initial main state for workflow"""
    return MainState(
        # Base fields
        task_id=task_id,
        query=query,
        user_id=user_id,
        team_id=team_id,
        channel_id=channel_id,

        # Messages for LangGraph
        messages=[],

        # Error tracking
        error=None,
        error_details=None,

        # Metadata
        metadata=metadata or {},
        start_time=datetime.now().timestamp(),
        end_time=None,

        # Workflow
        workflow_stage=WorkflowStage.INITIALIZING,
        workflow_history=[],

        # Substates (will be populated by subgraphs)
        context_state=None,
        sql_state=None,
        execution_state=None,

        # Performance
        stage_timings={},
        total_tokens_used=0,
        total_cost=0.0,

        # Output
        final_report=None,
        success=False
    )