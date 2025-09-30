"""
LangGraph-based workflow graphs
Location: workers/graphs/__init__.py
"""

from workers.graphs.base import (
    BaseState,
    ContextState,
    SQLState,
    ExecutionState,
    MainState,
    SearchStrategy,
    WorkflowStage,
    create_initial_state
)

from workers.graphs.context import ContextGatherer
from workers.graphs.sql import SQLGenerator
from workers.graphs.execution import QueryExecutor
from workers.graphs.main import MainOrchestrator

# Для обратной совместимости
TaskOrchestrator = MainOrchestrator

__all__ = [
    # States
    "BaseState",
    "ContextState",
    "SQLState",
    "ExecutionState",
    "MainState",

    # Enums
    "SearchStrategy",
    "WorkflowStage",

    # Subgraphs
    "ContextGatherer",
    "SQLGenerator",
    "QueryExecutor",

    # Main
    "MainOrchestrator",
    "TaskOrchestrator",

    # Helpers
    "create_initial_state"
]