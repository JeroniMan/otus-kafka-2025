"""
Task Orchestrator - Clean interface for graph-based processing
Location: workers/orchestrator.py

This is a thin facade that delegates all work to the graph structure.
"""

from typing import Dict, Any, Optional
from datetime import datetime

import structlog
from workers.graphs.main import MainOrchestrator

logger = structlog.get_logger()


class TaskOrchestrator:
    """
    Interface to the graph-based orchestration system.
    All actual logic is in workers/graphs/
    """

    def __init__(self):
        """Initialize the graph orchestrator"""
        logger.info("initializing_orchestrator_interface")

        # Create the main orchestrator from graphs
        self._orchestrator = MainOrchestrator()

        # Expose tools for backward compatibility
        self.bigquery_tool = self._orchestrator.bigquery_tool
        self.sql_generator = self._orchestrator.sql_generator
        self.rag_tool = self._orchestrator.rag_tool

        logger.info("orchestrator_interface_ready")

    async def process_query(
        self,
        query: str,
        task_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process a query through the graph workflow.

        Args:
            query: Natural language query
            task_id: Unique task identifier
            metadata: Optional metadata (user_id, team_id, etc.)

        Returns:
            Query results or confirmation request
        """
        # Delegate to graph orchestrator
        return await self._orchestrator.process_query(
            query=query,
            task_id=task_id,
            metadata=metadata or {}
        )

    async def execute_sql(
        self,
        sql: str,
        task_id: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL directly (for confirmed queries).

        Args:
            sql: SQL query to execute
            task_id: Task identifier
            params: Optional SQL parameters

        Returns:
            Execution results
        """
        try:
            # Direct execution via BigQuery tool
            result = await self.bigquery_tool.execute(
                sql=sql,
                params=params or {},
                task_id=task_id
            )

            # Format response
            return {
                "task_id": task_id,
                "sql": sql,
                "results": result.get("rows", []),
                "total_rows": result.get("totalRows", 0),
                "bytes_billed": result.get("totalBytesProcessed", 0),
                "estimated_cost": (result.get("totalBytesProcessed", 0) / (1024**4)) * 6.25,
                "cache_hit": result.get("cacheHit", False),
                "report": {
                    "summary": f"Query executed successfully. Returned {result.get('totalRows', 0)} rows.",
                    "preview": result.get("rows", [])[:10],
                    "execution_time_ms": result.get("totalSlotMs", 0)
                },
                "created_at": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error("direct_sql_execution_failed",
                        task_id=task_id,
                        error=str(e))

            return {
                "error": str(e),
                "task_id": task_id,
                "sql": sql,
                "created_at": datetime.now().isoformat()
            }