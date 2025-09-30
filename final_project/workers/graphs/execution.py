"""
Query execution subgraph with confirmation handling
Location: workers/graphs/execution.py
"""
from typing import Dict
import structlog
from langgraph.graph import StateGraph, END

from workers.graphs.base import ExecutionState
from workers.tools.bigquery import BigQueryTool

logger = structlog.get_logger()


class QueryExecutor:
    """
    Subgraph for query execution.

    Features:
    1. Cost threshold checking
    2. User confirmation for expensive queries
    3. Query execution with error handling
    4. Result materialization to temp tables
    5. Performance metrics collection
    """

    def __init__(self, bigquery_tool: BigQueryTool = None):
        self.bigquery_tool = bigquery_tool or BigQueryTool()
        self.graph = self._build_graph()

        # Configuration
        self.cost_threshold = 1.0  # $1
        self.bytes_threshold = 10 * (1024 ** 3)  # 10GB

    def _build_graph(self) -> StateGraph:
        """Build execution workflow"""
        workflow = StateGraph(ExecutionState)

        workflow.add_node("check_thresholds", self.check_thresholds)
        workflow.add_node("request_confirmation", self.request_confirmation)
        workflow.add_node("execute_query", self.execute_query)
        workflow.add_node("materialize_results", self.materialize_results)
        workflow.add_node("collect_metrics", self.collect_metrics)

        workflow.set_entry_point("check_thresholds")

        workflow.add_conditional_edges(
            "check_thresholds",
            lambda state: "confirm" if state["needs_confirmation"] else "execute",
            {
                "confirm": "request_confirmation",
                "execute": "execute_query"
            }
        )

        workflow.add_conditional_edges(
            "request_confirmation",
            lambda state: "execute" if state.get("user_confirmed") else "end",
            {
                "execute": "execute_query",
                "end": END
            }
        )

        workflow.add_edge("execute_query", "materialize_results")
        workflow.add_edge("materialize_results", "collect_metrics")
        workflow.add_edge("collect_metrics", END)

        return workflow.compile()

    async def check_thresholds(self, state: ExecutionState) -> ExecutionState:
        """Check if query exceeds cost/size thresholds"""
        cost = state.get("dry_run_result", {}).get("estimated_cost", 0)
        bytes_scanned = state.get("dry_run_result", {}).get("totalBytesProcessed", 0)

        state["needs_confirmation"] = (
                cost > self.cost_threshold or
                bytes_scanned > self.bytes_threshold
        )

        if state["needs_confirmation"]:
            state["confirmation_reason"] = (
                f"Query will cost ${cost:.2f} and scan "
                f"{bytes_scanned / (1024 ** 3):.2f}GB"
            )

        logger.info("threshold_check",
                    task_id=state["task_id"],
                    needs_confirmation=state["needs_confirmation"])

        return state

    async def request_confirmation(self, state: ExecutionState) -> ExecutionState:
        """Mark query as needing user confirmation"""
        logger.info("confirmation_requested",
                    task_id=state["task_id"],
                    reason=state["confirmation_reason"])
        return state

    async def execute_query(self, state: Dict) -> Dict:
        """Execute the query"""
        try:
            result = await self.bigquery_tool.execute(
                sql=state["sql"],
                task_id=state["task_id"]
            )

            # ИСПРАВЛЕНО: Используем полные результаты вместо preview
            state["execution_result"] = {
                "rows": result.get("results", []),  # ИЗМЕНЕНО: results вместо preview
                "totalRows": result.get("rows_count", 0),
                "totalBytesProcessed": result.get("bytes_processed", 0),
                "cacheHit": result.get("cache_hit", False),
                "executionTimeMs": result.get("execution_time", 0) * 1000,
                "tableName": result.get("table_name"),
                "tableLink": result.get("table_link"),
                "costUsd": result.get("cost_usd", 0),
                "preview": result.get("preview", []),  # ДОБАВЛЕНО: Сохраняем preview отдельно
                "resultsTruncated": result.get("results_truncated", False)
            }

            # Сохраняем количество строк отдельно для удобства
            state["rows_affected"] = result.get("rows_count", 0)

            logger.info("query_executed",
                        task_id=state["task_id"],
                        rows=state["rows_affected"],
                        results_count=len(result.get("results", [])),
                        preview_count=len(result.get("preview", [])))

        except Exception as e:
            logger.error("execution_failed",
                         error=str(e),
                         task_id=state["task_id"])
            state["error"] = str(e)
            state["execution_result"] = {
                "rows": [],
                "error": str(e),
                "totalRows": 0
            }
            state["rows_affected"] = 0

        return state

    async def materialize_results(self, state: ExecutionState) -> ExecutionState:
        """Save results to temporary table if needed"""
        if state.get("execution_result") and state["rows_affected"] > 1000:
            table_name = f"analytics_tmp.results_{state['task_id'].replace('-', '_')}"
            # Implementation for creating temp table
            state["result_table"] = table_name
            logger.info("results_materialized", table=table_name)

        return state

    async def collect_metrics(self, state: ExecutionState) -> ExecutionState:
        """Collect performance metrics"""
        if state.get("execution_result"):
            result = state["execution_result"]
            state["execution_time_ms"] = result.get("totalSlotMs", 0)
            state["bytes_processed"] = result.get("totalBytesProcessed", 0)
            state["cache_hit"] = result.get("cacheHit", False)

            logger.info("metrics_collected",
                        task_id=state["task_id"],
                        time_ms=state["execution_time_ms"],
                        cache_hit=state["cache_hit"])

        return state