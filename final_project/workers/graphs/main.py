"""
Main orchestrator that coordinates all subgraphs
Location: workers/graphs/main.py

DEBUG VERSION - With extensive logging to track state issues
"""
import os
import json
import traceback
from typing import Dict, Any, Optional, TYPE_CHECKING, List
from datetime import datetime

import structlog
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.store.memory import InMemoryStore
from langchain_openai import ChatOpenAI

import redis.asyncio as redis
from workers.utils.session import SessionManager


from workers.graphs.base import (
    MainState,
    WorkflowStage,
    SearchStrategy,
    create_initial_state
)

if TYPE_CHECKING:
    from workers.graphs.base import ContextState, SQLState, ExecutionState

from workers.graphs.context import ContextGatherer
from workers.graphs.sql import SQLGenerator
from workers.graphs.execution import QueryExecutor
from workers.tools.bigquery import BigQueryTool
from workers.tools.rag import MultiCollectionRAG
from workers.config import settings

logger = structlog.get_logger()


class MainOrchestrator:
    """Main orchestrator that coordinates all subgraphs."""

    def __init__(self):
        """Initialize orchestrator with subgraphs"""

        # Configure logging
        logger.info("initializing_main_orchestrator",
                    langsmith_enabled=os.getenv("LANGCHAIN_TRACING_V2") == "true")

        # Initialize store
        self.store = InMemoryStore()

        # Initialize BigQuery tool
        try:
            self.bigquery_tool = BigQueryTool()
            logger.info("BigQuery tool initialized")
        except Exception as e:
            logger.error("Failed to initialize BigQuery tool", error=str(e))
            raise

        # Initialize RAG tool if enabled
        self.rag_tool = None
        if settings.qdrant_enabled:
            try:
                from workers.tools.rag import MultiCollectionRAG
                self.rag_tool = MultiCollectionRAG()
                logger.info("RAG tool initialized",
                            url=settings.qdrant_url if hasattr(settings, 'qdrant_url') else 'default')
            except Exception as e:
                logger.error("Failed to initialize RAG tool",
                             error=str(e),
                             error_type=type(e).__name__)
                logger.warning("Continuing without RAG - context will be limited")
                self.rag_tool = None
        else:
            logger.info("Qdrant disabled in settings, RAG will not be available")

        # Initialize LLM
        self.llm = ChatOpenAI(
            model=settings.openai_model,
            temperature=settings.openai_temperature
        )
        logger.info("LLM initialized", model=settings.openai_model)

        # Initialize subgraphs with proper dependencies
        self.context_gatherer = ContextGatherer(
            rag_tool=self.rag_tool,
            bigquery_tool=self.bigquery_tool,
            store=self.store
        )
        logger.info("Context gatherer initialized", has_rag=bool(self.rag_tool))

        self.sql_generator = SQLGenerator(
            llm=self.llm,
            bigquery_tool=self.bigquery_tool
        )
        logger.info("SQL generator initialized")

        self.query_executor = QueryExecutor(
            bigquery_tool=self.bigquery_tool
        )
        logger.info("Query executor initialized")

        # Initialize Redis and session manager
        try:
            self.redis_client = redis.from_url(settings.redis_url)
            self.session_manager = SessionManager(self.redis_client)
            logger.info("Redis and session manager initialized")
        except Exception as e:
            logger.error("Failed to initialize Redis", error=str(e))
            raise

        # Build main workflow
        self.workflow = self._build_workflow()

        if hasattr(self.workflow, 'compile'):
            self.app = self.workflow.compile()
        else:
            self.app = self.workflow

        logger.info("Main orchestrator initialized successfully",
                    has_rag=bool(self.rag_tool),
                    components=[
                        "bigquery",
                        "rag" if self.rag_tool else "no-rag",
                        "context_gatherer",
                        "sql_generator",
                        "query_executor",
                        "session_manager"
                    ])

    def _build_workflow(self) -> StateGraph:
        """Build the main workflow that coordinates subgraphs"""
        workflow = StateGraph(MainState)

        # Add nodes
        workflow.add_node("initialize", self.initialize)
        workflow.add_node("gather_context", self.run_context_subgraph)
        workflow.add_node("generate_sql", self.run_sql_subgraph)
        workflow.add_node("execute_query", self.run_execution_subgraph)
        workflow.add_node("create_report", self.create_report)
        workflow.add_node("handle_error", self.handle_error)

        # Define flow
        workflow.set_entry_point("initialize")
        workflow.add_edge("initialize", "gather_context")
        workflow.add_edge("gather_context", "generate_sql")

        # DEBUG: Enhanced conditional edge with logging
        def check_sql_generation(state):
            """Check if SQL was generated successfully"""
            logger.debug("🔍 DEBUG: Checking SQL generation",
                         task_id=state.get("task_id"),
                         has_sql_state=bool(state.get("sql_state")),
                         sql_state_type=type(state.get("sql_state")),
                         sql_state_keys=list(state.get("sql_state", {}).keys()) if isinstance(state.get("sql_state"),
                                                                                              dict) else None,
                         generated_sql=state.get("sql_state", {}).get("generated_sql")[:100] if isinstance(
                             state.get("sql_state"), dict) and state.get("sql_state", {}).get(
                             "generated_sql") else None,
                         generated_sql_empty=not bool(state.get("sql_state", {}).get("generated_sql")) if isinstance(
                             state.get("sql_state"), dict) else True)

            # Check if SQL was generated
            if state.get("sql_state") and state["sql_state"].get("generated_sql"):
                logger.debug("✅ SQL generation successful, proceeding to execute",
                            task_id=state.get("task_id"))
                return "execute"
            else:
                logger.debug("❌ SQL generation failed or empty, going to error",
                            task_id=state.get("task_id"),
                            reason="No SQL generated" if not state.get("sql_state", {}).get("generated_sql") else "Unknown")
                return "error"

        workflow.add_conditional_edges(
            "generate_sql",
            check_sql_generation,
            {
                "execute": "execute_query",
                "error": "handle_error"
            }
        )

        # DEBUG: Enhanced confirmation check
        def check_confirmation_needed(state):
            """Check if confirmation is needed"""
            needs_confirmation = state.get("execution_state", {}).get("needs_confirmation", False)
            logger.debug("🔍 DEBUG: Checking confirmation",
                        task_id=state.get("task_id"),
                        has_execution_state=bool(state.get("execution_state")),
                        needs_confirmation=needs_confirmation)

            if needs_confirmation:
                return "end"
            else:
                return "report"

        workflow.add_conditional_edges(
            "execute_query",
            check_confirmation_needed,
            {
                "report": "create_report",
                "end": END
            }
        )

        workflow.add_edge("create_report", END)
        workflow.add_edge("handle_error", END)

        checkpointer = MemorySaver()
        return workflow.compile(checkpointer=checkpointer)

    async def initialize(self, state: MainState) -> MainState:
        """Initialize workflow with thread-aware session context"""
        state["workflow_stage"] = WorkflowStage.INITIALIZING

        # Extract thread_ts from metadata
        thread_ts = None
        if state.get("metadata") and state["metadata"].get("slack_context"):
            thread_ts = state["metadata"]["slack_context"].get("thread_ts")

        # Load thread-aware session context
        if state.get("metadata"):
            context = await self.session_manager.get_context_for_query(
                team_id=state["team_id"],
                channel_id=state["channel_id"],
                user_id=state["user_id"],
                thread_ts=thread_ts,
                current_query=state["query"]
            )

            # Store in metadata
            state["metadata"]["session_context"] = context

            # Log session type
            logger.info("session_context_loaded",
                        task_id=state["task_id"],
                        session_type=context["session_type"],
                        thread_ts=thread_ts,
                        has_history=len(context["previous_queries"]) > 0)

            # Enhanced query for threads with history
            if context["session_type"] == "thread" and context["previous_queries"]:
                # In thread - provide full conversation context
                thread_context = f"[Thread conversation with {len(context['previous_queries'])} previous queries]\n"

                for i, pq in enumerate(context["previous_queries"], 1):
                    thread_context += f"\nQ{i}: {pq['query']}\n"
                    thread_context += f"Result: {pq['row_count']} rows from {', '.join(pq['tables'][:2])}\n"

                state["enhanced_query"] = f"{thread_context}\nCurrent query: {state['query']}"
                state["metadata"]["is_thread_continuation"] = True

            elif context["has_reference"] and context["previous_queries"]:
                # Has reference but not in thread
                last_q = context["previous_queries"][-1]
                state["enhanced_query"] = f"""[Previous: "{last_q['query']}" → {last_q['row_count']} rows]
    Current: {state['query']}"""

            else:
                state["enhanced_query"] = state["query"]

        return state

    async def create_report(self, state: MainState) -> MainState:
        """Create report and save to thread-aware session"""
        # ... existing report creation ...

        # Extract thread_ts for session
        thread_ts = None
        if state.get("metadata") and state["metadata"].get("slack_context"):
            thread_ts = state["metadata"]["slack_context"].get("thread_ts")

        # Save to thread-aware session
        if state.get("success") and state.get("execution_state"):
            sql = state.get("sql_state", {}).get("generated_sql", "")
            tables = self._extract_table_names(sql)

            await self.session_manager.add_query_result(
                team_id=state["team_id"],
                channel_id=state["channel_id"],
                user_id=state["user_id"],
                thread_ts=thread_ts,  # Thread-aware
                query=state["query"],
                sql=sql,
                tables_used=tables,
                row_count=state["execution_state"].get("rows_affected", 0),
                execution_time_ms=state["execution_state"].get("execution_time_ms", 0)
            )

            logger.info("saved_to_thread_session",
                        task_id=state["task_id"],
                        thread_ts=thread_ts,
                        session_type=state["metadata"].get("session_context", {}).get("session_type"))

        return state

    async def run_context_subgraph(self, state: MainState) -> MainState:
        """Run context gathering subgraph"""
        state["workflow_stage"] = WorkflowStage.GATHERING_CONTEXT
        start_time = datetime.now()

        logger.debug("🔍 DEBUG: Starting context gathering",
                    task_id=state["task_id"],
                    state_keys_before=list(state.keys()))

        context_state = {
            "task_id": state["task_id"],
            "query": state["query"],
            "user_id": state["user_id"],
            "team_id": state["team_id"],
            "channel_id": state["channel_id"],
            "messages": state["messages"],
            "metadata": state["metadata"],
            "start_time": state["start_time"],
            "end_time": None,
            "error": None,
            "error_details": None,
            "search_strategy": SearchStrategy.COMPREHENSIVE,
            "sql_examples": [],
            "table_schemas": [],
            "dbt_models": [],
            "business_knowledge": [],
            "relevance_scores": {},
            "selected_tables": [],
            "cache_keys": []
        }

        try:
            logger.debug("🔍 DEBUG: Invoking context_gatherer",
                        task_id=state["task_id"])

            result = await self.context_gatherer.app.ainvoke(context_state)

            logger.debug("🔍 DEBUG: Context gatherer result",
                        task_id=state["task_id"],
                        result_type=type(result),
                        result_keys=list(result.keys()) if result else None,
                        has_error=result.get("error") if result else None)

            state["context_state"] = result

        except Exception as e:
            logger.error("context_gathering_failed",
                        task_id=state["task_id"],
                        error=str(e),
                        traceback=traceback.format_exc())
            state["error"] = f"Context gathering failed: {str(e)}"
            state["workflow_stage"] = WorkflowStage.FAILED
            return state

        state["stage_timings"]["context"] = (datetime.now() - start_time).total_seconds()

        logger.info("context_gathering_complete",
                    task_id=state["task_id"],
                    duration=state["stage_timings"]["context"])

        logger.debug("🔍 DEBUG: State after context gathering",
                    task_id=state["task_id"],
                    has_context_state=bool(state.get("context_state")),
                    context_keys=list(state.get("context_state", {}).keys()) if state.get("context_state") else None)

        return state

    async def run_sql_subgraph(self, state: MainState) -> MainState:
        """Run SQL generation subgraph"""
        state["workflow_stage"] = WorkflowStage.GENERATING_SQL
        start_time = datetime.now()

        logger.debug("🔍 DEBUG: Starting SQL generation",
                     task_id=state["task_id"],
                     has_context=bool(state.get("context_state")))

        # ИСПРАВЛЕНИЕ: Проверяем и логируем контекст
        if state.get("context_state"):
            context_state = state["context_state"]
            logger.debug("🔍 DEBUG: Context for SQL generation",
                         task_id=state["task_id"],
                         num_examples=len(context_state.get("sql_examples", [])),
                         num_schemas=len(context_state.get("table_schemas", [])))

        # Prepare SQL state
        sql_state = {
            "task_id": state["task_id"],
            "query": state["query"],
            "user_id": state["user_id"],
            "team_id": state["team_id"],
            "channel_id": state["channel_id"],
            "messages": state["messages"],
            "metadata": state["metadata"],
            "start_time": state["start_time"],
            "end_time": None,
            "error": None,
            "error_details": None,
            "context": {},  # Инициализируем пустым словарем
            "generated_sql": None,
            "sql_params": {},
            "sql_explanation": "",
            "validation_passed": False,
            "validation_issues": [],
            "validation_warnings": [],
            "estimated_bytes": None,
            "estimated_cost": None,
            "estimated_rows": None,
            "optimization_hints": []
        }

        # ГЛАВНОЕ ИСПРАВЛЕНИЕ: Передаем контекст из context_state в sql_state
        if state.get("context_state"):
            context_state = state["context_state"]
            sql_state["context"] = {
                "sql_examples": context_state.get("sql_examples", []),
                "table_schemas": context_state.get("table_schemas", []),
                "dbt_models": context_state.get("dbt_models", []),
                "business_knowledge": context_state.get("business_knowledge", [])
            }

        try:
            logger.debug("🔍 DEBUG: Invoking sql_generator",
                         task_id=state["task_id"],
                         sql_state_keys=list(sql_state.keys()))

            result = await self.sql_generator.graph.ainvoke(sql_state)

            logger.debug("🔍 DEBUG: SQL generator result",
                         task_id=state["task_id"],
                         result_type=type(result),
                         result_keys=list(result.keys()) if result else None,
                         has_generated_sql=bool(result.get("generated_sql")),
                         generated_sql_preview=result.get("generated_sql", "")[:200] if result.get(
                             "generated_sql") else None,
                         has_error=result.get("error"))

            state["sql_state"] = result

            logger.debug("🔍 DEBUG: State after SQL assignment",
                         task_id=state["task_id"],
                         has_generated_sql=bool(state.get("sql_state", {}).get("generated_sql")),
                         sql_state_is_none=state.get("sql_state") is None,
                         sql_state_type=type(state.get("sql_state")))

        except Exception as e:
            logger.error("sql_generation_failed",
                         task_id=state["task_id"],
                         error=str(e),
                         traceback=traceback.format_exc())
            state["error"] = f"SQL generation failed: {str(e)}"
            state["workflow_stage"] = WorkflowStage.FAILED
            return state

        state["stage_timings"]["sql"] = (datetime.now() - start_time).total_seconds()

        logger.info("sql_generation_complete",
                    task_id=state["task_id"],
                    duration=state["stage_timings"]["sql"])

        logger.debug("🔍 DEBUG: Final state after SQL generation",
                     task_id=state["task_id"],
                     has_sql_state=bool(state.get("sql_state")),
                     generated_sql_exists=bool(state.get("sql_state", {}).get("generated_sql")),
                     workflow_stage=state.get("workflow_stage"),
                     state_error=state.get("error"))

        return state

    async def run_execution_subgraph(self, state: MainState) -> MainState:
        """Run query execution subgraph"""
        state["workflow_stage"] = WorkflowStage.EXECUTING
        start_time = datetime.now()

        logger.debug("🔍 DEBUG: Starting execution",
                    task_id=state["task_id"],
                    has_sql_state=bool(state.get("sql_state")),
                    generated_sql=state.get("sql_state", {}).get("generated_sql", "")[:100])

        if not state.get("sql_state") or not state["sql_state"].get("generated_sql"):
            logger.error("🔍 DEBUG: No SQL to execute",
                        task_id=state["task_id"],
                        sql_state=state.get("sql_state"))
            state["error"] = "No SQL query to execute"
            state["workflow_stage"] = WorkflowStage.FAILED
            return state

        execution_state = {
            "task_id": state["task_id"],
            "query": state["query"],
            "user_id": state["user_id"],
            "team_id": state["team_id"],
            "channel_id": state["channel_id"],
            "messages": state["messages"],
            "metadata": state["metadata"],
            "start_time": state["start_time"],
            "end_time": None,
            "error": None,
            "error_details": None,
            "sql": state["sql_state"]["generated_sql"],
            "sql_params": state["sql_state"].get("sql_params", {}),
            "dry_run_result": {},
            "needs_confirmation": False,
            "confirmation_reason": None,
            "user_confirmed": None,
            "execution_result": None,
            "result_table": None,
            "rows_affected": None,
            "execution_time_ms": None,
            "bytes_processed": None,
            "cache_hit": None
        }

        try:
            logger.debug("🔍 DEBUG: Invoking query_executor",
                        task_id=state["task_id"])

            result = await self.query_executor.graph.ainvoke(execution_state)

            logger.debug("🔍 DEBUG: Execution result",
                        task_id=state["task_id"],
                        result_type=type(result),
                        has_error=result.get("error") if result else None,
                        needs_confirmation=result.get("needs_confirmation") if result else None)

            state["execution_state"] = result

        except Exception as e:
            logger.error("query_execution_failed",
                        task_id=state["task_id"],
                        error=str(e),
                        traceback=traceback.format_exc())
            state["error"] = f"Query execution failed: {str(e)}"
            state["workflow_stage"] = WorkflowStage.FAILED
            return state

        state["stage_timings"]["execution"] = (datetime.now() - start_time).total_seconds()

        logger.info("query_execution_complete",
                    task_id=state["task_id"],
                    duration=state["stage_timings"]["execution"])

        return state

    async def create_report(self, state: MainState) -> MainState:
        """Create final report from execution results"""
        from workers.utils.report_generator import ReportGenerator  # Добавить импорт

        state["workflow_stage"] = WorkflowStage.CREATING_REPORT

        execution_state = state.get("execution_state", {})
        execution_result = execution_state.get("execution_result", {})

        # FIXED: Ensure we capture the actual query results
        query_results = execution_result.get("rows", [])
        preview_results = execution_result.get("preview", [])

        # Create the main report
        final_report = {
            "task_id": state["task_id"],
            "query": state["query"],
            "sql": state.get("sql_state", {}).get("generated_sql", ""),
            "sql_params": state.get("sql_state", {}).get("sql_params", {}),
            "results": query_results,
            "preview": preview_results,
            "total_rows": execution_result.get("totalRows", 0),
            "rows_count": execution_result.get("totalRows", 0),
            "execution_time_ms": execution_result.get("executionTimeMs", 0),
            "bytes_processed": execution_result.get("totalBytesProcessed", 0),
            "bytes_processed_gb": execution_result.get("totalBytesProcessed", 0) / (1024 ** 3),
            "estimated_cost": execution_result.get("costUsd", 0),
            "cache_hit": execution_result.get("cacheHit", False),
            "table_name": execution_result.get("tableName"),
            "table_link": execution_result.get("tableLink"),
            "stage_timings": state["stage_timings"],
            "created_at": datetime.now().isoformat()
        }

        # Генерируем отчеты с учетом размера данных
        if len(query_results) > 100:
            # Для больших результатов используем только preview в отчетах
            report_results = preview_results
        else:
            # Для небольших результатов используем все
            report_results = query_results

        # НОВОЕ: Генерируем HTML отчет
        html_report = ReportGenerator.generate_html_report(
            query=state["query"],
            sql=state.get("sql_state", {}).get("generated_sql", ""),
            results=report_results,
            execution_time_ms=final_report["execution_time_ms"],
            total_rows=final_report["total_rows"],
            bytes_processed=final_report["bytes_processed"],
            cache_hit=final_report["cache_hit"],
            error=execution_state.get("error")
        )

        # НОВОЕ: Генерируем Markdown отчет
        markdown_report = ReportGenerator.generate_markdown_report(
            query=state["query"],
            sql=state.get("sql_state", {}).get("generated_sql", ""),
            results=report_results,  # Первые 20 строк для Markdown
            execution_time_ms=final_report["execution_time_ms"],
            total_rows=final_report["total_rows"],
            bytes_processed=final_report["bytes_processed"],
            cache_hit=final_report["cache_hit"],
            error=execution_state.get("error")
        )

        # НОВОЕ: Генерируем простой текстовый отчет для Slack
        simple_report = ReportGenerator.generate_simple_text_report(
            query=state["query"],
            sql=state.get("sql_state", {}).get("generated_sql", ""),
            results=report_results,
            execution_time_ms=final_report["execution_time_ms"],
            total_rows=final_report["total_rows"],
            error=execution_state.get("error")
        )

        # ОБНОВЛЕНО: Добавляем HTML и Markdown отчеты
        final_report["html_report"] = html_report
        final_report["markdown_report"] = markdown_report

        # CRITICAL FIX: Properly structure the report for Slack communicator
        final_report["report"] = {
            "summary": f"Query completed successfully. Found {final_report['total_rows']} rows.",
            "text": simple_report,  # Добавляем текстовую версию
            "sql": final_report["sql"],
            "results": report_results,  # First 100 rows for display
            "total_rows": final_report["total_rows"],
            "execution_time_ms": final_report["execution_time_ms"],
            "bytes_processed_gb": final_report["bytes_processed_gb"],
            "estimated_cost": final_report["estimated_cost"],
            "cache_hit": final_report["cache_hit"]
        }

        state["final_report"] = final_report
        state["success"] = True
        state["workflow_stage"] = WorkflowStage.COMPLETED

        # UPDATED: Save to session history with thread_ts support
        if state.get("success"):
            sql = state.get("sql_state", {}).get("generated_sql", "")
            tables_used = self._extract_table_names(sql)

            # Extract thread_ts from metadata
            thread_ts = None
            if state.get("metadata") and state["metadata"].get("slack_context"):
                thread_ts = state["metadata"]["slack_context"].get("thread_ts")

            # Only save to session if we have the required IDs
            if state.get("team_id") and state.get("channel_id") and state.get("user_id"):
                try:
                    await self.session_manager.add_query_result(
                        team_id=state["team_id"],
                        channel_id=state["channel_id"],
                        user_id=state["user_id"],
                        thread_ts=thread_ts,  # ADDED: thread_ts parameter
                        query=state["query"][:500],  # Limit query length
                        sql=sql,
                        tables_used=tables_used,
                        row_count=final_report.get("total_rows", 0),
                        execution_time_ms=final_report.get("execution_time_ms", 0)
                    )

                    logger.info("saved_to_thread_session",
                                task_id=state["task_id"],
                                thread_ts=thread_ts,
                                team_id=state["team_id"],
                                channel_id=state["channel_id"])
                except Exception as e:
                    # Don't fail the whole workflow if session save fails
                    logger.error("failed_to_save_session",
                                 task_id=state["task_id"],
                                 error=str(e),
                                 traceback=traceback.format_exc())
            else:
                logger.warning("missing_session_identifiers",
                               task_id=state["task_id"],
                               has_team_id=bool(state.get("team_id")),
                               has_channel_id=bool(state.get("channel_id")),
                               has_user_id=bool(state.get("user_id")))

        logger.info("report_created",
                    task_id=state["task_id"],
                    total_rows=final_report.get("total_rows", 0),
                    execution_time_ms=final_report.get("execution_time_ms", 0),
                    cache_hit=final_report.get("cache_hit", False))

        return state

    async def handle_error(self, state: MainState) -> MainState:
        """Handle errors in the workflow"""
        state["workflow_stage"] = WorkflowStage.FAILED
        state["success"] = False

        logger.error("workflow_failed",
                     task_id=state["task_id"],
                     error=state.get("error"))

        # DEBUG: Log state when error occurs
        logger.debug("🔍 DEBUG: Error handler state",
                    task_id=state["task_id"],
                    error=state.get("error"),
                    has_sql_state=bool(state.get("sql_state")),
                    sql_state_keys=list(state.get("sql_state", {}).keys()) if state.get("sql_state") else None,
                    generated_sql=state.get("sql_state", {}).get("generated_sql", "")[:100] if state.get("sql_state") else None,
                    workflow_history=state.get("workflow_history"))

        return state


    async def process_query(self, query: str, task_id: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[
        str, Any]:
        """Process query through workflow"""
        logger.info("processing_query", task_id=task_id, query=query[:100])

        # Get session context if we have user metadata
        session_context = {}
        enhanced_context = query  # Default to original query

        if metadata:
            team_id = metadata.get("team_id", "default")
            channel_id = metadata.get("channel_id", "default")
            user_id = metadata.get("user_id", "unknown")

            # UPDATED: Extract thread_ts from metadata
            thread_ts = None
            if metadata.get("slack_context"):
                thread_ts = metadata["slack_context"].get("thread_ts")

            # UPDATED: Pass thread_ts to get_context_for_query
            session_context = await self.session_manager.get_context_for_query(
                team_id=team_id,
                channel_id=channel_id,
                user_id=user_id,
                thread_ts=thread_ts,  # ADDED: thread_ts parameter
                current_query=query  # FIXED: renamed from 'query' to 'current_query'
            )

            # If query references previous results, enhance it
            if session_context.get("has_reference") and session_context.get("previous_queries"):
                # Add context to the state based on session type
                if session_context.get("session_type") == "thread":
                    # In thread - provide full conversation context
                    enhanced_context = f"[Thread conversation]\n"
                    for i, pq in enumerate(session_context["previous_queries"], 1):
                        enhanced_context += f"Q{i}: {pq['query']}\n"
                        enhanced_context += f"Result: {pq['row_count']} rows from {', '.join(pq['tables'][:2])}\n\n"
                    enhanced_context += f"Current query: {query}"
                else:
                    # Not in thread - just last query
                    last_query = session_context["previous_queries"][-1]
                    enhanced_context = (
                        f"Previous query: {last_query['query']}\n"
                        f"Previous SQL: {last_query['sql']}\n"
                        f"Previous results: {last_query['row_count']} rows\n"
                        f"Tables used: {', '.join(last_query['tables'])}\n\n"
                        f"Current query: {query}"
                    )

                logger.info("query_enhanced_with_context",
                            task_id=task_id,
                            session_type=session_context.get("session_type"),
                            thread_ts=thread_ts,
                            has_previous=True)

        try:
            initial_state = create_initial_state(
                task_id=task_id,
                query=enhanced_context,  # Use enhanced query if available
                user_id=metadata.get("user_id", "unknown") if metadata else "unknown",
                team_id=metadata.get("team_id", "unknown") if metadata else "unknown",
                channel_id=metadata.get("channel_id", "unknown") if metadata else "unknown",
                metadata=metadata
            )

            # Add session context to state
            initial_state["session_context"] = session_context

            # Store thread_ts in metadata for later use
            if metadata and metadata.get("slack_context", {}).get("thread_ts"):
                initial_state["metadata"]["thread_ts"] = metadata["slack_context"]["thread_ts"]

            logger.debug("🔍 DEBUG: Initial state created",
                         task_id=task_id,
                         state_keys=list(initial_state.keys()),
                         session_type=session_context.get("session_type", "none"),
                         thread_ts=metadata.get("slack_context", {}).get("thread_ts") if metadata else None)

            config = {"configurable": {"thread_id": task_id,
                                       "checkpoint_ns": f"main_{task_id}",
                                       "store": self.store
                                       }}
            final_state = await self.workflow.ainvoke(initial_state, config)

            # Check if workflow returned None
            if final_state is None:
                logger.error("workflow_returned_none", task_id=task_id)
                return {
                    "error": "Workflow failed to return a result",
                    "task_id": task_id,
                    "query": query,
                    "created_at": datetime.now().isoformat()
                }

            logger.debug("🔍 DEBUG: Final state received",
                         task_id=task_id,
                         state_type=type(final_state),
                         state_keys=list(final_state.keys()) if final_state else None,
                         workflow_stage=final_state.get("workflow_stage"),
                         success=final_state.get("success"),
                         has_error=bool(final_state.get("error")),
                         error=final_state.get("error"))

            # Check for errors
            if final_state.get("error"):
                logger.error("workflow_error_detected",
                             task_id=task_id,
                             error=final_state.get("error"))
                return {
                    "error": final_state.get("error"),
                    "error_details": final_state.get("error_details"),
                    "task_id": task_id,
                    "query": query,
                    "created_at": datetime.now().isoformat()
                }

            # Check if confirmation needed
            if final_state.get("execution_state") and final_state["execution_state"].get("needs_confirmation"):
                logger.info("confirmation_needed",
                            task_id=task_id,
                            reason=final_state["execution_state"]["confirmation_reason"])
                return {
                    "needs_confirmation": True,
                    "confirmation_reason": final_state["execution_state"]["confirmation_reason"],
                    "dry_run_result": final_state["execution_state"]["dry_run_result"],
                    "estimated_cost": final_state.get("sql_state", {}).get("estimated_cost", 0),
                    "sql": final_state.get("sql_state", {}).get("generated_sql", ""),
                    "task_id": task_id,
                    "query": query,
                    "created_at": datetime.now().isoformat()
                }

            # Check for successful result
            if final_state.get("success") and final_state.get("final_report"):
                logger.info("returning_final_report",
                            task_id=task_id,
                            rows=final_state["final_report"].get("total_rows", 0))

                # Extract table names from SQL
                sql = final_state["final_report"].get("sql", "")
                tables_used = self._extract_table_names(sql)

                # UPDATED: Save to thread-aware session
                if metadata:
                    thread_ts = None
                    if metadata.get("slack_context"):
                        thread_ts = metadata["slack_context"].get("thread_ts")

                    await self.session_manager.add_query_result(
                        team_id=metadata.get("team_id", "default"),
                        channel_id=metadata.get("channel_id", "default"),
                        user_id=metadata.get("user_id", "unknown"),
                        thread_ts=thread_ts,  # ADDED: thread_ts
                        query=query,
                        sql=sql,
                        tables_used=tables_used,
                        row_count=final_state["final_report"].get("total_rows", 0),
                        execution_time_ms=final_state.get("execution_state", {}).get("execution_time_ms", 0)
                    )

                    logger.info("saved_to_session",
                                task_id=task_id,
                                session_type=session_context.get("session_type"),
                                thread_ts=thread_ts)

                return final_state["final_report"]

            # Fallback for unexpected state
            logger.warning("unexpected_final_state",
                           task_id=task_id,
                           workflow_stage=final_state.get("workflow_stage"),
                           success=final_state.get("success"),
                           has_report=bool(final_state.get("final_report")))

            return {
                "error": "Query processing completed but no results were generated",
                "task_id": task_id,
                "query": query,
                "workflow_stage": str(final_state.get("workflow_stage")),
                "created_at": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error("query_processing_failed",
                         task_id=task_id,
                         error=str(e),
                         error_type=type(e).__name__,
                         traceback=traceback.format_exc())

            return {
                "error": f"Query processing failed: {str(e)}",
                "error_details": traceback.format_exc() if settings.log_level == "DEBUG" else None,
                "task_id": task_id,
                "query": query,
                "created_at": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error("process_query_failed",
                        task_id=task_id,
                        error=str(e),
                        error_type=type(e).__name__,
                        traceback=traceback.format_exc())

            return {
                "error": f"Query processing failed: {str(e)}",
                "error_type": type(e).__name__,
                "task_id": task_id,
                "query": query,
                "created_at": datetime.now().isoformat()
            }

    def _extract_table_names(self, sql: str) -> List[str]:
        """Extract table names from SQL query"""
        import re

        # Simple regex to find table references (project.dataset.table format)
        pattern = r'`([^`]+\.[^`]+\.[^`]+)`'
        matches = re.findall(pattern, sql)

        # Also find FROM/JOIN clauses
        from_pattern = r'(?:FROM|JOIN)\s+`([^`]+)`'
        from_matches = re.findall(from_pattern, sql, re.IGNORECASE)

        all_tables = list(set(matches + from_matches))
        return all_tables[:10]  # Limit to 10 tables