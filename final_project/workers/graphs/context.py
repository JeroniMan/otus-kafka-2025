# File: workers/graphs/context.py - ПОЛНЫЙ ФАЙЛ

"""
Context gathering subgraph for SQL generation
"""

import time
from typing import Dict, Any, Optional, List
from langgraph.graph import StateGraph, END
from pydantic import BaseModel, Field
import structlog

from workers.tools.rag import MultiCollectionRAG
from workers.tools.bigquery import BigQueryTool

logger = structlog.get_logger()


class ContextState(BaseModel):
    """State for context gathering subgraph"""
    task_id: str = Field(default="")
    query: str = Field(default="")
    user_id: str = Field(default="")
    team_id: str = Field(default="")
    channel_id: str = Field(default="")

    # Context data
    search_strategy: str = Field(default="comprehensive")
    sql_examples: List[Dict] = Field(default_factory=list)
    table_schemas: List[Dict] = Field(default_factory=list)
    dbt_models: List[Dict] = Field(default_factory=list)
    business_knowledge: List[Dict] = Field(default_factory=list)

    # Metadata
    relevance_scores: Dict = Field(default_factory=dict)
    selected_tables: List[str] = Field(default_factory=list)
    cache_keys: List[str] = Field(default_factory=list)

    # Session context
    session_context: Dict = Field(default_factory=dict)

    # Error handling
    error: Optional[str] = None
    error_details: Optional[Dict] = None


class ContextGatherer:
    """Context gathering subgraph for enriching queries with relevant information"""

    def __init__(self, rag_tool=None, bigquery_tool=None, store=None):
        """Initialize context gatherer"""
        self.rag_tool = rag_tool
        self.bigquery_tool = bigquery_tool
        self.store = store

        # Log initialization status
        if self.rag_tool:
            logger.info("ContextGatherer initialized with RAG tool")
        else:
            logger.warning("ContextGatherer initialized WITHOUT RAG tool - context will be limited")

        # Build workflow
        self.workflow = self._build_workflow()
        self.app = self.workflow.compile()

    def _build_workflow(self) -> StateGraph:
        """Build the context gathering workflow"""
        workflow = StateGraph(ContextState)

        # Add nodes
        workflow.add_node("determine_strategy", self.determine_strategy)
        workflow.add_node("gather_context", self.gather_context)
        workflow.add_node("analyze_context", self.analyze_context)
        workflow.add_node("extract_tables", self.extract_tables)

        # Define flow
        workflow.set_entry_point("determine_strategy")
        workflow.add_edge("determine_strategy", "gather_context")
        workflow.add_edge("gather_context", "analyze_context")
        workflow.add_edge("analyze_context", "extract_tables")
        workflow.add_edge("extract_tables", END)

        return workflow

    async def determine_strategy(self, state: ContextState) -> Dict:
        """Determine the search strategy based on query"""
        query = state.query.lower()
        session_context = state.session_context

        # Determine strategy
        if any(word in query for word in ['show', 'list', 'all', 'tables', 'datasets', 'schema']):
            strategy = 'schema_focused'
        elif any(word in query for word in ['example', 'how to', 'sample', 'demo']):
            strategy = 'example_focused'
        elif session_context.get('has_reference'):
            strategy = 'context_aware'
        elif any(word in query for word in ['count', 'sum', 'average', 'max', 'min']):
            strategy = 'aggregation_focused'
        else:
            strategy = 'comprehensive'

        logger.info("strategy_determined",
                    strategy=strategy,
                    task_id=state.task_id)

        return {"search_strategy": strategy}

    async def gather_context(self, state: ContextState) -> Dict:
        """Gather context from RAG"""

        query = state.query
        strategy = state.search_strategy
        task_id = state.task_id

        logger.debug("Starting context gathering",
                     task_id=task_id,
                     strategy=strategy,
                     has_rag=bool(self.rag_tool))

        # Initialize results
        context_data = {
            "sql_examples": [],
            "table_schemas": [],
            "business_knowledge": [],
            "dbt_models": []
        }

        if not self.rag_tool:
            logger.warning("RAG tool not available, skipping context gathering",
                           task_id=task_id)
            return context_data

        try:
            from workers.tools.rag import SearchStrategy

            # Map our strategy to RAG SearchStrategy
            rag_strategy_map = {
                'comprehensive': SearchStrategy.COMPREHENSIVE,
                'schema_focused': SearchStrategy.SCHEMA_ONLY,
                'example_focused': SearchStrategy.SQL_GENERATION,
                'context_aware': SearchStrategy.BUSINESS_CONTEXT,
                'aggregation_focused': SearchStrategy.SQL_GENERATION
            }

            rag_strategy = rag_strategy_map.get(strategy, SearchStrategy.COMPREHENSIVE)

            # Single search with strategy
            search_results = await self.rag_tool.search_knowledge(
                query=query,
                strategy=rag_strategy,
                limit_per_collection=5
            )

            # Sort results by collection type
            for result in search_results:
                collection = result.get('collection', '')

                if collection == 'sql_examples':
                    context_data["sql_examples"].append({
                        'text': result.get('sql', '') or result.get('content', ''),
                        'score': result.get('weighted_score', 0),
                        'metadata': result.get('metadata', {}),
                        'question': result.get('question', '')
                    })
                elif collection == 'table_schemas':
                    context_data["table_schemas"].append({
                        'text': result.get('content', ''),
                        'score': result.get('weighted_score', 0),
                        'metadata': result.get('metadata', {}),
                        'table_name': result.get('table_name', ''),
                        'columns': result.get('columns', [])
                    })
                elif collection == 'business_knowledge':
                    context_data["business_knowledge"].append({
                        'text': result.get('content', ''),
                        'score': result.get('weighted_score', 0),
                        'metadata': result.get('metadata', {})
                    })
                elif collection == 'dbt_models':
                    context_data["dbt_models"].append({
                        'text': result.get('content', ''),
                        'score': result.get('weighted_score', 0),
                        'metadata': result.get('metadata', {}),
                        'model_name': result.get('model_name', ''),
                        'sql_preview': result.get('sql_preview', '')
                    })

            logger.info("context_gathering_complete",
                        task_id=task_id,
                        strategy=strategy,
                        total_results=len(search_results),
                        examples=len(context_data["sql_examples"]),
                        schemas=len(context_data["table_schemas"]),
                        knowledge=len(context_data["business_knowledge"]),
                        models=len(context_data["dbt_models"]))

        except Exception as e:
            logger.error("Context gathering failed",
                         error=str(e),
                         error_type=type(e).__name__,
                         task_id=task_id)

        return context_data

    async def analyze_context(self, state: ContextState) -> Dict:
        """Analyze gathered context for relevance"""

        # Calculate relevance scores
        relevance_scores = {}

        for example in state.sql_examples:
            if 'score' in example:
                relevance_scores[example.get('id', '')] = example['score']

        for schema in state.table_schemas:
            if 'score' in schema:
                relevance_scores[schema.get('id', '')] = schema['score']

        logger.debug("Context analysis complete",
                     task_id=state.task_id,
                     total_scores=len(relevance_scores))

        return {"relevance_scores": relevance_scores}

    async def extract_tables(self, state: ContextState) -> Dict:
        """Extract table names from context"""

        selected_tables = []

        # Extract from schemas
        for schema in state.table_schemas:
            if 'metadata' in schema and 'table_name' in schema['metadata']:
                table_name = schema['metadata']['table_name']
                if table_name not in selected_tables:
                    selected_tables.append(table_name)

        # Extract from SQL examples
        for example in state.sql_examples:
            text = example.get('text', '')
            # Simple extraction - look for backtick-quoted table names
            import re
            tables = re.findall(r'`([^`]+)`', text)
            for table in tables:
                if '.' in table and table not in selected_tables:
                    selected_tables.append(table)

        logger.debug("Table extraction complete",
                     task_id=state.task_id,
                     tables_found=len(selected_tables))

        return {"selected_tables": selected_tables}

    async def run(self, state: Dict) -> Dict:
        """Run the context gathering subgraph"""
        try:
            result = await self.app.ainvoke(state)
            return dict(result)  # Convert from ContextState to dict
        except Exception as e:
            logger.error("Context subgraph failed", error=str(e))
            return {
                "error": f"Context gathering failed: {str(e)}",
                "sql_examples": [],
                "table_schemas": [],
                "business_knowledge": [],
                "dbt_models": []
            }