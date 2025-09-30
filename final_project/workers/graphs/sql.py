"""
ИСПРАВЛЕНИЕ: Правильная типизация для SQLGenerator
Location: workers/graphs/sql.py

Проблема: LangGraph работает с Dict, не с TypedDict
"""

import re
from typing import Dict, Any, Optional

import structlog
from langgraph.graph import StateGraph, END
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_openai import ChatOpenAI

from workers.tools.bigquery import BigQueryTool
from workers.config import settings

logger = structlog.get_logger()


class SQLGenerator:
    """
    SQL generation subgraph - FIXED VERSION with correct typing
    """

    def __init__(
            self,
            llm: Optional[ChatOpenAI] = None,
            bigquery_tool: Optional[BigQueryTool] = None
    ):
        logger.debug("🔍 DEBUG: Initializing SQLGenerator")

        self.llm = llm or ChatOpenAI(
            model=settings.openai_model,
            temperature=settings.openai_temperature
        )

        self.bigquery_tool = bigquery_tool or BigQueryTool()
        self._init_prompts()
        self.graph = self._build_graph()

        self.dangerous_keywords = [
            "DROP", "DELETE", "TRUNCATE", "UPDATE",
            "INSERT", "CREATE", "ALTER", "MERGE"
        ]

        logger.debug("🔍 DEBUG: SQLGenerator initialized",
                    llm_model=settings.openai_model,
                    has_bigquery_tool=bool(self.bigquery_tool))

    def _init_prompts(self):
        """Initialize prompt templates"""
        self.sql_generation_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a BigQuery SQL expert. Generate optimal, safe SQL queries.

RULES:
1. NEVER use SELECT * - list columns explicitly
2. ALWAYS filter on partition columns when available
3. Use parameterized queries for user inputs
4. Add LIMIT for exploratory queries
5. Use SAFE functions where appropriate
6. Include comments explaining the query
7. Optimize for minimal data scanning

Return ONLY the SQL query, no explanations."""),
            ("human", "Query: {query}\n\nContext:\n{context}")
        ])

        self.refinement_prompt = ChatPromptTemplate.from_messages([
            ("system", "Fix the SQL based on validation feedback. Return only the corrected SQL."),
            ("human", "SQL: {sql}\nIssues: {issues}\nFix these issues and return the corrected SQL.")
        ])

    def _build_graph(self) -> StateGraph:
        """Build the SQL generation workflow"""
        # ВАЖНО: Используем Dict, не SQLState!
        workflow = StateGraph(dict)

        # Add nodes - все методы принимают и возвращают Dict
        workflow.add_node("prepare_context", self.prepare_context)
        workflow.add_node("generate_sql", self.generate_sql)
        workflow.add_node("validate_sql", self.validate_sql)
        workflow.add_node("estimate_cost", self.estimate_cost)
        workflow.add_node("optimize_if_needed", self.optimize_if_needed)

        # Define flow
        workflow.set_entry_point("prepare_context")
        workflow.add_edge("prepare_context", "generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")

        # Conditional edge based on validation
        workflow.add_conditional_edges(
            "validate_sql",
            lambda state: "estimate" if state.get("validation_passed") else "end",
            {
                "estimate": "estimate_cost",
                "end": END
            }
        )

        workflow.add_edge("estimate_cost", "optimize_if_needed")
        workflow.add_edge("optimize_if_needed", END)

        return workflow.compile()

    def prepare_context(self, state: Dict) -> str:
        """Prepare context from RAG results for prompt"""
        logger.debug("🔍 DEBUG SQL: prepare_context called",
                     task_id=state.get("task_id"),
                     has_context=bool(state.get("context")),
                     context_keys=list(state.get("context", {}).keys()))

        context_parts = []

        # SQL примеры
        sql_examples = state.get("sql_examples", [])
        if sql_examples:
            context_parts.append("=== Relevant SQL Examples ===")
            for i, example in enumerate(sql_examples[:3], 1):
                text = example.get('text', '')
                context_parts.append(f"Example {i}: {text[:300]}")
            context_parts.append("")

        # Схемы таблиц
        table_schemas = state.get("table_schemas", [])
        if table_schemas:
            context_parts.append("=== Relevant Table Schemas ===")
            for schema in table_schemas[:5]:
                text = schema.get('text', '')
                context_parts.append(f"- {text[:200]}")
            context_parts.append("")

        # Бизнес знания
        business_knowledge = state.get("business_knowledge", [])
        if business_knowledge:
            context_parts.append("=== Business Context ===")
            for knowledge in business_knowledge[:2]:
                text = knowledge.get('text', '')
                context_parts.append(f"- {text[:200]}")

        context = "\n".join(context_parts)

        logger.debug("🔍 DEBUG SQL: prepare_context called",
                     task_id=state.get("task_id"),
                     has_context=bool(context),
                     context_keys=['table_schemas', 'sql_examples', 'business_knowledge'])

        state["prepared_context"] = context
        return state

    async def generate_sql(self, state: Dict) -> Dict:
        """Generate SQL query using LLM"""
        logger.debug("🔍 DEBUG SQL: generate_sql called",
                     task_id=state.get("task_id"),
                     query=state.get("query"),
                     has_prepared_context=bool(state.get("prepared_context")))

        try:
            if not state.get("query"):
                state["error"] = "No query provided"
                state["generated_sql"] = ""
                return state

            # ИСПРАВЛЕНИЕ: правильная обработка контекста из словарей
            context_dict = state.get("context", {})
            context_parts = []

            # Извлекаем текст из словарей
            if context_dict.get("table_schemas"):
                schemas = []
                for item in context_dict["table_schemas"][:3]:
                    if isinstance(item, dict):
                        schemas.append(item.get("content", "") or item.get("text", "") or str(item))
                    else:
                        schemas.append(str(item))
                if schemas:
                    context_parts.append("Available tables:\n" + "\n".join(schemas))

            if context_dict.get("sql_examples"):
                examples = []
                for item in context_dict["sql_examples"][:2]:
                    if isinstance(item, dict):
                        examples.append(item.get("content", "") or item.get("text", "") or str(item))
                    else:
                        examples.append(str(item))
                if examples:
                    context_parts.append("Similar queries:\n" + "\n".join(examples))

            if context_dict.get("business_knowledge"):
                knowledge = []
                for item in context_dict["business_knowledge"][:2]:
                    if isinstance(item, dict):
                        knowledge.append(item.get("content", "") or item.get("text", "") or str(item))
                    else:
                        knowledge.append(str(item))
                if knowledge:
                    context_parts.append("Business context:\n" + "\n".join(knowledge))

            context = "\n\n".join(context_parts) if context_parts else ""

            prompt = f"""
            <prompt>
              <role>
                You are an expert in BigQuery SQL. 
                Your task is to generate valid, executable BigQuery SQL queries.
              </role>

              <input>
                <user_request>{state["query"]}</user_request>
                <context>{context if context else "No specific context available. Use general BigQuery knowledge."}</context>
              </input>

              <rules>
                <rule>ALWAYS use the project ID: gcp-project</rule>
                <rule>For metadata (tables, columns, partitions) use INFORMATION_SCHEMA</rule>
                <rule>NEVER use placeholder names like 'project', 'your_project', or 'dataset'</rule>
                <rule>NEVER explain the query, only return the raw SQL</rule>
                <rule>Ensure queries are syntactically valid for BigQuery</rule>
              </rules>

              <output>
                <format>Return ONLY the SQL query as plain text</format>
              </output>
            </prompt>
            """

            # Call LLM
            response = await self.llm.ainvoke(prompt)
            sql = response.content if hasattr(response, 'content') else str(response)

            # Extract SQL from response
            sql = self._extract_sql_from_response(sql)

            # ДОБАВЛЕНО: Проверка и замена плейсхолдеров
            if 'your_project' in sql or 'project.dataset' in sql:
                logger.warning("Found placeholder in SQL, fixing",
                               task_id=state.get("task_id"),
                               original_sql=sql[:200])

                sql = sql.replace('your_project', 'gcp-project')
                sql = sql.replace('project.dataset', 'gcp-project.raw_solana')
                sql = sql.replace('`project.', '`gcp-project.')

            logger.debug("🔍 DEBUG SQL: SQL generated",
                         prompt=prompt,
                         task_id=state.get("task_id"),
                         sql_preview=sql[:200],
                         sql_length=len(sql))

            state["generated_sql"] = sql
            state["sql_explanation"] = f"Generated SQL for: {state['query']}"

        except Exception as e:
            logger.error("SQL generation failed",
                         task_id=state.get("task_id"),
                         error=str(e))
            state["error"] = f"SQL generation failed: {str(e)}"
            state["generated_sql"] = ""

        return state

    # ИСПРАВЛЕНО: Dict вместо SQLState
    async def validate_sql(self, state: Dict) -> Dict:
        """Validate generated SQL"""
        sql = state.get("generated_sql", "")

        if not sql:
            state["validation_passed"] = False
            state["validation_issues"] = ["No SQL query generated"]
            return state

        issues = []
        warnings = []

        # Check for dangerous operations
        for keyword in self.dangerous_keywords:
            if keyword in sql.upper():
                issues.append(f"Dangerous operation detected: {keyword}")

        # Check for SELECT *
        if "SELECT *" in sql.upper() and "INFORMATION_SCHEMA" not in sql.upper():
            issues.append("SELECT * is not allowed. Please specify columns explicitly.")

        # Basic syntax check
        if not sql.strip().upper().startswith(("SELECT", "WITH")):
            issues.append("Query must start with SELECT or WITH")

        state["validation_passed"] = len(issues) == 0
        state["validation_issues"] = issues
        state["validation_warnings"] = warnings

        logger.debug("SQL validation complete",
                    task_id=state.get("task_id"),
                    passed=state["validation_passed"],
                    issues=issues)

        return state

    # ИСПРАВЛЕНО: Dict вместо SQLState
    async def estimate_cost(self, state: Dict) -> Dict:
        """Estimate query cost via dry run"""
        if not state.get("generated_sql") or not state.get("validation_passed"):
            return state

        try:
            if self.bigquery_tool:
                dry_run_result = await self.bigquery_tool.dry_run(
                    state["generated_sql"]
                )
                state["estimated_bytes"] = dry_run_result.get("bytes_billed", 0)
                state["estimated_cost"] = dry_run_result.get("estimated_cost_usd", 0)

                logger.debug("Cost estimation complete",
                           task_id=state.get("task_id"),
                           bytes_gb=state["estimated_bytes"] / (1024**3),
                           cost_usd=state["estimated_cost"])
        except Exception as e:
            logger.warning("Cost estimation failed",
                          task_id=state.get("task_id"),
                          error=str(e))
            state["validation_warnings"] = state.get("validation_warnings", [])
            state["validation_warnings"].append(f"Could not estimate cost: {str(e)}")

        return state

    # ИСПРАВЛЕНО: Dict вместо SQLState
    async def optimize_if_needed(self, state: Dict) -> Dict:
        """Apply optimizations if query is expensive"""
        estimated_bytes = state.get("estimated_bytes")
        if estimated_bytes and estimated_bytes > 10 * (1024 ** 3):  # > 10GB
            state["optimization_hints"] = [
                "Consider adding partition filters",
                "Use APPROX functions for large aggregations",
                "Add LIMIT clause for exploration"
            ]

        return state

    def _extract_sql_from_response(self, response: str) -> str:
        """Extract clean SQL from LLM response"""
        # Remove markdown code blocks
        if "```sql" in response:
            sql = response.split("```sql")[1].split("```")[0]
        elif "```" in response:
            sql = response.split("```")[1].split("```")[0]
        else:
            sql = response

        return sql.strip()


# ============================================================
# ТЕСТ для проверки
# ============================================================

async def test_sql_generator():
    """Test SQLGenerator with Dict state"""
    from workers.config import settings

    generator = SQLGenerator()

    # Test state - обычный Dict, не TypedDict!
    test_state = {
        "task_id": "test-001",
        "query": "count tables in raw_solana dataset",
        "context": {
            "table_schemas": ["INFORMATION_SCHEMA.TABLES"],
            "sql_examples": []
        },
        "session_context": {}  # Пустой контекст сессии
    }

    # Запускаем workflow
    result = await generator.graph.ainvoke(test_state)

    print("Result type:", type(result))  # Должно быть dict
    print("Generated SQL:", result.get("generated_sql"))
    print("Validation passed:", result.get("validation_passed"))

    assert isinstance(result, dict), "Result must be dict"
    assert result.get("generated_sql"), "SQL must be generated"

    print("✅ SQLGenerator test passed!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_sql_generator())