"""
Slack Communicator for sending updates from workers
"""

import json
import traceback
import re
import base64
from typing import Dict, Any, Optional, List  # ДОБАВЛЕНО List
from datetime import datetime
from decimal import Decimal

import structlog
import redis.asyncio as redis
from langchain_openai import ChatOpenAI  # ДОБАВЛЕНО для LLM интерпретации

from workers.config import settings

logger = structlog.get_logger()


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and other special types"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        elif hasattr(obj, '__dict__'):
            return str(obj)
        return super().default(obj)


class SlackCommunicator:
    """Handles communication from workers to Slack bot via Redis Pub/Sub"""

    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.pubsub_channel = settings.pubsub_channel

        # ДОБАВЛЕНО: Инициализация LLM для интерпретации
        self.interpreter_llm = ChatOpenAI(
            model=settings.openai_model,
            temperature=0.3  # Низкая температура для точной интерпретации
        )

    async def send_progress(self, task: Dict[str, Any], message: str):
        """Send progress update to Slack (alias for send_status_update)"""
        await self.send_status_update(task, message)

    async def send_status_update(self, task: Dict, message: str, blocks: Optional[list] = None):
        """Send status update to Slack"""

        try:
            update = {
                "type": "status_update",
                "task_id": task["id"],
                "slack_context": task.get("slack_context", {}),
                "message": message,
                "blocks": blocks,
                "timestamp": datetime.utcnow().isoformat()
            }

            await self._publish(update)
            logger.debug("sent_status_update", task_id=task["id"], message=message)

        except Exception as e:
            logger.error("failed_to_send_status_update",
                         error=str(e),
                         task_id=task.get("id"),
                         traceback=traceback.format_exc())

    async def send_result(self, task: Dict[str, Any], result: Dict[str, Any]):
        """Send final result to Slack (alias for send_query_result)"""
        await self.send_query_result(task, result)

    async def send_query_result(self, task: Dict, result: Dict[str, Any]):
        """Send query execution result to Slack bot for posting"""

        try:
            # Логирование входящих данных
            logger.debug("DEBUG send_query_result input",
                        task_id=task.get("id"),
                        result_keys=list(result.keys()) if result else None,
                        has_html_report=bool(result.get("html_report")),
                        has_results=bool(result.get("results")),
                        total_rows=result.get("total_rows"),
                        rows_count=result.get("rows_count"),
                        results_len=len(result.get("results", [])) if result else 0)

            # Очищаем результат для JSON
            clean_result = self._clean_for_json(result)

            # Форматируем блоки для Slack
            if result.get("report"):
                blocks = self._format_result_blocks(result["report"])
            else:
                # Fallback если нет отчета
                simple_report = {
                    "summary": f"Query completed successfully. Found {clean_result.get('total_rows', 0)} rows.",
                    "preview": clean_result.get("preview", clean_result.get("results", []))[:10],
                    "results": clean_result.get("results", []),
                    "sql": clean_result.get("sql", ""),
                    "rows_count": clean_result.get("total_rows", 0),
                    "bytes_processed_gb": clean_result.get("bytes_processed", 0) / (1024 ** 3) if clean_result.get(
                        "bytes_processed") else 0,
                    "table_link": clean_result.get("table_link"),
                    "execution_time_ms": clean_result.get("execution_time_ms", 0),
                    "estimated_cost": clean_result.get("estimated_cost", 0),
                    "cache_hit": clean_result.get("cache_hit", False)
                }
                blocks = self._format_result_blocks(simple_report)

            # ИЗМЕНЕНО: Кодируем HTML для передачи через Redis
            html_report_encoded = None
            if result.get("html_report"):
                try:
                    # Кодируем HTML в base64 для безопасной передачи через JSON
                    html_bytes = result["html_report"].encode('utf-8')
                    html_report_encoded = base64.b64encode(html_bytes).decode('ascii')
                    logger.debug("html_report_encoded",
                               task_id=task.get("id"),
                               original_size=len(result["html_report"]),
                               encoded_size=len(html_report_encoded))
                except Exception as e:
                    logger.error("html_encoding_failed",
                               task_id=task.get("id"),
                               error=str(e))

            # Отправляем результат через Redis
            update = {
                "type": "query_result",
                "task_id": task["id"],
                "slack_context": task.get("slack_context", {}),
                "message": self._format_result_message(clean_result),
                "result": clean_result,
                "blocks": blocks,
                "html_report_encoded": html_report_encoded,  # ИЗМЕНЕНО: Передаем закодированный HTML
                "timestamp": datetime.utcnow().isoformat()
            }

            await self._publish(update)

            logger.info("sent_query_result",
                       task_id=task["id"],
                       rows=clean_result.get("rows_count", clean_result.get("total_rows", 0)),
                       has_html_report=bool(html_report_encoded))

        except Exception as e:
            logger.error("failed_to_send_query_result",
                        error=str(e),
                        task_id=task.get("id"),
                        result_keys=list(result.keys()) if result else None,
                        traceback=traceback.format_exc())

            await self.send_error(
                task,
                "Failed to format query results",
                details=str(e)
            )

    # ДОБАВЛЕНО: Новый метод для LLM интерпретации
    async def _get_llm_interpretation(
            self,
            question: str,
            sql: str,
            results: List[Dict],
            total_rows: int
    ) -> Optional[str]:
        """
        Get intelligent interpretation of results using LLM
        """
        try:
            # Определяем, нужна ли LLM интерпретация
            if not self._needs_llm_interpretation(question, sql, results):
                # Используем простую интерпретацию для базовых запросов
                return self._get_simple_interpretation(sql, results, total_rows)

            # Подготавливаем промпт для LLM
            prompt = f"""You are a data analyst assistant. Interpret the query results in a clear, concise way.

User Question: {question}

SQL Query Executed:
{sql}

Query Results (first {len(results)} rows of {total_rows} total):
{self._format_results_for_prompt(results)}

Provide a brief, insightful interpretation of these results that directly answers the user's question.
Focus on:
1. What the data shows
2. Key insights or patterns
3. Business implications if relevant

Keep the interpretation to 2-3 sentences maximum.
Do not repeat the SQL or restate the question.
Be specific with numbers and facts from the results."""

            # Вызываем LLM
            response = await self.interpreter_llm.ainvoke(prompt)
            interpretation = response.content if hasattr(response, 'content') else str(response)

            # Очищаем и форматируем ответ
            interpretation = interpretation.strip()

            # Ограничиваем длину для Slack
            if len(interpretation) > 500:
                interpretation = interpretation[:497] + "..."

            logger.debug("LLM interpretation generated",
                         question_length=len(question),
                         interpretation_length=len(interpretation))

            return interpretation

        except Exception as e:
            logger.warning("Failed to get LLM interpretation",
                           error=str(e),
                           question=question[:100])
            # Fallback на простую интерпретацию
            return self._get_simple_interpretation(sql, results, total_rows)

    # ДОБАВЛЕНО: Метод для определения необходимости LLM
    def _needs_llm_interpretation(
            self,
            question: str,
            sql: str,
            results: List[Dict]
    ) -> bool:
        """
        Determine if LLM interpretation would be valuable
        """
        question_lower = question.lower()
        sql_lower = sql.lower()

        # Простые COUNT запросы не требуют LLM
        if "count(*)" in sql_lower and "group by" not in sql_lower:
            return False

        # Вопросы требующие интерпретации
        interpretation_keywords = [
            "why", "what does", "explain", "analyze",
            "compare", "trend", "pattern", "insight",
            "significant", "unusual", "interesting"
        ]
        if any(keyword in question_lower for keyword in interpretation_keywords):
            return True

        # Сложные агрегации
        if "group by" in sql_lower and len(results) > 1:
            return True

        # Множественные метрики
        if results and len(results[0]) > 3:
            return True

        # Вычисления и выражения
        if any(op in sql_lower for op in ["avg(", "sum(", "stddev(", "percentile"]):
            return True

        # JOIN запросы обычно требуют объяснения
        if "join" in sql_lower:
            return True

        return False

    # ДОБАВЛЕНО: Форматирование результатов для промпта
    def _format_results_for_prompt(self, results: List[Dict], max_rows: int = 10) -> str:
        """Format results for LLM prompt"""
        if not results:
            return "No results returned"

        lines = []

        # Заголовки
        headers = list(results[0].keys())
        lines.append(" | ".join(headers))
        lines.append("-" * 50)

        # Данные
        for row in results[:max_rows]:
            values = [str(row.get(h, "")) for h in headers]
            lines.append(" | ".join(values))

        return "\n".join(lines)

    # ДОБАВЛЕНО: Простая интерпретация
    def _get_simple_interpretation(
            self,
            sql: str,
            results: List[Dict],
            total_rows: int
    ) -> Optional[str]:
        """
        Simple rule-based interpretation for basic queries
        """
        try:
            sql_lower = sql.lower()

            # Для COUNT запросов
            if "count(*)" in sql_lower or "count(1)" in sql_lower:
                if results and len(results) > 0:
                    first_row = results[0]
                    count_value = None
                    for key, value in first_row.items():
                        if 'count' in key.lower() or key == 'f0_':
                            count_value = value
                            break

                    if count_value is not None:
                        if "information_schema.tables" in sql_lower:
                            if "raw_solana" in sql_lower:
                                return f"The raw_solana dataset contains {count_value} tables."
                            else:
                                return f"Found {count_value} tables in the dataset."
                        else:
                            return f"Count result: {count_value}"

            # Для других простых случаев
            if total_rows == 1 and results:
                return None  # Не нужна интерпретация для одной строки
            elif total_rows > 10:
                return f"Query returned {total_rows} rows. Showing first 10 rows."

            return None

        except Exception as e:
            logger.debug("Simple interpretation failed", error=str(e))
            return None

    async def send_confirmation_request(self, task: Dict, dry_run_result: Dict):
        """Send confirmation request to Slack"""

        try:
            # Generate interaction ID for this confirmation
            interaction_id = f"{task['id']}_confirm"

            # Clean dry run result
            clean_dry_run = self._clean_for_json(dry_run_result)

            # Store session data for later
            await self._store_session(interaction_id, {
                "task": task,
                "dry_run_result": clean_dry_run,
                "sql": clean_dry_run.get("sql", "")
            })

            # Format confirmation blocks
            blocks = self._format_confirmation_blocks(clean_dry_run)

            update = {
                "type": "request_confirmation",
                "task_id": task["id"],
                "slack_context": task.get("slack_context", {}),
                "message": "Query validation complete. Please review and confirm.",
                "dry_run_result": clean_dry_run,
                "blocks": blocks,
                "interaction_id": interaction_id,
                "timestamp": datetime.utcnow().isoformat()
            }

            await self._publish(update)
            logger.info("sent_confirmation_request",
                        task_id=task["id"],
                        bytes_gb=clean_dry_run.get("total_bytes_billed", 0) / (1024 ** 3))

        except Exception as e:
            logger.error("failed_to_send_confirmation",
                         error=str(e),
                         task_id=task.get("id"),
                         traceback=traceback.format_exc())

    async def send_error(self, task: Dict, error: str, details: Optional[str] = None):
        """Send error message to Slack"""

        try:
            # Format error blocks
            blocks = self._format_error_blocks(error, details)

            update = {
                "type": "error",
                "task_id": task["id"],
                "slack_context": task.get("slack_context", {}),
                "message": f"❌ Query failed: {error}",
                "error": error,
                "details": details,
                "blocks": blocks,
                "timestamp": datetime.utcnow().isoformat()
            }

            await self._publish(update)
            logger.error("sent_error", task_id=task["id"], error=error)

        except Exception as e:
            logger.error("failed_to_send_error",
                         original_error=error,
                         send_error=str(e),
                         task_id=task.get("id"),
                         traceback=traceback.format_exc())

    async def _publish(self, update: Dict):
        """Publish update to Redis Pub/Sub"""

        try:
            # Use custom encoder for JSON serialization
            message = json.dumps(update, cls=JSONEncoder)
            await self.redis_client.publish(self.pubsub_channel, message)

            logger.debug("published_to_redis",
                         channel=self.pubsub_channel,
                         update_type=update.get("type"),
                         message_size=len(message))

        except TypeError as e:
            # JSON serialization error - log the problematic data
            logger.error("json_serialization_error",
                         error=str(e),
                         update_type=update.get("type"),
                         update_keys=list(update.keys()),
                         traceback=traceback.format_exc())

            # Try to find what's not serializable
            for key, value in update.items():
                try:
                    json.dumps(value, cls=JSONEncoder)
                except:
                    logger.error("non_serializable_field",
                                 field=key,
                                 type=type(value).__name__,
                                 value=str(value)[:100])
            raise

        except Exception as e:
            logger.error("failed_to_publish",
                         error=str(e),
                         update_type=update.get("type"),
                         traceback=traceback.format_exc())
            raise

    async def _store_session(self, session_id: str, data: Dict):
        """Store session data in Redis"""

        try:
            key = f"session:{session_id}"
            # Clean data before storing
            clean_data = self._clean_for_json(data)
            value = json.dumps(clean_data, cls=JSONEncoder)

            # Store with 1 hour TTL
            await self.redis_client.setex(key, 3600, value)
            logger.debug("stored_session", session_id=session_id)

        except Exception as e:
            logger.error("failed_to_store_session",
                         error=str(e),
                         session_id=session_id,
                         traceback=traceback.format_exc())

    def _clean_for_json(self, obj: Any) -> Any:
        """Recursively clean object for JSON serialization"""

        if obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        elif isinstance(obj, dict):
            return {k: self._clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._clean_for_json(item) for item in obj]
        else:
            # For unknown types, convert to string
            return str(obj)

    def _format_result_message(self, result: Dict) -> str:
        """Format result message for Slack"""

        try:
            rows = result.get("rows_count", 0)
            time = result.get("execution_time", 0)
            cost = result.get("estimated_cost", result.get("cost_usd", 0))
            cache = "✓ Cache hit" if result.get("cache_hit") else ""

            parts = [
                f"✅ Query completed successfully",
                f"• Returned {rows:,} rows in {time:.1f}s" if isinstance(rows,
                                                                         (int, float)) else f"• Returned {rows} rows",
                f"• Cost: ${cost:.4f}" if cost else ""
            ]

            if cache:
                parts.append(f"• {cache}")

            # Filter out empty parts
            parts = [p for p in parts if p]

            return "\n".join(parts)

        except Exception as e:
            logger.error("failed_to_format_result",
                         error=str(e),
                         result_keys=list(result.keys()) if result else None)
            return "✅ Query completed (see details below)"

    # ОБНОВЛЕНО: Добавлено отображение результатов и интерпретации
    def _format_result_blocks(self, report: Dict[str, Any]) -> list:
        """Format result as Slack blocks"""

        blocks = []

        # Summary section с метриками
        rows_count = report.get("rows_count", 0)
        bytes_gb = report.get("bytes_processed_gb", 0)
        execution_time_ms = report.get("execution_time_ms", 0)
        cost = report.get("estimated_cost", 0)
        cache_hit = report.get("cache_hit", False)

        # Заголовок с основными метриками
        metrics_text = [f"*✅ Query Completed - {rows_count:,} rows*"]

        sub_metrics = []
        if execution_time_ms:
            sub_metrics.append(f"⏱ {execution_time_ms:.0f}ms")
        if bytes_gb > 0:
            sub_metrics.append(f"💾 {bytes_gb:.2f}GB")
        if cost > 0:
            sub_metrics.append(f"💰 ${cost:.4f}")
        if cache_hit:
            sub_metrics.append("🚀 Cached")

        if sub_metrics:
            metrics_text.append(" | ".join(sub_metrics))

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(metrics_text)
            }
        })

        # Data preview - используем results если есть, иначе preview
        data_to_show = report.get("results", []) or report.get("preview", [])

        if data_to_show and rows_count > 0:
            # Ограничиваем preview для Slack
            preview_rows = 10
            result_text = f"*Data Preview (first {min(preview_rows, len(data_to_show))} rows):*\n```\n"

            if len(data_to_show) > 0:
                # Получаем заголовки
                headers = list(data_to_show[0].keys())

                # Ограничиваем количество столбцов для читаемости
                max_cols = 5
                if len(headers) > max_cols:
                    headers = headers[:max_cols]
                    cols_truncated = True
                else:
                    cols_truncated = False

                # Форматируем заголовок таблицы
                header_row = " | ".join(str(h)[:15] for h in headers)
                result_text += header_row + "\n"
                result_text += "-" * min(60, len(header_row)) + "\n"

                # Добавляем строки данных
                for i, row in enumerate(data_to_show[:preview_rows]):
                    values = []
                    for h in headers:
                        val = str(row.get(h, ""))
                        if len(val) > 15:
                            val = val[:12] + "..."
                        values.append(val)
                    result_text += " | ".join(values) + "\n"

                # Информация об усечении
                if rows_count > preview_rows:
                    result_text += f"\n... +{rows_count - preview_rows} more rows"
                if cols_truncated:
                    result_text += f"\n... showing {max_cols} of {len(list(data_to_show[0].keys()))} columns"

            result_text += "```"

            # Добавляем блок с данными если он помещается в лимит Slack
            if len(result_text) < 2900:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": result_text
                    }
                })

        # Ссылка на BigQuery
        table_link = report.get("table_link")
        if table_link:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"📊 <{table_link}|View full results in BigQuery>"
                }
            })

        # SQL запрос (компактно)
        sql = report.get("sql")
        if sql:
            sql_preview = sql.replace('\n', ' ')[:150]
            if len(sql) > 150:
                sql_preview += "..."
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Query: `{sql_preview}`"
                    }
                ]
            })

        return blocks

    def _format_confirmation_blocks(self, dry_run_result: Dict[str, Any]) -> list:
        """Format confirmation request as Slack blocks"""

        blocks = []

        # Warning header
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⚠️ *Large Query Detected*"
            }
        })

        # Details
        gb = dry_run_result.get('total_bytes_billed', 0) / (1024 ** 3)
        cost = dry_run_result.get('estimated_cost', 0)

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"This query will process *{gb:.2f} GB* of data\n"
                    f"Estimated cost: *${cost:.2f}*\n\n"
                    "Do you want to proceed?"
                )
            }
        })

        # Show SQL preview if available
        if dry_run_result.get("sql"):
            sql_preview = dry_run_result["sql"][:500]
            if len(dry_run_result["sql"]) > 500:
                sql_preview += "..."

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```sql\n{sql_preview}\n```"
                }
            })

        # Action buttons
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "✅ Proceed"
                    },
                    "style": "primary",
                    "action_id": "confirm_query"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "❌ Cancel"
                    },
                    "style": "danger",
                    "action_id": "cancel_query"
                }
            ]
        })

        return blocks

    def _format_error_blocks(self, error: str, details: Optional[str] = None) -> list:
        """Format error message as Slack blocks"""

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"❌ *Query Failed*\n{error}"
                }
            }
        ]

        if details:
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Details: {details[:200]}"  # Limit details length
                    }
                ]
            })

        # Add retry button
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "🔄 Retry"
                    },
                    "action_id": "retry_query"
                }
            ]
        })

        return blocks

    def _format_preview(self, rows: list, max_rows: int = 10) -> str:
        """Format preview data as table"""

        if not rows:
            return "No data"

        # Get sample rows
        sample = rows[:max_rows]

        # Get column names
        columns = list(sample[0].keys()) if sample else []

        if not columns:
            return "No columns"

        # Limit columns for display
        max_cols = 5
        if len(columns) > max_cols:
            columns = columns[:max_cols]
            truncated = True
        else:
            truncated = False

        # Calculate column widths (max 20 chars per column)
        widths = {}
        for col in columns:
            widths[col] = min(
                20,
                max(
                    len(str(col)),
                    max(len(str(row.get(col, ""))[:20]) for row in sample)
                )
            )

        # Format header
        header = " | ".join(
            str(col)[:widths[col]].ljust(widths[col])
            for col in columns
        )
        separator = "-+-".join("-" * widths[col] for col in columns)

        # Format rows
        formatted_rows = []
        for row in sample:
            formatted_row = " | ".join(
                str(row.get(col, ""))[:widths[col]].ljust(widths[col])
                for col in columns
            )
            formatted_rows.append(formatted_row)

        # Combine
        result = [header, separator] + formatted_rows

        # Add notes about truncation
        notes = []
        if len(rows) > max_rows:
            notes.append(f"... {len(rows) - max_rows} more rows")
        if truncated:
            notes.append(f"... {len(list(sample[0].keys())) - max_cols} more columns")

        if notes:
            result.append(" | ".join(notes))

        return "\n".join(result)

    def _get_bq_console_url(self, table_ref: str) -> str:
        """Get BigQuery console URL for table"""

        try:
            # Handle different formats: project.dataset.table or `project.dataset.table`
            table_ref = table_ref.replace("`", "")
            parts = table_ref.split(".")

            if len(parts) == 3:
                project, dataset, table = parts
                return (
                    f"https://console.cloud.google.com/bigquery"
                    f"?project={project}"
                    f"&ws=!1m5!1m4!4m3!1s{project}!2s{dataset}!3s{table}"
                )
            else:
                # Fallback to search
                return f"https://console.cloud.google.com/bigquery?q={table_ref}"

        except Exception as e:
            logger.error("failed_to_format_bq_url", error=str(e), table_ref=table_ref)
            return "https://console.cloud.google.com/bigquery"