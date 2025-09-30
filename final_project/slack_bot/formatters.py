"""
Message formatters for Slack
"""

from typing import Dict, List, Optional, Any


def format_initial_response() -> str:
    """Format initial acknowledgment message"""
    return "🔍 Analyzing your request..."


def format_error_response(error: str) -> str:
    """Format error message"""
    return f"❌ An error occurred: {error}\n\nPlease try again or contact support if the issue persists."


def format_error_message(error: str) -> str:
    """Format error message for update"""
    return f"❌ Query failed: {error}"


def format_status_update(message: str) -> List[Dict]:
    """Format status update as Slack blocks"""
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message
            }
        }
    ]


def format_query_result(result: Dict) -> List[Dict]:
    """Format query execution result as Slack blocks"""

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "Query Results"
        }
    })

    # Metadata section
    metadata_text = []

    if "rows_count" in result:
        metadata_text.append(f"*Rows:* {result['rows_count']:,}")

    if "bytes_processed" in result:
        gb = result["bytes_processed"] / (1024 ** 3)
        metadata_text.append(f"*Data Processed:* {gb:.2f} GB")

    if "execution_time" in result:
        metadata_text.append(f"*Execution Time:* {result['execution_time']:.2f}s")

    if "cache_hit" in result and result["cache_hit"]:
        metadata_text.append("*Source:* 📦 Cached")

    if metadata_text:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": " | ".join(metadata_text)
            }
        })

    # SQL query (collapsed)
    if "sql" in result:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*SQL Query:*"
            }
        })
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```sql\n{_truncate_sql(result['sql'])}\n```"
            }
        })

    # Results preview
    if "preview" in result and result["preview"]:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Preview (first 10 rows):*"
            }
        })
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```\n{_format_table_preview(result['preview'])}\n```"
            }
        })

    # BigQuery table link
    if "table_link" in result:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{result['table_link']}|📊 View full results in BigQuery>"
            }
        })

    # Add divider
    blocks.append({"type": "divider"})

    return blocks


def format_confirmation_request(dry_run_result: Dict, interaction_id: str) -> List[Dict]:
    """Format confirmation request with buttons"""

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "Query Validation Complete"
        }
    })

    # Dry run results
    metadata = []

    if "bytes_billed" in dry_run_result:
        gb = dry_run_result["bytes_billed"] / (1024 ** 3)
        cost = gb * 5 / 1000  # ~$5 per TB
        metadata.append(f"*Estimated Scan:* {gb:.2f} GB (${cost:.4f})")

    if "referenced_tables" in dry_run_result:
        tables = ", ".join([f"`{t}`" for t in dry_run_result["referenced_tables"][:3]])
        metadata.append(f"*Tables:* {tables}")

    if metadata:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(metadata)
            }
        })

    # SQL query
    if "sql" in dry_run_result:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Generated SQL:*\n```sql\n{_truncate_sql(dry_run_result['sql'])}\n```"
            }
        })

    # Warning if high cost
    if "bytes_billed" in dry_run_result:
        gb = dry_run_result["bytes_billed"] / (1024 ** 3)
        if gb > 10:  # More than 10GB
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "⚠️ *Warning:* This query will process a large amount of data"
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
                    "text": "✅ Execute Query"
                },
                "style": "primary",
                "action_id": "execute_query",
                "value": interaction_id
            },
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "❌ Cancel"
                },
                "style": "danger",
                "action_id": "cancel_query",
                "value": interaction_id
            }
        ]
    })

    return blocks


def _truncate_sql(sql: str, max_lines: int = 20) -> str:
    """Truncate long SQL queries"""
    lines = sql.strip().split("\n")
    if len(lines) > max_lines:
        return "\n".join(lines[:max_lines]) + f"\n... ({len(lines) - max_lines} more lines)"
    return sql


def _format_table_preview(preview: List[Dict]) -> str:
    """Format table preview as ASCII table"""

    if not preview:
        return "No data"

    # Get column names
    columns = list(preview[0].keys())

    # Calculate column widths
    widths = {}
    for col in columns:
        widths[col] = max(
            len(str(col)),
            max(len(str(row.get(col, ""))) for row in preview)
        )

    # Build header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)

    # Build rows
    rows = []
    for row in preview[:10]:  # Limit to 10 rows
        row_str = " | ".join(
            str(row.get(col, "")).ljust(widths[col])
            for col in columns
        )
        rows.append(row_str)

    # Combine
    table = "\n".join([header, separator] + rows)

    # Truncate if too long
    if len(table) > 3000:
        table = table[:2997] + "..."

    return table