from typing import Dict, List, Any, Optional
import json
from datetime import datetime
import pandas as pd
from io import StringIO


class ReportGenerator:
    """Generate HTML reports for query results"""

    @staticmethod
    def generate_html_report(
            query: str,
            sql: str,
            results: List[Dict],
            execution_time_ms: float,
            total_rows: int,
            bytes_processed: Optional[int] = None,
            cache_hit: bool = False,
            error: Optional[str] = None
    ) -> str:
        """Generate HTML report with results"""

        # Convert results to DataFrame for better formatting
        df = pd.DataFrame(results) if results else pd.DataFrame()

        # HTML template
        html_template = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Query Results</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 24px;
        }}
        .header .timestamp {{
            opacity: 0.9;
            font-size: 14px;
        }}
        .metrics {{
            display: flex;
            gap: 30px;
            padding: 20px 30px;
            background: #fafafa;
            border-bottom: 1px solid #e0e0e0;
        }}
        .metric {{
            display: flex;
            flex-direction: column;
        }}
        .metric-label {{
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
            margin-bottom: 4px;
        }}
        .metric-value {{
            font-size: 20px;
            font-weight: 600;
            color: #333;
        }}
        .metric-value.success {{
            color: #10b981;
        }}
        .metric-value.cached {{
            color: #3b82f6;
        }}
        .section {{
            padding: 30px;
        }}
        .section-title {{
            font-size: 16px;
            font-weight: 600;
            color: #333;
            margin-bottom: 15px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .query-box {{
            background: #f8f8f8;
            border: 1px solid #e0e0e0;
            border-radius: 6px;
            padding: 15px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 13px;
            color: #333;
            margin-bottom: 20px;
        }}
        .sql-box {{
            background: #1e1e1e;
            color: #d4d4d4;
            border-radius: 6px;
            padding: 20px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 13px;
            line-height: 1.5;
            overflow-x: auto;
            margin-bottom: 20px;
            white-space: pre-wrap;
        }}
        .results-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        .results-table th {{
            background: #f3f4f6;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #374151;
            border-bottom: 2px solid #e5e7eb;
        }}
        .results-table td {{
            padding: 12px;
            border-bottom: 1px solid #e5e7eb;
        }}
        .results-table tr:hover {{
            background: #f9fafb;
        }}
        .results-table tr:last-child td {{
            border-bottom: none;
        }}
        .no-results {{
            text-align: center;
            padding: 60px 20px;
            color: #9ca3af;
        }}
        .error-box {{
            background: #fef2f2;
            border: 1px solid #fecaca;
            border-radius: 6px;
            padding: 15px;
            color: #dc2626;
            margin-bottom: 20px;
        }}
        .export-buttons {{
            margin-bottom: 20px;
            display: flex;
            gap: 10px;
        }}
        .export-btn {{
            padding: 8px 16px;
            background: #4f46e5;
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            transition: background 0.2s;
        }}
        .export-btn:hover {{
            background: #4338ca;
        }}
        .footer {{
            padding: 20px 30px;
            background: #f9fafb;
            border-top: 1px solid #e5e7eb;
            text-align: center;
            font-size: 12px;
            color: #6b7280;
        }}
    </style>
    <script>
        function exportToCSV() {{
            const table = document.getElementById('results-table');
            if (!table) {{
                alert('No results to export');
                return;
            }}

            let csv = [];
            const rows = table.querySelectorAll('tr');

            for (let row of rows) {{
                const cols = row.querySelectorAll('td, th');
                const csvRow = [];
                for (let col of cols) {{
                    let data = col.innerText.replace(/"/g, '""');
                    csvRow.push('"' + data + '"');
                }}
                csv.push(csvRow.join(','));
            }}

            const csvContent = csv.join('\\n');
            const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = 'query_results.csv';
            link.click();
        }}

        function exportToJSON() {{
            const table = document.getElementById('results-table');
            if (!table) {{
                alert('No results to export');
                return;
            }}

            const headers = Array.from(table.querySelectorAll('th')).map(th => th.innerText);
            const rows = Array.from(table.querySelectorAll('tbody tr'));

            const data = rows.map(row => {{
                const cells = Array.from(row.querySelectorAll('td'));
                const obj = {{}};
                headers.forEach((header, i) => {{
                    obj[header] = cells[i] ? cells[i].innerText : '';
                }});
                return obj;
            }});

            const jsonContent = JSON.stringify(data, null, 2);
            const blob = new Blob([jsonContent], {{ type: 'application/json' }});
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = 'query_results.json';
            link.click();
        }}
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Query Execution Report</h1>
            <div class="timestamp">{timestamp}</div>
        </div>

        <div class="metrics">
            <div class="metric">
                <div class="metric-label">Execution Time</div>
                <div class="metric-value">{execution_time}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Total Rows</div>
                <div class="metric-value success">{total_rows}</div>
            </div>
            {bytes_metric}
            <div class="metric">
                <div class="metric-label">Cache Status</div>
                <div class="metric-value {cache_class}">{cache_status}</div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Original Query</div>
            <div class="query-box">{query}</div>
        </div>

        <div class="section">
            <div class="section-title">Generated SQL</div>
            <div class="sql-box">{sql}</div>
        </div>

        <div class="section">
            <div class="section-title">Results</div>
            {error_section}
            <div class="export-buttons">
                <button class="export-btn" onclick="exportToCSV()">Export as CSV</button>
                <button class="export-btn" onclick="exportToJSON()">Export as JSON</button>
            </div>
            {results_section}
        </div>

        <div class="footer">
            Generated by AI Analytics Assistant
        </div>
    </div>
</body>
</html>"""

        # Format execution time
        if execution_time_ms < 1000:
            exec_time_str = f"{execution_time_ms:.0f}ms"
        else:
            exec_time_str = f"{execution_time_ms / 1000:.2f}s"

        # Format bytes processed
        bytes_metric = ""
        if bytes_processed:
            if bytes_processed < 1024 ** 2:
                bytes_str = f"{bytes_processed / 1024:.1f} KB"
            elif bytes_processed < 1024 ** 3:
                bytes_str = f"{bytes_processed / (1024 ** 2):.1f} MB"
            else:
                bytes_str = f"{bytes_processed / (1024 ** 3):.2f} GB"
            bytes_metric = f"""
            <div class="metric">
                <div class="metric-label">Data Processed</div>
                <div class="metric-value">{bytes_str}</div>
            </div>
            """

        # Format results table
        results_section = ""
        error_section = ""

        if error:
            error_section = f'<div class="error-box">Error: {error}</div>'
            results_section = '<div class="no-results">Query execution failed</div>'
        elif results and len(results) > 0:
            # Convert to HTML table with ID for export
            html_table = df.to_html(
                index=False,
                classes='results-table',
                table_id='results-table',
                escape=True
            )
            results_section = html_table
        else:
            results_section = '<div class="no-results">No results returned</div>'

        # Escape special characters in query and SQL
        query_escaped = query.replace('<', '&lt;').replace('>', '&gt;')
        sql_escaped = sql.replace('<', '&lt;').replace('>', '&gt;')

        # Fill template - using double curly braces for CSS and single for Python format
        html = html_template.format(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            query=query_escaped,
            sql=sql_escaped,
            execution_time=exec_time_str,
            total_rows=f"{total_rows:,}",
            bytes_metric=bytes_metric,
            cache_status="HIT" if cache_hit else "MISS",
            cache_class="cached" if cache_hit else "",
            error_section=error_section,
            results_section=results_section
        )

        return html

    @staticmethod
    def generate_markdown_report(
            query: str,
            sql: str,
            results: List[Dict],
            execution_time_ms: float,
            total_rows: int,
            bytes_processed: Optional[int] = None,
            cache_hit: bool = False,
            error: Optional[str] = None
    ) -> str:
        """Generate Markdown report with results"""

        df = pd.DataFrame(results) if results else pd.DataFrame()

        report = f"""# Query Execution Report

## Query
```
{query}
```

## Generated SQL
```sql
{sql}
```

## Execution Metrics
- **Execution Time**: {execution_time_ms:.0f}ms
- **Total Rows**: {total_rows:,}
- **Cache Hit**: {'Yes' if cache_hit else 'No'}
"""

        if bytes_processed:
            gb = bytes_processed / (1024 ** 3)
            report += f"- **Data Processed**: {gb:.2f} GB\n"

        if error:
            report += f"\n## Error\n```\n{error}\n```\n"
        elif results and len(results) > 0:
            # Limit table to 20 rows for markdown
            df_limited = df.head(20)
            report += f"\n## Results (showing first {len(df_limited)} rows)\n"
            report += df_limited.to_markdown(index=False)
            if len(df) > 20:
                report += f"\n\n*... and {len(df) - 20} more rows*"
        else:
            report += "\n## Results\nNo results returned\n"

        report += f"\n\n---\n*Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"

        return report

    @staticmethod
    def generate_simple_text_report(
            query: str,
            sql: str,
            results: List[Dict],
            execution_time_ms: float,
            total_rows: int,
            error: Optional[str] = None
    ) -> str:
        """Generate simple text report for Slack"""

        if error:
            return f"""❌ Query Failed
Query: {query}
Error: {error}"""

        if not results:
            return f"""✅ Query Executed Successfully
Query: {query}
Result: No rows returned
Execution time: {execution_time_ms:.0f}ms"""

        # Format first 10 rows for Slack
        df = pd.DataFrame(results[:10])
        table_str = df.to_string(index=False, max_colwidth=20)

        report = f"""✅ Query Executed Successfully
Query: {query}

Results ({len(results)} of {total_rows} rows):
```
{table_str}
```
Execution time: {execution_time_ms:.0f}ms"""

        if total_rows > len(results):
            report += f"\n\n*Showing first {len(results)} rows of {total_rows} total*"

        return report