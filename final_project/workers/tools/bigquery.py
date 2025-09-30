"""
BigQuery Tool for executing queries with safety guardrails
"""

import hashlib
import re
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import structlog
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from workers.config import settings

logger = structlog.get_logger()


class BigQueryTool:
    """BigQuery client with safety features"""

    def __init__(self):
        """Initialize BigQuery client with proper authentication"""
        import os
        from google.oauth2 import service_account

        # Check if credentials are set
        creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        credentials = None

        if creds_path:
            if not os.path.exists(creds_path):
                logger.error("Service account file not found", path=creds_path)
                raise FileNotFoundError(f"Service account file not found: {creds_path}")

            logger.info("Loading service account credentials", path=creds_path)
            try:
                # Explicitly load credentials
                credentials = service_account.Credentials.from_service_account_file(
                    creds_path,
                    scopes=["https://www.googleapis.com/auth/bigquery"]
                )
                logger.info("Credentials loaded successfully")
            except Exception as e:
                logger.error("Failed to load credentials", error=str(e))
                raise
        else:
            logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set, trying default credentials")

        # Initialize client with source project for billing
        try:
            if credentials:
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=settings.bq_source_project,
                    location=settings.bq_location
                )
            else:
                self.client = bigquery.Client(
                    project=settings.bq_source_project,
                    location=settings.bq_location
                )

            logger.info("BigQuery client initialized",
                       project=settings.bq_source_project,
                       location=settings.bq_location,
                       has_credentials=credentials is not None)
        except Exception as e:
            logger.error("Failed to initialize BigQuery client",
                        error=str(e),
                        project=settings.bq_source_project)
            raise

        self.results_project = settings.bq_results_project
        self.results_dataset = settings.bq_results_dataset
        self.max_bytes_billed = settings.bq_max_bytes_billed

    async def dry_run(self, sql: str) -> Dict[str, Any]:
        """
        Perform dry run to estimate query cost

        Returns:
            Dict with bytes_billed, bytes_processed, cache_hit, referenced_tables
        """
        logger.info("performing_dry_run", sql_length=len(sql))

        try:
            # Validate SQL first
            validation_errors = self._validate_sql(sql)
            if validation_errors:
                raise ValueError(f"SQL validation failed: {', '.join(validation_errors)}")

            # Configure dry run
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=True,
                labels={"component": "ai_assistant", "type": "dry_run"}
            )

            # Run dry run
            query_job = self.client.query(sql, job_config=job_config)

            # Extract dry run results
            result = {
                "bytes_billed": query_job.total_bytes_billed or 0,
                "bytes_processed": query_job.total_bytes_processed or 0,
                "cache_hit": query_job.cache_hit if hasattr(query_job, 'cache_hit') else False,
                "referenced_tables": self._extract_referenced_tables(query_job),
                "estimated_cost_usd": self._calculate_cost(query_job.total_bytes_billed or 0)
            }

            logger.info("dry_run_complete",
                       bytes_gb=result["bytes_billed"] / (1024**3),
                       cost_usd=result["estimated_cost_usd"],
                       cache_hit=result["cache_hit"])

            return result

        except Exception as e:
            logger.error("dry_run_failed", error=str(e))
            raise

    async def execute(self, sql: str, task_id: str, max_results: int = 1000) -> Dict[str, Any]:
        """
        Execute SQL query with all safety checks

        Returns:
            Dict with results, metadata, and destination table
        """
        logger.info("executing_query", task_id=task_id, sql_preview=sql)

        try:
            # First do a dry run
            dry_run_result = await self.dry_run(sql)

            # Check if query exceeds limits
            if dry_run_result["bytes_billed"] > self.max_bytes_billed:
                raise ValueError(
                    f"Query exceeds maximum bytes limit: "
                    f"{dry_run_result['bytes_billed'] / (1024**3):.2f}GB > "
                    f"{self.max_bytes_billed / (1024**3):.2f}GB"
                )

            # Generate destination table name
            destination_table = self._generate_destination_table(task_id)

            # Configure actual query
            job_config = bigquery.QueryJobConfig(
                destination=destination_table,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                use_query_cache=True,
                maximum_bytes_billed=self.max_bytes_billed,
                labels={
                    "component": "ai_assistant",
                    "task_id": task_id[:63],  # Label values must be ≤63 chars
                    "type": "execute"
                }
            )

            # Execute query
            query_job = self.client.query(sql, job_config=job_config)

            # Wait for completion
            query_job.result()  # This waits for the job to complete

            # Now get the actual row count from the destination table
            total_rows = 0
            preview_rows = []
            full_results = []

            try:
                if query_job.destination:
                    # Get the destination table to check row count
                    destination_table_ref = query_job.destination
                    table = self.client.get_table(destination_table_ref)
                    total_rows = table.num_rows

                    # Fetch preview rows from the destination table
                    query_preview = f"SELECT * FROM `{destination_table_ref.project}.{destination_table_ref.dataset_id}.{destination_table_ref.table_id}` LIMIT 100"
                    preview_job = self.client.query(query_preview)
                    preview_results = preview_job.result()

                    for row in preview_results:
                        preview_rows.append(dict(row))

                    if total_rows > 100:
                        query_full = f"SELECT * FROM `{destination_table_ref.project}.{destination_table_ref.dataset_id}.{destination_table_ref.table_id}` LIMIT {max_results}"
                        full_job = self.client.query(query_full)
                        full_results_iter = full_job.result()

                        for row in full_results_iter:
                            full_results.append(dict(row))
                    else:
                        full_results = preview_rows

                        logger.debug("fetched_full_results",
                                     task_id=task_id,
                                     preview_count=len(preview_rows),
                                     full_count=len(full_results),
                                     total_rows=total_rows)

            except Exception as e:
                logger.warning("failed_to_get_preview_or_count",
                              error=str(e),
                              task_id=task_id)
                # Continue without preview - don't fail the whole query

            # Calculate execution time safely
            execution_time = 0
            try:
                if query_job.ended and query_job.started:
                    execution_time = (query_job.ended - query_job.started).total_seconds()
            except:
                pass

            # Build result - ensure all values are serializable
            result = {
                "sql": sql,
                "rows_count": int(total_rows) if total_rows else 0,
                "bytes_processed": int(query_job.total_bytes_processed or 0),
                "bytes_billed": int(query_job.total_bytes_billed or 0),
                "cache_hit": bool(query_job.cache_hit) if hasattr(query_job, 'cache_hit') else False,
                "execution_time": float(execution_time),
                "preview": preview_rows[:10],  # Limit preview
                "results": full_results,
                "table_name": f"{destination_table_ref.project}.{destination_table_ref.dataset_id}.{destination_table_ref.table_id}" if query_job.destination else None,
                "table_link": self._generate_bigquery_link(destination_table_ref) if query_job.destination else None,
                "cost_usd": float(((query_job.total_bytes_billed or 0) / (1024**4)) * 5.0)
            }

            logger.info("query_executed",
                       task_id=task_id,
                       rows=result["rows_count"],
                       bytes_gb=result["bytes_billed"] / (1024**3),
                       cost_usd=result["cost_usd"],
                       cache_hit=result["cache_hit"])

            # Set table expiration (7 days)
            try:
                if query_job.destination:
                    self._set_table_expiration(destination_table_ref, days=7)
            except Exception as e:
                logger.warning("failed_to_set_expiration",
                              error=str(e),
                              task_id=task_id)

            return result

        except GoogleCloudError as e:
            logger.error("bigquery_error",
                        error=str(e),
                        error_type=type(e).__name__,
                        task_id=task_id,
                        sql_snippet=sql[:500] if sql else None)
            raise
        except Exception as e:
            logger.error("execution_failed",
                        error=str(e),
                        error_type=type(e).__name__,
                        task_id=task_id,
                        traceback=traceback.format_exc())
            raise

    async def get_available_tables(self) -> List[Dict[str, Any]]:
        """Get list of available tables with basic schema"""

        try:
            tables = []

            # Try to get tables from configured datasets
            # You can configure specific datasets in settings or environment
            datasets_to_check = []

            # If we have specific datasets configured, use them
            # Otherwise, try to list datasets (may also fail with permissions)
            if hasattr(settings, 'bq_allowed_datasets') and settings.bq_allowed_datasets:
                datasets_to_check = settings.bq_allowed_datasets.split(',')
            else:
                # Try to list datasets, but handle permission errors
                try:
                    datasets = list(self.client.list_datasets(max_results=5))
                    datasets_to_check = [d.dataset_id for d in datasets]
                except Exception as e:
                    logger.warning("cannot_list_datasets", error=str(e))
                    # Return empty list if we can't list datasets
                    return []

            # Try to get tables from each dataset
            for dataset_id in datasets_to_check:
                try:
                    dataset_ref = self.client.dataset(dataset_id)
                    table_list = list(self.client.list_tables(dataset_ref, max_results=5))

                    for table_item in table_list:
                        try:
                            table_ref = dataset_ref.table(table_item.table_id)
                            table = self.client.get_table(table_ref)

                            tables.append({
                                "full_name": f"{self.client.project}.{dataset_id}.{table_item.table_id}",
                                "description": table.description or "",
                                "columns": [
                                    {
                                        "name": field.name,
                                        "type": field.field_type,
                                        "description": field.description or ""
                                    }
                                    for field in table.schema[:10]  # Limit to first 10 columns
                                ]
                            })

                            if len(tables) >= 10:  # Limit total tables
                                break

                        except Exception as e:
                            logger.debug("cannot_get_table_details",
                                       table=f"{dataset_id}.{table_item.table_id}",
                                       error=str(e))
                            continue

                except Exception as e:
                    logger.debug("cannot_list_tables_in_dataset",
                               dataset=dataset_id,
                               error=str(e))
                    continue

            return tables

        except Exception as e:
            logger.warning("failed_to_get_tables", error=str(e))
            # Return empty list instead of failing
            return []

    def _validate_sql(self, sql: str) -> List[str]:
        """Validate SQL query for safety"""
        errors = []

        sql_upper = sql.upper()

        # Check for forbidden operations
        forbidden_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'TRUNCATE', 'MERGE', 'GRANT', 'REVOKE'
        ]

        for keyword in forbidden_keywords:
            if re.search(r'\b' + keyword + r'\b', sql_upper):
                errors.append(f"Forbidden operation: {keyword}")

        # Check for SELECT * - but allow it for INFORMATION_SCHEMA queries
        if 'INFORMATION_SCHEMA' not in sql_upper:
            if re.search(r'\bSELECT\s+\*', sql_upper):
                errors.append("SELECT * is not allowed. Please specify columns explicitly.")

        # Check for missing partition filter only for known partitioned tables
        # This is a more sophisticated check that avoids false positives

        # List of tables that we KNOW require partition filters
        # These should be configured based on your actual partitioned tables
        partitioned_tables = {
            'events': ['_PARTITIONDATE', 'event_date', 'date'],
            'transactions': ['_PARTITIONDATE', 'transaction_date', 'date'],
            'blocks': ['_PARTITIONDATE', 'block_date', 'date'],
            'operations': ['_PARTITIONDATE', 'operation_date', 'date']
        }

        # Only check for partition filters if we're actually querying these tables
        # and NOT querying INFORMATION_SCHEMA
        if 'INFORMATION_SCHEMA' not in sql_upper:
            for table_name, partition_columns in partitioned_tables.items():
                # Check if this table is referenced in the query
                table_pattern = rf'\b{table_name}\b'
                if re.search(table_pattern, sql.lower()):
                    # Check if it's the main table being queried (not a subquery or CTE)
                    # This is a simple check - might need refinement for complex queries
                    if f'FROM ' in sql_upper and table_name.upper() in sql_upper.split('FROM')[1].split('WHERE')[0]:
                        # Check if any partition column is used in WHERE clause
                        has_partition_filter = False
                        for col in partition_columns:
                            if col.upper() in sql_upper:
                                has_partition_filter = True
                                break

                        if not has_partition_filter:
                            # Only add error if we're sure it's needed
                            # Skip if it's a metadata query or aggregate without scanning data
                            if 'COUNT(*)' not in sql_upper or 'LIMIT' not in sql_upper:
                                logger.warning(f"Table '{table_name}' might need a partition filter",
                                             table=table_name,
                                             sql_snippet=sql[:200])
                                # Don't block the query, just warn
                                # errors.append(f"Table '{table_name}' requires a partition filter")

        return errors

    def _extract_referenced_tables(self, query_job) -> List[str]:
        """Extract table references from query job"""
        tables = []

        if hasattr(query_job, 'referenced_tables'):
            for table_ref in query_job.referenced_tables:
                tables.append(f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}")

        return tables

    def _generate_destination_table(self, task_id: str) -> bigquery.TableReference:
        """Generate unique destination table name"""

        # Create table name with task_id hash (to keep it short)
        table_hash = hashlib.md5(task_id.encode()).hexdigest()[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = f"query_{timestamp}_{table_hash}"

        # Ensure results dataset exists
        self._ensure_dataset_exists()

        # Return table reference
        dataset_ref = self.client.dataset(self.results_dataset, project=self.results_project)
        return dataset_ref.table(table_name)

    def _ensure_dataset_exists(self):
        """Ensure results dataset exists"""
        dataset_id = f"{self.results_project}.{self.results_dataset}"

        try:
            self.client.get_dataset(dataset_id)
        except:
            # Create dataset if it doesn't exist
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.client.location
            dataset.description = "Temporary tables for AI Analytics Assistant"
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info("created_results_dataset", dataset=dataset_id)

    def _generate_bigquery_link(self, table_ref: bigquery.TableReference) -> str:
        """Generate BigQuery console link"""
        base_url = "https://console.cloud.google.com/bigquery"
        params = f"?project={table_ref.project}&ws=!1m5!1m4!4m3!1s{table_ref.project}!2s{table_ref.dataset_id}!3s{table_ref.table_id}"
        return base_url + params

    def _set_table_expiration(self, table_ref: bigquery.TableReference, days: int = 7):
        """Set expiration time for table"""
        try:
            table = self.client.get_table(table_ref)
            table.expires = datetime.now() + timedelta(days=days)
            self.client.update_table(table, ["expires"])
            logger.info("set_table_expiration",
                       table=f"{table_ref.dataset_id}.{table_ref.table_id}",
                       days=days)
        except Exception as e:
            logger.error("failed_to_set_expiration", error=str(e))

    def _calculate_cost(self, bytes_billed: int) -> float:
        """Calculate estimated cost in USD"""
        # BigQuery pricing: $5 per TB (may vary by region)
        cost_per_tb = 5.0
        tb = bytes_billed / (1024 ** 4)
        return round(tb * cost_per_tb, 4)

    async def get_schema_context(self, query: str, tables: List[Dict]) -> List[Dict]:
        """
        Get schema context for relevant tables based on the query

        Args:
            query: User's question
            tables: List of available tables

        Returns:
            List of table schemas relevant to the query
        """
        logger.info("getting_schema_context", query_length=len(query), tables_count=len(tables))

        # For now, return all tables as context
        # In future, can add smart filtering based on query keywords
        schema_context = []

        for table in tables[:10]:  # Limit to 10 tables to avoid context overflow
            try:
                # Try to get more detailed schema if possible
                if "full_name" in table:
                    parts = table["full_name"].split(".")
                    if len(parts) == 3:
                        project, dataset, table_name = parts

                        # Try to get column information
                        try:
                            table_ref = self.client.get_table(f"{project}.{dataset}.{table_name}")

                            columns = []
                            for field in table_ref.schema[:20]:  # Limit columns
                                columns.append({
                                    "name": field.name,
                                    "type": field.field_type,
                                    "mode": field.mode,
                                    "description": field.description or ""
                                })

                            schema_context.append({
                                "full_name": table["full_name"],
                                "dataset": dataset,
                                "table": table_name,
                                "columns": columns,
                                "description": table.get("description", ""),
                                "row_count": table.get("row_count", 0)
                            })
                        except Exception as e:
                            logger.warning("failed_to_get_detailed_schema",
                                           table=table["full_name"],
                                           error=str(e))
                            # Use basic info if detailed fetch fails
                            schema_context.append(table)
                else:
                    schema_context.append(table)

            except Exception as e:
                logger.warning("schema_context_error", error=str(e))
                continue

        logger.info("schema_context_ready", context_count=len(schema_context))
        return schema_context