#!/usr/bin/env python3
"""
Enhanced Document Indexer with Multi-Collection Support - FIXED VERSION
Location: indexer/main.py
"""

import asyncio
import json
import yaml
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

import structlog
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from openai import AsyncOpenAI

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class CollectionType(str, Enum):
    """Types of collections for different content"""
    SQL_EXAMPLES = "sql_examples"
    DBT_MODELS = "dbt_models"
    TABLE_SCHEMAS = "table_schemas"
    BUSINESS_KNOWLEDGE = "business_knowledge"


class MultiCollectionIndexer:
    """Enhanced indexer with support for multiple collections"""

    # Collection configurations - ИСПРАВЛЕНО: используем .value для ключей
    COLLECTIONS = {
        CollectionType.SQL_EXAMPLES.value: {
            "size": 1536,
            "distance": Distance.COSINE,
            "description": "SQL query examples with questions and solutions"
        },
        CollectionType.DBT_MODELS.value: {
            "size": 1536,
            "distance": Distance.COSINE,
            "description": "dbt model definitions and documentation"
        },
        CollectionType.TABLE_SCHEMAS.value: {
            "size": 1536,
            "distance": Distance.COSINE,
            "description": "BigQuery table schemas and metadata"
        },
        CollectionType.BUSINESS_KNOWLEDGE.value: {
            "size": 1536,
            "distance": Distance.COSINE,
            "description": "Business documentation and knowledge base"
        }
    }

    def __init__(self,
                 qdrant_url: str = "http://localhost:6333",
                 openai_api_key: str = None,
                 recreate_collections: bool = False):
        """
        Initialize multi-collection indexer

        Args:
            qdrant_url: Qdrant server URL
            openai_api_key: OpenAI API key for embeddings
            recreate_collections: Whether to recreate existing collections
        """
        self.qdrant = QdrantClient(url=qdrant_url)
        self.openai = AsyncOpenAI(api_key=openai_api_key)
        self.recreate = recreate_collections

        # Ensure all collections exist
        self._ensure_collections()

        logger.info("multi_collection_indexer_initialized",
                    qdrant_url=qdrant_url,
                    collections=list(self.COLLECTIONS.keys()))

    def _ensure_collections(self):
        """Create all collections if they don't exist"""
        try:
            existing_collections = {
                c.name for c in self.qdrant.get_collections().collections
            }

            for collection_name, config in self.COLLECTIONS.items():
                # collection_name уже является строкой ("sql_examples", etc.)
                if self.recreate and collection_name in existing_collections:
                    # Delete and recreate
                    self.qdrant.delete_collection(collection_name)
                    logger.info("collection_deleted", name=collection_name)
                    existing_collections.discard(collection_name)

                if collection_name not in existing_collections:
                    self.qdrant.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(
                            size=config["size"],
                            distance=config["distance"]
                        )
                    )
                    logger.info("collection_created",
                                name=collection_name,
                                config=config)
                else:
                    logger.info("collection_exists", name=collection_name)

        except Exception as e:
            logger.error("collection_creation_failed", error=str(e))
            raise

    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for text using OpenAI"""
        try:
            response = await self.openai.embeddings.create(
                model="text-embedding-3-small",
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error("embedding_failed", error=str(e), text_preview=text[:100])
            return None

    async def index_sql_examples(self, examples_path: Path) -> int:
        """Index SQL examples into dedicated collection"""

        logger.info("indexing_sql_examples", path=str(examples_path))
        collection = CollectionType.SQL_EXAMPLES.value  # ИСПРАВЛЕНО: используем .value

        try:
            # Load examples
            with open(examples_path) as f:
                if examples_path.suffix == '.json':
                    data = json.load(f)
                else:
                    data = yaml.safe_load(f)

            examples = data.get('examples', [])
            points = []

            for i, example in enumerate(examples):
                # Create searchable text
                text = f"""
                Question: {example['question']}
                SQL Query: {example['sql']}
                Tables Used: {', '.join(example.get('tables', []))}
                Context: {example.get('context', '')}
                Difficulty: {example.get('difficulty', 'medium')}
                """

                # Get embedding
                embedding = await self.get_embedding(text)
                if not embedding:
                    continue

                # Create unique ID
                point_id = hashlib.md5(
                    f"{collection}:{example['question']}".encode()
                ).hexdigest()

                points.append(PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "type": "sql_example",
                        "question": example['question'],
                        "sql": example['sql'],
                        "tables": example.get('tables', []),
                        "context": example.get('context', ''),
                        "difficulty": example.get('difficulty', 'medium'),
                        "tags": example.get('tags', []),
                        "indexed_at": datetime.utcnow().isoformat(),
                        "source_file": str(examples_path.name)
                    }
                ))

            # Batch upsert
            if points:
                self.qdrant.upsert(
                    collection_name=collection,
                    points=points
                )
                logger.info("sql_examples_indexed",
                            collection=collection,
                            count=len(points))

            return len(points)

        except Exception as e:
            logger.error("sql_indexing_failed",
                         collection=collection,
                         error=str(e))
            return 0

    async def index_dbt_models(self, dbt_project_path: Path) -> int:
        """Index dbt models into dedicated collection"""

        logger.info("indexing_dbt_models", path=str(dbt_project_path))
        collection = CollectionType.DBT_MODELS.value  # ИСПРАВЛЕНО: используем .value

        try:
            models_path = dbt_project_path / "models"
            points = []

            # Process all SQL files
            for sql_file in models_path.rglob("*.sql"):
                # Read SQL content
                with open(sql_file) as f:
                    sql_content = f.read()

                # Look for schema.yml
                schema_file = sql_file.parent / "schema.yml"
                model_docs = {}

                if schema_file.exists():
                    with open(schema_file) as f:
                        schema = yaml.safe_load(f) or {}

                    # Find documentation for this model
                    for model in schema.get('models', []):
                        if model['name'] == sql_file.stem:
                            model_docs = model
                            break

                # Create searchable text
                text = f"""
                dbt Model: {sql_file.stem}
                Path: {sql_file.relative_to(dbt_project_path)}
                Description: {model_docs.get('description', 'No description')}
                Tags: {', '.join(model_docs.get('tags', []))}

                Columns:
                {self._format_columns(model_docs.get('columns', []))}

                SQL Preview:
                {sql_content[:1000]}
                """

                # Get embedding
                embedding = await self.get_embedding(text)
                if not embedding:
                    continue

                # Create unique ID
                point_id = hashlib.md5(
                    f"{collection}:{sql_file.stem}".encode()
                ).hexdigest()

                points.append(PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "type": "dbt_model",
                        "model_name": sql_file.stem,
                        "path": str(sql_file.relative_to(dbt_project_path)),
                        "description": model_docs.get('description', ''),
                        "sql_preview": sql_content[:500],
                        "columns": model_docs.get('columns', []),
                        "tags": model_docs.get('tags', []),
                        "tests": model_docs.get('tests', []),
                        "indexed_at": datetime.utcnow().isoformat()
                    }
                ))

            # Batch upsert
            if points:
                self.qdrant.upsert(
                    collection_name=collection,
                    points=points
                )
                logger.info("dbt_models_indexed",
                            collection=collection,
                            count=len(points))

            return len(points)

        except Exception as e:
            logger.error("dbt_indexing_failed",
                         collection=collection,
                         error=str(e))
            return 0

    async def index_table_schemas(self, schemas_path: Path) -> int:
        """Index BigQuery table schemas into dedicated collection"""

        logger.info("indexing_schemas", path=str(schemas_path))
        collection = CollectionType.TABLE_SCHEMAS.value  # ИСПРАВЛЕНО: используем .value

        try:
            # Load schemas
            with open(schemas_path) as f:
                if schemas_path.suffix == '.json':
                    schemas = json.load(f)
                else:
                    schemas = yaml.safe_load(f)

            points = []

            for table in schemas.get('tables', []):
                # Create searchable text
                text = f"""
                Table: {table['name']}
                Dataset: {table.get('dataset', '')}
                Project: {table.get('project', '')}
                Description: {table.get('description', '')}

                Columns:
                {self._format_columns(table.get('columns', []))}

                Partitioning: {table.get('partition_field', 'None')}
                Clustering: {', '.join(table.get('clustering_fields', []))}
                Row Count: {table.get('row_count', 'Unknown')}
                Size: {table.get('size_gb', 'Unknown')} GB
                """

                # Get embedding
                embedding = await self.get_embedding(text)
                if not embedding:
                    continue

                # Create unique ID
                full_table_name = f"{table.get('project', '')}.{table.get('dataset', '')}.{table['name']}"
                point_id = hashlib.md5(
                    f"{collection}:{full_table_name}".encode()
                ).hexdigest()

                points.append(PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "type": "table_schema",
                        "table_name": table['name'],
                        "dataset": table.get('dataset', ''),
                        "project": table.get('project', ''),
                        "full_name": full_table_name,
                        "description": table.get('description', ''),
                        "columns": table.get('columns', []),
                        "partition_field": table.get('partition_field'),
                        "clustering_fields": table.get('clustering_fields', []),
                        "row_count": table.get('row_count'),
                        "size_gb": table.get('size_gb'),
                        "labels": table.get('labels', {}),
                        "indexed_at": datetime.utcnow().isoformat()
                    }
                ))

            # Batch upsert
            if points:
                self.qdrant.upsert(
                    collection_name=collection,
                    points=points
                )
                logger.info("schemas_indexed",
                            collection=collection,
                            count=len(points))

            return len(points)

        except Exception as e:
            logger.error("schema_indexing_failed",
                         collection=collection,
                         error=str(e))
            return 0

    async def index_business_knowledge(self, knowledge_path: Path) -> int:
        """Index business documentation into dedicated collection"""

        logger.info("indexing_knowledge", path=str(knowledge_path))
        collection = CollectionType.BUSINESS_KNOWLEDGE.value  # ИСПРАВЛЕНО: используем .value

        try:
            points = []

            # Process all markdown and text files
            for pattern in ['*.md', '*.txt', '*.rst']:
                for doc_file in knowledge_path.rglob(pattern):
                    with open(doc_file, encoding='utf-8') as f:
                        content = f.read()

                    # Skip empty or very short files
                    if len(content) < 100:
                        continue

                    # Split into chunks (simple paragraph-based)
                    chunks = self._split_into_chunks(content, chunk_size=1000)

                    for i, chunk in enumerate(chunks):
                        # Create searchable text with context
                        text = f"""
                        Document: {doc_file.stem}
                        Section: Part {i + 1}/{len(chunks)}
                        Path: {doc_file.relative_to(knowledge_path)}

                        Content:
                        {chunk}
                        """

                        # Get embedding
                        embedding = await self.get_embedding(text)
                        if not embedding:
                            continue

                        # Create unique ID
                        chunk_id = f"{doc_file.stem}_chunk_{i}"
                        point_id = hashlib.md5(
                            f"{collection}:{chunk_id}".encode()
                        ).hexdigest()

                        points.append(PointStruct(
                            id=point_id,
                            vector=embedding,
                            payload={
                                "type": "knowledge",
                                "document_name": doc_file.stem,
                                "chunk_index": i,
                                "total_chunks": len(chunks),
                                "content": chunk,
                                "source_path": str(doc_file.relative_to(knowledge_path)),
                                "file_type": doc_file.suffix,
                                "indexed_at": datetime.utcnow().isoformat()
                            }
                        ))

            # Batch upsert
            if points:
                # Upload in batches to avoid memory issues
                batch_size = 100
                for i in range(0, len(points), batch_size):
                    batch = points[i:i + batch_size]
                    self.qdrant.upsert(
                        collection_name=collection,
                        points=batch
                    )

                logger.info("knowledge_indexed",
                            collection=collection,
                            count=len(points))

            return len(points)

        except Exception as e:
            logger.error("knowledge_indexing_failed",
                         collection=collection,
                         error=str(e))
            return 0

    def _format_columns(self, columns: List[Dict]) -> str:
        """Format column information for embedding"""
        if not columns:
            return "No columns documented"

        formatted = []
        for col in columns:
            col_str = f"- {col.get('name', 'unknown')} ({col.get('type', 'unknown')})"
            if col.get('description'):
                col_str += f": {col['description']}"
            formatted.append(col_str)

        return "\n".join(formatted)

    def _split_into_chunks(self, text: str, chunk_size: int = 1000) -> List[str]:
        """Split text into chunks with overlap"""
        chunks = []
        paragraphs = text.split('\n\n')

        current_chunk = ""
        for paragraph in paragraphs:
            # If adding this paragraph exceeds chunk size, save current chunk
            if len(current_chunk) + len(paragraph) > chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                # Keep last 100 chars for context overlap
                current_chunk = current_chunk[-100:] + "\n\n" + paragraph
            else:
                current_chunk += "\n\n" + paragraph if current_chunk else paragraph

        # Add remaining chunk
        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks

    async def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics for all collections"""
        stats = {}

        # ИСПРАВЛЕНО: используем значения из COLLECTIONS (уже строки)
        for collection_name in self.COLLECTIONS.keys():
            try:
                collection_info = self.qdrant.get_collection(collection_name)
                stats[collection_name] = {
                    "points_count": collection_info.points_count,
                    "indexed_count": collection_info.indexed_vectors_count,
                    "status": collection_info.status,
                    "config": self.COLLECTIONS[collection_name]
                }
            except Exception as e:
                stats[collection_name] = {
                    "error": str(e)
                }

        return stats


async def main():
    """Main indexing pipeline"""

    import os
    from dotenv import load_dotenv

    load_dotenv()

    # Initialize multi-collection indexer
    indexer = MultiCollectionIndexer(
        qdrant_url=os.getenv("QDRANT_URL", "http://localhost:6333"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        recreate_collections=os.getenv("RECREATE_COLLECTIONS", "false").lower() == "true"
    )

    # Define paths
    project_root = Path(__file__).parent.parent

    # Track indexing results
    results = {
        "sql_examples": 0,
        "dbt_models": 0,
        "table_schemas": 0,
        "business_knowledge": 0
    }

    # 1. Index SQL Examples
    examples_path = project_root / "data" / "sql_examples.json"
    if examples_path.exists():
        results["sql_examples"] = await indexer.index_sql_examples(examples_path)
    else:
        logger.warning("sql_examples_not_found", path=str(examples_path))

    # 2. Index dbt Models
    dbt_path = project_root / "data" / "dbt_project"
    if dbt_path.exists():
        results["dbt_models"] = await indexer.index_dbt_models(dbt_path)
    else:
        logger.warning("dbt_project_not_found", path=str(dbt_path))

    # 3. Index Table Schemas
    schemas_path = project_root / "data" / "schemas.json"
    if schemas_path.exists():
        results["table_schemas"] = await indexer.index_table_schemas(schemas_path)
    else:
        logger.warning("schemas_not_found", path=str(schemas_path))

    # 4. Index Business Knowledge
    docs_path = project_root / "data" / "docs"
    if docs_path.exists():
        results["business_knowledge"] = await indexer.index_business_knowledge(docs_path)
    else:
        logger.warning("docs_not_found", path=str(docs_path))

    # Get final statistics
    stats = await indexer.get_collection_stats()

    # Print summary
    logger.info("=" * 50)
    logger.info("INDEXING COMPLETE")
    logger.info("=" * 50)

    for collection, count in results.items():
        logger.info(f"{collection}: {count} documents indexed")

    logger.info("-" * 50)
    logger.info("Collection Statistics:")
    for collection, info in stats.items():
        if "error" not in info:
            logger.info(f"{collection}: {info['points_count']} total points")

    return results


if __name__ == "__main__":
    asyncio.run(main())