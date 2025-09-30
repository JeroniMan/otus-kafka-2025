"""
Enhanced RAG Tool with Multi-Collection Support
Location: workers/tools/rag.py
"""

import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum
import asyncio

import structlog
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from openai import AsyncOpenAI
import redis

from workers.config import settings

logger = structlog.get_logger()


class CollectionType(str, Enum):
    """Types of collections for different content"""
    SQL_EXAMPLES = "sql_examples"
    DBT_MODELS = "dbt_models"
    TABLE_SCHEMAS = "table_schemas"
    BUSINESS_KNOWLEDGE = "business_knowledge"


class SearchStrategy(str, Enum):
    """Search strategies for different query types"""
    SQL_GENERATION = "sql_generation"  # Focus on examples and schemas
    BUSINESS_CONTEXT = "business_context"  # Focus on knowledge and models
    COMPREHENSIVE = "comprehensive"  # Search all collections
    SCHEMA_ONLY = "schema_only"  # Only table schemas


class MultiCollectionRAG:
    """Enhanced RAG with multi-collection search capabilities"""

    # ИСПРАВЛЕНО: Маппинг для простых имен коллекций (как в вашем Qdrant)
    COLLECTION_NAME_MAP = {
        CollectionType.SQL_EXAMPLES: "sql_examples",
        CollectionType.DBT_MODELS: "dbt_models",
        CollectionType.TABLE_SCHEMAS: "table_schemas",
        CollectionType.BUSINESS_KNOWLEDGE: "business_knowledge"
    }

    # Collection search priorities for different strategies
    SEARCH_STRATEGIES = {
        SearchStrategy.SQL_GENERATION: [
            (CollectionType.SQL_EXAMPLES, 0.35),
            (CollectionType.TABLE_SCHEMAS, 0.35),
            (CollectionType.DBT_MODELS, 0.20),
            (CollectionType.BUSINESS_KNOWLEDGE, 0.10)
        ],
        SearchStrategy.BUSINESS_CONTEXT: [
            (CollectionType.BUSINESS_KNOWLEDGE, 0.40),
            (CollectionType.DBT_MODELS, 0.30),
            (CollectionType.SQL_EXAMPLES, 0.20),
            (CollectionType.TABLE_SCHEMAS, 0.10)
        ],
        SearchStrategy.COMPREHENSIVE: [
            (CollectionType.SQL_EXAMPLES, 0.25),
            (CollectionType.TABLE_SCHEMAS, 0.25),
            (CollectionType.DBT_MODELS, 0.25),
            (CollectionType.BUSINESS_KNOWLEDGE, 0.25)
        ],
        SearchStrategy.SCHEMA_ONLY: [
            (CollectionType.TABLE_SCHEMAS, 1.0)
        ]
    }

    def __init__(self,
                 qdrant_url: str = None,
                 openai_api_key: str = None,
                 redis_client: Optional[redis.Redis] = None):
        """
        Initialize multi-collection RAG

        Args:
            qdrant_url: Qdrant server URL
            openai_api_key: OpenAI API key
            redis_client: Redis client for caching
        """
        self.qdrant = None
        self.openai = None
        self.redis = redis_client

        # Initialize Qdrant client
        if settings.qdrant_enabled:
            try:
                self.qdrant = QdrantClient(
                    url=qdrant_url or settings.qdrant_url,
                    api_key=settings.qdrant_api_key if settings.qdrant_api_key else None,
                    timeout=30
                )

                # Verify collections exist
                self._verify_collections()

                logger.info("multi_collection_rag_initialized",
                           url=qdrant_url or settings.qdrant_url)

            except Exception as e:
                logger.error("rag_init_failed", error=str(e))
                self.qdrant = None

        # Initialize OpenAI client
        try:
            self.openai = AsyncOpenAI(
                api_key=openai_api_key or settings.openai_api_key
            )
        except Exception as e:
            logger.error("openai_init_failed", error=str(e))

    def _verify_collections(self):
        """Verify that expected collections exist"""
        try:
            existing = {c.name for c in self.qdrant.get_collections().collections}

            # Ожидаемые коллекции с простыми именами
            expected_names = {
                "sql_examples",
                "dbt_models",
                "table_schemas",
                "business_knowledge"
            }

            # Проверяем что все коллекции существуют
            missing = expected_names - existing
            if missing:
                logger.warning("missing_collections",
                             missing=list(missing),
                             existing=list(existing))

            # ИСПРАВЛЕНО: Используем простые имена напрямую
            self.COLLECTION_NAME_MAP = {
                CollectionType.SQL_EXAMPLES: "sql_examples",
                CollectionType.DBT_MODELS: "dbt_models",
                CollectionType.TABLE_SCHEMAS: "table_schemas",
                CollectionType.BUSINESS_KNOWLEDGE: "business_knowledge"
            }

            logger.info("collection_mapping_established",
                       mapping={k.value: v for k, v in self.COLLECTION_NAME_MAP.items()},
                       existing_collections=list(existing))

        except Exception as e:
            logger.error("collection_verification_failed", error=str(e))

    def _get_collection_name(self, collection_type: CollectionType) -> str:
        """Get the actual collection name in Qdrant"""
        # Так как теперь имена совпадают, можно просто вернуть value
        return collection_type.value

    async def search_knowledge(self,
                              query: str,
                              strategy: SearchStrategy = SearchStrategy.COMPREHENSIVE,
                              limit_per_collection: int = 5,
                              filters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Search across multiple collections with strategy-based prioritization

        Args:
            query: Search query
            strategy: Search strategy to use
            limit_per_collection: Max results per collection
            filters: Additional filters for search

        Returns:
            Combined and ranked results from multiple collections
        """
        if not self.qdrant or not self.openai:
            logger.warning("search_skipped", reason="qdrant_or_openai_not_initialized")
            return []

        try:
            # Get embedding for query
            embedding = await self._get_embedding(query)
            if not embedding:
                logger.warning("embedding_failed_for_query", query=query[:50])
                return []

            # Get search configuration for strategy
            search_config = self.SEARCH_STRATEGIES[strategy]

            # Search each collection in parallel
            search_tasks = []
            for collection_type, weight in search_config:
                if weight > 0:  # Skip collections with 0 weight
                    search_tasks.append(
                        self._search_collection(
                            collection_type=collection_type,
                            embedding=embedding,
                            limit=limit_per_collection,
                            filters=filters,
                            weight=weight
                        )
                    )

            # Execute searches in parallel
            results = await asyncio.gather(*search_tasks, return_exceptions=True)

            # Combine and rank results
            combined_results = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error("collection_search_error", error=str(result))
                    continue
                combined_results.extend(result)

            # Sort by weighted score
            combined_results.sort(key=lambda x: x['weighted_score'], reverse=True)

            # Deduplicate similar results
            final_results = self._deduplicate_results(combined_results)

            logger.info("multi_collection_search_complete",
                       query_length=len(query),
                       strategy=strategy.value,
                       total_results=len(final_results))

            return final_results

        except Exception as e:
            logger.error("multi_collection_search_failed",
                        error=str(e),
                        error_type=type(e).__name__)
            return []

    async def search_single_collection(self,
                                      collection_name: str,
                                      query: str,
                                      limit: int = 5) -> Dict[str, Any]:
        """
        Simple search in a single collection by name
        For backward compatibility and direct collection access
        """
        if not self.qdrant or not self.openai:
            return {'results': [], 'error': 'Qdrant or OpenAI not initialized'}

        try:
            # Map collection name to enum
            collection_map = {
                'sql_examples': CollectionType.SQL_EXAMPLES,
                'table_schemas': CollectionType.TABLE_SCHEMAS,
                'business_knowledge': CollectionType.BUSINESS_KNOWLEDGE,
                'dbt_models': CollectionType.DBT_MODELS
            }

            collection_type = collection_map.get(collection_name)
            if not collection_type:
                logger.warning(f"Unknown collection: {collection_name}")
                return {'results': []}

            # Get embedding
            embedding = await self._get_embedding(query)
            if not embedding:
                return {'results': [], 'error': 'Failed to generate embedding'}

            # Search specific collection
            results = await self._search_collection(
                collection_type=collection_type,
                embedding=embedding,
                limit=limit,
                filters=None,
                weight=1.0
            )

            return {'results': results}

        except Exception as e:
            logger.error(f"Single collection search failed",
                        collection=collection_name,
                        error=str(e))
            return {'results': [], 'error': str(e)}

    async def _search_collection(self,
                                collection_type: CollectionType,
                                embedding: List[float],
                                limit: int,
                                filters: Optional[Dict],
                                weight: float) -> List[Dict[str, Any]]:
        """Search a single collection"""
        try:
            # ИСПРАВЛЕНО: Используем простое имя коллекции
            collection_name = collection_type.value

            # Perform search
            results = self.qdrant.search(
                collection_name=collection_name,
                query_vector=embedding,
                limit=limit,
                with_payload=True,
                query_filter=filters
            )

            # Format results with weighted scores
            formatted_results = []
            for hit in results:
                result = {
                    "collection": collection_type.value,
                    "content": hit.payload.get("content", ""),
                    "metadata": hit.payload,
                    "score": hit.score,
                    "weighted_score": hit.score * weight,
                    "weight": weight
                }

                # Add collection-specific fields
                if collection_type == CollectionType.SQL_EXAMPLES:
                    result["sql"] = hit.payload.get("sql", "")
                    result["question"] = hit.payload.get("question", "")
                elif collection_type == CollectionType.TABLE_SCHEMAS:
                    result["table_name"] = hit.payload.get("full_name", "")
                    result["columns"] = hit.payload.get("columns", [])
                elif collection_type == CollectionType.DBT_MODELS:
                    result["model_name"] = hit.payload.get("model_name", "")
                    result["sql_preview"] = hit.payload.get("sql_preview", "")
                elif collection_type == CollectionType.BUSINESS_KNOWLEDGE:
                    result["document_name"] = hit.payload.get("document_name", "")
                    result["content"] = hit.payload.get("content", "")

                formatted_results.append(result)

            logger.debug("collection_searched",
                        collection=collection_name,
                        results_count=len(formatted_results))

            return formatted_results

        except Exception as e:
            logger.error("collection_search_failed",
                        collection=collection_type.value,
                        error=str(e))
            return []

    async def _get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding with caching"""
        try:
            if not self.openai:
                logger.error("openai_client_not_initialized")
                return None

            # Check cache first
            if self.redis:
                cache_key = f"embedding:{hashlib.md5(text.encode()).hexdigest()}"
                cached = self.redis.get(cache_key)
                if cached:
                    import json
                    logger.debug("embedding_cache_hit")
                    return json.loads(cached)

            # Generate embedding
            response = await self.openai.embeddings.create(
                model="text-embedding-3-small",
                input=text,
                encoding_format="float"
            )

            embedding = response.data[0].embedding

            # Cache the result
            if self.redis:
                import json
                self.redis.setex(
                    cache_key,
                    604800,  # 7 days TTL
                    json.dumps(embedding)
                )

            return embedding

        except Exception as e:
            logger.error("embedding_generation_failed",
                        error=str(e),
                        text_preview=text[:50])
            return None

    def _deduplicate_results(self, results: List[Dict],
                            similarity_threshold: float = 0.85) -> List[Dict]:
        """Remove duplicate or very similar results"""
        if not results:
            return []

        deduplicated = []
        seen_content = set()

        for result in results:
            # Create content hash for exact duplicates
            content_hash = hashlib.md5(
                result.get("content", "").encode()
            ).hexdigest()

            if content_hash not in seen_content:
                seen_content.add(content_hash)
                deduplicated.append(result)

        return deduplicated

    async def get_schema_context(self,
                                 table_names: List[str]) -> List[Dict[str, Any]]:
        """Get specific table schemas by name"""
        if not self.qdrant or not table_names:
            return []

        try:
            results = []

            for table_name in table_names:
                # Search for exact table name match
                table_results = await self.search_knowledge(
                    query=f"table schema {table_name}",
                    strategy=SearchStrategy.SCHEMA_ONLY,
                    limit_per_collection=1
                )

                if table_results:
                    results.extend(table_results)

            return results

        except Exception as e:
            logger.error("schema_context_failed", error=str(e))
            return []

    async def get_relevant_examples(self,
                                   query: str,
                                   limit: int = 3) -> List[Dict[str, Any]]:
        """Get most relevant SQL examples for a query"""
        try:
            # Search with SQL generation strategy
            results = await self.search_knowledge(
                query=query,
                strategy=SearchStrategy.SQL_GENERATION,
                limit_per_collection=limit
            )

            # Filter to only SQL examples
            sql_examples = [
                r for r in results
                if r.get("collection") == CollectionType.SQL_EXAMPLES.value
            ]

            return sql_examples[:limit]

        except Exception as e:
            logger.error("get_examples_failed", error=str(e))
            return []

    async def get_business_context(self,
                                  query: str,
                                  limit: int = 5) -> List[Dict[str, Any]]:
        """Get business context and documentation"""
        try:
            # Search with business context strategy
            results = await self.search_knowledge(
                query=query,
                strategy=SearchStrategy.BUSINESS_CONTEXT,
                limit_per_collection=limit
            )

            return results[:limit]

        except Exception as e:
            logger.error("get_business_context_failed", error=str(e))
            return []


# Backward compatibility alias
RAGTool = MultiCollectionRAG