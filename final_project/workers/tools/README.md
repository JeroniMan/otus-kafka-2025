# Tools Component

> External service integrations for BigQuery operations, semantic search, and SQL generation.

## 🎯 What It Does

Provides abstraction layers between LangGraph workflows and external services. Each tool handles authentication, rate limiting, error recovery, and response formatting for its respective service.

### Available Tools

| Tool | Service | Purpose |
|------|---------|---------|
| **BigQueryTool** | Google BigQuery | Query execution with safety controls |
| **MultiCollectionRAG** | Qdrant Vector DB | Semantic search across collections |
| **SQLGenerator** | OpenAI GPT-4 | Natural language to SQL conversion |
| **SchemaLoader** | BigQuery + Cache | Table metadata discovery |
| **SQLValidator** | BigQuery (dry-run) | Query validation and cost estimation |

## 🏗️ Architecture

### Tool Interactions

```
LangGraph Workflows
        ↓
   Tools Layer     ←── Caching, Rate Limiting, Error Handling
        ↓
External Services  ←── BigQuery, Qdrant, OpenAI
```

### Design Principles

- **Abstraction** - Hide service complexity from workflows
- **Safety** - Validate all operations before execution
- **Resilience** - Retry transient failures automatically
- **Efficiency** - Cache expensive operations
- **Monitoring** - Track usage and performance

## 🗄️ BigQuery Tool

### Capabilities

| Operation | Purpose | Cost Impact |
|-----------|---------|-------------|
| `dry_run` | Validate and estimate costs | Free |
| `execute` | Run query with monitoring | Billed |
| `get_schema` | Retrieve table metadata | Minimal |
| `list_tables` | Discover available data | Free |
| `stream_results` | Handle large datasets | Optimized |

### Safety Features

- **Query Validation** - Blocks dangerous operations
- **Cost Limits** - Rejects expensive queries
- **Byte Limits** - Prevents runaway scans
- **Timeout Protection** - Cancels long-running queries
- **Result Truncation** - Limits memory usage

### Configuration

```bash
BQ_SOURCE_PROJECT=your-data-project
BQ_LOCATION=US
BQ_MAX_BYTES_BILLED=20000000000  # 20GB
MAX_QUERY_COST=5.0                # $5
QUERY_TIMEOUT=300                 # 5 minutes
```

## 🔍 RAG Tool

### Search Strategy

```
Query → Embedding → Parallel Search → Combine Results → Rerank → Top K
           ↓              ↓                                ↓
        [OpenAI]    [All Collections]                 [Scoring]
```

### Collections

| Collection | Content | Weight | Use Case |
|------------|---------|--------|----------|
| `schemas` | Table structures | 1.0 | Table/column discovery |
| `examples` | SQL patterns | 1.5 | Similar query patterns |
| `docs` | Business logic | 0.8 | Definitions, rules |
| `metrics` | KPI definitions | 1.2 | Business metrics |

### Ranking Factors

1. **Semantic Similarity** - Vector distance (40%)
2. **Keyword Match** - Exact terms (30%)
3. **Recency** - Newer examples preferred (20%)
4. **Popularity** - Usage frequency (10%)

### Performance

| Operation | Target Latency | Cache TTL |
|-----------|---------------|-----------|
| Embedding generation | <500ms | 7 days |
| Collection search | <1s | 1 hour |
| Reranking | <200ms | - |
| Full RAG pipeline | <2s | 1 hour |

## 🧠 SQL Generator

### Generation Process

```
Question + Context → Prompt Template → LLM → SQL Extraction → Validation
                          ↓                      ↓              ↓
                    [Few-shot Examples]      [GPT-4]        [Rules]
```

### Prompt Components

| Component | Purpose | Size |
|-----------|---------|------|
| System prompt | Role and rules | ~500 tokens |
| Context | Schemas and examples | ~2000 tokens |
| Few-shot examples | Pattern learning | ~1000 tokens |
| User question | Actual query | ~50 tokens |

### Model Settings

```bash
OPENAI_MODEL=gpt-4o-mini    # Fast and cheap
# or gpt-4-turbo            # Better quality
# or gpt-4o                 # Best quality

OPENAI_TEMPERATURE=0.1       # Low for consistency
MAX_TOKENS=2000             # SQL output limit
```

### Quality Controls

- **Syntax Validation** - Parse SQL before returning
- **Schema Verification** - Check table/column existence
- **Cost Estimation** - Predict query expense
- **Complexity Scoring** - Assess query difficulty

## ✅ SQL Validator

### Validation Pipeline

```
SQL → Syntax Check → Safety Rules → Schema Validation → Cost Estimation → Score
         ↓               ↓                ↓                  ↓           ↓
     [Parser]        [Ruleset]        [Metadata]        [Dry Run]    [0-100]
```

### Validation Rules

| Rule | Check | Action if Failed |
|------|-------|-----------------|
| **Read-only** | No DML/DDL operations | Reject |
| **No SELECT *** | Explicit columns required | Warning |
| **Partition Filter** | Required for large tables | Add filter |
| **Row Limit** | LIMIT for exploration | Add LIMIT 1000 |
| **Join Safety** | No CROSS JOIN without WHERE | Reject |
| **Cost Limit** | Under $5 threshold | Request approval |

## 📊 Schema Loader

### Discovery Strategy

```
List Datasets → Filter Accessible → List Tables → Get Metadata → Cache
       ↓              ↓                  ↓             ↓          ↓
   [BigQuery]     [Permissions]     [BigQuery]    [BigQuery]  [Redis]
```

### Metadata Collected

- Table structure (columns, types)
- Partitioning configuration
- Clustering fields
- Row counts and data size
- Last modified timestamp
- Description and labels

### Circuit Breaker

```
Closed (Normal) → Failures++ → Open (Block) → Wait → Half-Open (Test) → Reset
                      ↓                        ↓           ↓
                  [Threshold]              [Timeout]    [Success?]
```

## 📈 Performance Optimization

### Caching Layers

1. **Result Cache** - Full query results (Redis)
2. **Embedding Cache** - Generated embeddings (Memory + Redis)
3. **Schema Cache** - Table metadata (Redis)
4. **Connection Pool** - Client connections (In-process)

## 📊 Monitoring

### Key Metrics

| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| BigQuery latency | <5s | >10s |
| OpenAI latency | <3s | >10s |
| Qdrant latency | <1s | >3s |
| Cache hit rate | >70% | <50% |
| Error rate | <1% | >5% |

## 🔗 Dependencies

- **[google-cloud-bigquery](https://cloud.google.com/python/docs/reference/bigquery/latest)** - BigQuery client
- **[qdrant-client](https://github.com/qdrant/qdrant-client)** - Vector search
- **[openai](https://github.com/openai/openai-python)** - GPT-4 API
- **[redis-py](https://github.com/redis/redis-py)** - Caching layer

---

*Tools provide reliable, safe, and efficient access to external services for the Workers component.*