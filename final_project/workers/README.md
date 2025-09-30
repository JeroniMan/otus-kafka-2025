# Workers Component

> The query processing engine that transforms natural language questions into SQL queries and executes them safely on BigQuery.

## 🎯 What It Does

Workers pull tasks from the queue, orchestrate the entire query lifecycle using LangGraph state machines, and send results back through Pub/Sub. Each worker can process multiple tasks concurrently.

### Key Responsibilities

- **Context Retrieval** - Finds relevant schemas, examples, and documentation
- **SQL Generation** - Converts questions to optimized BigQuery SQL
- **Query Validation** - Ensures safety and estimates costs
- **Execution** - Runs queries with monitoring and limits
- **Result Caching** - Intelligently caches based on data freshness
- **Session Management** - Maintains context between questions

## 🏗️ Architecture

### Workflow Pipeline

```
Task → Context Gathering → SQL Generation → Validation → Execution → Results
           ↓                    ↓              ↓            ↓
         [RAG]               [GPT-4o-mini]   [Dry Run]    [BigQuery]
```

### Component Structure

```
workers/
├── graphs/         # LangGraph workflows (orchestration)
├── tools/          # External integrations (BigQuery, RAG, OpenAI)
├── utils/          # Utilities (caching, sessions, validation)
├── main.py         # Worker entry point
└── config.py       # Configuration management
```

### State Machine

Workers use LangGraph to manage workflow states:

| Stage | Purpose | Failure Action |
|-------|---------|---------------|
| `INITIALIZING` | Setup context, load session | Retry |
| `GATHERING_CONTEXT` | RAG search for relevant info | Use defaults |
| `GENERATING_SQL` | LLM creates SQL query | Retry with feedback |
| `VALIDATING` | Safety and cost checks | Reject if unsafe |
| `EXECUTING` | Run on BigQuery | Return error |
| `COMPLETED` | Format and cache results | - |

## 🚀 Configuration

### Essential Settings

```bash
# LLM Settings
OPENAI_MODEL=gpt-4o-mini        # Model selection
OPENAI_TEMPERATURE=0.1          # Lower = more deterministic

# BigQuery Limits
BQ_MAX_BYTES_BILLED=20000000000 # 20GB max scan
MAX_QUERY_COST=5.0              # $5 per query limit

# Worker Performance
WORKER_CONCURRENCY=5            # Parallel tasks per worker
MAX_RETRIES=3                   # Retry attempts

# Cache Strategy
CACHE_TTL_HISTORICAL=86400      # 24 hours for old data
CACHE_TTL_REALTIME=0           # No cache for current data
```

## 🔄 Processing Flow

### 1. Context Gathering

Searches multiple Qdrant collections in parallel:
- **Table schemas** - Structure and column information
- **SQL examples** - Similar queries and patterns  
- **Documentation** - Business logic and definitions

### 2. SQL Generation

Builds optimized queries using:
- Retrieved context
- Session history
- Few-shot examples
- Safety guidelines

### 3. Query Validation

Enforces safety rules:
- ✅ Read-only (SELECT only)
- ✅ No SELECT * (except INFORMATION_SCHEMA)
- ✅ Partition filters required
- ✅ Cost under limit
- ✅ Bytes scanned under limit

### 4. Smart Execution

Optimizes performance via:
- Cache checking first
- Query result streaming for large data
- Automatic retries for transient errors
- Dead letter queue for failed tasks

## 📊 Monitoring

### Metrics Tracked

- Query latency (p50, p95, p99)
- Cache hit rate
- Token usage and costs
- Error rates by type
- Bytes scanned per query

### Health Indicators

```bash
# Check worker health
curl http://localhost:8081/health

# View processing metrics
docker-compose exec worker python -m workers.metrics

# Monitor queue depth
redis-cli LLEN analytics:queue:pending
```

## 📁 Subcomponents

### [Graphs](graphs/README.md)
LangGraph workflow orchestration - state machines, conditional flows, error handling

### [Tools](tools/README.md)  
External integrations - BigQuery client, RAG search, SQL generator, validators

### [Utils](utils/README.md)
Supporting utilities - cache manager, session handler, metrics collector

## 🔗 Related Documentation

- [Main README](../README.md) - System overview
- [Architecture](../docs/ARCHITECTURE.md) - Design decisions
- [Slack Bot](../slack_bot/README.md) - Message handling
- [LangGraph Docs](https://github.com/langchain-ai/langgraph) - Framework

---

*Part of AI Analytics Assistant - Workers are the brain of the system, processing queries safely and efficiently.*