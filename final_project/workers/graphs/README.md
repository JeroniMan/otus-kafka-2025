# LangGraph Workflows

> State machine orchestration for query processing using LangGraph framework.

## 🎯 What It Does

Coordinates the entire query processing pipeline through composable state machines. Each subgraph handles a specific aspect of the workflow, with the main orchestrator managing the overall flow and error recovery.

### Core Subgraphs

| Subgraph | Purpose | Input | Output |
|----------|---------|-------|--------|
| **MainOrchestrator** | Coordinates all subgraphs | Task from queue | Final report |
| **ContextGatherer** | RAG retrieval | Query text | Schemas, examples, docs |
| **SQLGenerator** | Query generation | Context + question | Validated SQL |
| **QueryExecutor** | Safe execution | SQL query | Results + metrics |

## 🏗️ Architecture

### Workflow Flow

```
┌─────────────┐
│  Initialize │
└──────┬──────┘
       ↓
┌─────────────┐     ┌──────────┐
│   Context   │────→│  Schemas │
│  Gathering  │     │ Examples │
└──────┬──────┘     │   Docs   │
       ↓            └──────────┘
┌─────────────┐     ┌──────────┐
│     SQL     │────→│   LLM    │
│ Generation  │     │ Validate │
└──────┬──────┘     └──────────┘
       ↓
┌─────────────┐     ┌──────────┐
│  Execution  │────→│ BigQuery │
│   & Cache   │     │  Cache   │
└──────┬──────┘     └──────────┘
       ↓
┌─────────────┐
│   Report    │
└─────────────┘
```

### State Types

```
BaseState                 # Shared across all workflows
├── ContextState         # RAG search results
├── SQLState            # Generated query + validation
├── ExecutionState      # Query results + metrics
└── MainState          # Complete workflow state
```

## 🔄 Main Orchestrator

### Workflow Stages

| Stage | Actions | Next Stage | On Error |
|-------|---------|------------|----------|
| `initialize` | Load session, set config | `gather_context` | `handle_error` |
| `gather_context` | RAG search, rank results | `generate_sql` | `generate_sql` (with defaults) |
| `generate_sql` | LLM generation, validation | `execute_query` | `handle_error` |
| `execute_query` | Check cache, run query | `create_report` | `handle_error` |
| `create_report` | Format results | `END` | - |
| `handle_error` | Log, notify, retry/fail | `END` | - |

### State Management

- **Persistent State** - Maintained across entire workflow
- **Subgraph State** - Scoped to specific subgraph
- **Session State** - User context across queries
- **Error State** - Failure tracking and recovery

## 📚 Context Gatherer

### Search Strategies

| Strategy | When Used | Collections Searched |
|----------|-----------|---------------------|
| `SQL_HEAVY` | Metrics, calculations | SQL examples, schemas |
| `BUSINESS_HEAVY` | Definitions, concepts | Documentation, glossary |
| `SCHEMA_ONLY` | Table structure | Schema metadata only |
| `COMPREHENSIVE` | Complex queries | All collections |

### Ranking Algorithm

1. **Semantic similarity** - Vector distance score
2. **Recency weighting** - Prefer recent examples
3. **Usage frequency** - Popular patterns scored higher
4. **Source authority** - Official docs weighted more

## 🧠 SQL Generator

### Generation Pipeline

```
Question + Context → Prompt Building → LLM → SQL Extraction → Validation → Refinement
                          ↓                       ↓              ↓            ↓
                     [Templates]            [GPT-4]      [Rules]     [Retry]
```

### Validation Rules

- ✅ **Syntax** - Valid BigQuery SQL
- ✅ **Safety** - Read-only operations
- ✅ **Efficiency** - Partition filters present
- ✅ **Cost** - Under configured limits
- ✅ **Completeness** - All referenced tables exist

### Refinement Loop

| Attempt | Strategy | Max Retries |
|---------|----------|-------------|
| 1 | Initial generation | - |
| 2 | Add missing filters | 2 |
| 3 | Simplify query | 1 |
| 4 | Fallback to basic | - |

## 🚀 Query Executor

### Execution Strategy

```
Check Cache → Found? ──Yes──→ Return Cached
     ↓         
     No
     ↓
Cost Check → OK? ──No──→ Request Confirmation
     ↓
    Yes
     ↓
Execute → Stream/Batch → Cache → Return
```

### Cache Strategies

| Data Type | Cache TTL | Invalidation |
|-----------|-----------|--------------|
| Historical (>30 days) | Permanent | Manual |
| Recent (7-30 days) | 24 hours | Daily |
| Current (<7 days) | 1 hour | Hourly |
| Real-time (today) | No cache | - |
| Metadata (schemas) | 24 hours | On change |

## 🛡️ Error Handling

### Error Recovery Strategies

| Error Type | Detection | Recovery | User Feedback |
|------------|-----------|----------|---------------|
| **LLM Timeout** | 30s limit | Retry with simpler prompt | "Simplifying query..." |
| **Invalid SQL** | Validation fails | Refine with feedback | Show specific issue |
| **Cost Exceeded** | Dry run > limit | Request confirmation | Show cost estimate |
| **BigQuery Error** | Execution fails | Retry or simplify | Technical message |
| **No Context** | RAG returns empty | Use defaults | "Using general knowledge..." |

### Workflow Resilience

- **Checkpointing** - Save state after each stage
- **Partial Success** - Continue with available data
- **Graceful Degradation** - Fallback to simpler approaches
- **Circuit Breaking** - Stop after repeated failures

## 📊 Configuration

### Workflow Settings

```bash
# Timeouts (seconds)
CONTEXT_TIMEOUT=10
SQL_GENERATION_TIMEOUT=30
EXECUTION_TIMEOUT=300

# Retries
MAX_RETRIES=3
RETRY_BACKOFF_BASE=2

# Cache
CACHE_ENABLED=true
SESSION_CACHE_TTL=3600

# Safety
REQUIRE_DRY_RUN=true
MAX_COST_USD=5.0
```

## 🧪 Testing

```bash
# Test individual subgraph
pytest workers/graphs/tests/test_context.py

# Test full workflow
pytest workers/graphs/tests/test_orchestrator.py

# Test state transitions
pytest workers/graphs/tests/test_states.py
```

## 📈 Performance Metrics

| Metric | Target | Monitoring |
|--------|--------|------------|
| Context retrieval | <2s | RAG latency |
| SQL generation | <5s | LLM response time |
| Validation | <1s | Dry run time |
| Total workflow | <30s | End-to-end |
| Cache hit rate | >70% | Redis stats |

## 🔗 Dependencies

- **[LangGraph](https://github.com/langchain-ai/langgraph)** - Workflow framework
- **[LangChain](https://python.langchain.com/)** - LLM integration
- **[Tools](../tools/README.md)** - BigQuery, RAG, SQL generator
- **[Utils](../utils/README.md)** - Caching, sessions

---

*LangGraph workflows are the brain of the Workers component, orchestrating complex query processing with reliability and observability.*