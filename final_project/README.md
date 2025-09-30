# AI Analytics Assistant

> A production-ready Slack bot that enables business users to query BigQuery data warehouses using natural language, powered by LangGraph orchestration and GPT-4.

## 🎯 What It Does

Transforms questions like *"What was our revenue last month?"* into optimized BigQuery SQL queries, executes them safely, and returns formatted results in Slack.

### Key Features

- **Natural Language Interface** - No SQL knowledge required
- **Smart Context** - Uses RAG to understand your data schema and business logic
- **Safety First** - Automatic cost estimation, query validation, and execution limits
- **Intelligent Caching** - Reduces costs by caching results based on query patterns
- **Session Awareness** - Maintains context across multiple questions

## 🏗️ Architecture Overview

```
User → Slack Bot → Redis Queue → Workers → BigQuery
         ↑                           ↓
         └──────── Results ──────────┘
```

### Components

| Component | Role | Technology |
|-----------|------|------------|
| **Slack Bot** | User interface, message routing | Python, Slack Bolt |
| **Workers** | Query processing, SQL generation | LangGraph, OpenAI |
| **Queue** | Task distribution, decoupling | Redis |
| **Vector DB** | Schema and documentation search | Qdrant |
| **Data Warehouse** | Query execution | Google BigQuery |

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Google Cloud Project with BigQuery
- OpenAI API key
- Slack workspace admin access

### Installation

```bash
# Clone and setup
git clone https://github.com/your-org/ai-analytics-assistant.git
cd ai-analytics-assistant
cp .env.example .env

# Configure credentials in .env
# - Slack tokens (SLACK_BOT_TOKEN, SLACK_APP_TOKEN)
# - OpenAI key (OPENAI_API_KEY)
# - BigQuery project (BQ_SOURCE_PROJECT)

# Start services
docker-compose up -d

# Verify health
make status
```

### Slack Setup

1. Create app at [api.slack.com/apps](https://api.slack.com/apps) using the manifest in `docs/slack-manifest.yaml`
2. Enable Socket Mode and generate app token
3. Install to workspace
4. Invite bot to channels: `/invite @AI Analytics Assistant`

## 💬 Usage

### In Slack

- **Direct Message**: `what's our MRR?`
- **Mention**: `@AI Analytics Assistant show top 10 customers`
- **Slash Command**: `/ask calculate churn rate for Q4`

### Query Flow

1. Bot acknowledges: *"🔍 Analyzing your request..."*
2. Worker processes in background
3. Returns preview with cost estimate
4. Executes after confirmation (if expensive)
5. Delivers formatted results

## 🛡️ Safety Features

- **Read-only** queries (SELECT only)
- **Cost limits** ($5 default per query)
- **Data limits** (20GB scan maximum)
- **Partition enforcement** for large tables
- **Rate limiting** per user/workspace
- **Channel restrictions** for sensitive data

## 📁 Project Structure

```
ai-analytics-assistant/
├── slack_bot/          # Slack interface (Producer)
├── workers/            # Query processing (Consumers)
│   ├── graphs/        # LangGraph workflows
│   ├── tools/         # BigQuery, RAG integrations
│   └── utils/         # Caching, sessions
├── docs/              # Documentation
├── docker-compose.yml # Service orchestration
└── .env.example       # Configuration template
```

## 🔄 How It Works

### LangGraph Orchestration

The system uses a state machine approach to process queries:

```
Initialize → Gather Context → Generate SQL → Validate → Execute → Format Results
                    ↓              ↓            ↓         ↓
                  [RAG]         [GPT-4o-mini]    [Dry Run]  [BigQuery]
```

### Intelligent Caching

- **Historical data** → Cached permanently
- **Aggregations** → Cached for 1 hour  
- **Real-time data** → Never cached
- **Schema queries** → Cached for 24 hours

## 📊 Configuration

Key settings in `.env`:

```bash
# Model Selection
OPENAI_MODEL=gpt-4o-mini  # or gpt-4-turbo for better quality

# Safety Limits  
BQ_MAX_BYTES_BILLED=20000000000  # 20GB
MAX_QUERY_COST=5.0                # $5

# Rate Limiting
RATE_LIMIT_PER_MIN=10
CONCURRENT_LIMIT=5
```

See [.env.example](.env.example) for all options.

## 🔧 Operations

### Monitoring

```bash
# View logs
make logs

# Check queue depth
make queue-status

# Health check
curl http://localhost:8080/health
```

### Scaling

```bash
# Scale workers horizontally
docker-compose up --scale worker=1

# Adjust per-worker concurrency
WORKER_CONCURRENCY=2 docker-compose up
```

## 📚 Documentation

- **[Architecture Guide](docs/ARCHITECTURE.md)** - System design and patterns
- **[Setup Guide](docs/SETUP.md)** - Detailed installation instructions
- **[Slack Bot](slack_bot/README.md)** - Bot component details
- **[Workers](workers/README.md)** - Processing engine documentation
- **[LangGraph Workflows](workers/graphs/README.md)** - Orchestration details
- **[Tools](workers/tools/README.md)** - External integrations