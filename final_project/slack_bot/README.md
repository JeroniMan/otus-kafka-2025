# Slack Bot Component

> The user interface layer that handles Slack interactions and routes messages between users and workers.

## 🎯 What It Does

Acts as the **Producer** in the system - receives messages from Slack, creates tasks for workers, and delivers results back to users. Maintains WebSocket connection via Socket Mode for real-time communication without public endpoints.

### Key Responsibilities

- **Event Handling** - Process mentions, DMs, and slash commands
- **Task Queuing** - Convert messages to tasks for workers
- **Result Delivery** - Format and send responses to users
- **Access Control** - Channel and user permissions
- **Rate Limiting** - Prevent abuse and control costs
- **Session Tracking** - Link queries to conversations

## 🏗️ Architecture

### Message Flow

```
User → Slack API → Bot (Socket Mode) → Redis Queue → Workers
         ↑                                  ↓
         └────── Formatted Results ─── Pub/Sub
```

### Component Structure

```
slack_bot/
├── main.py          # Service orchestrator
├── handlers.py      # Event processing
├── listeners.py     # Pub/Sub updates
├── formatters.py    # Message formatting
├── validators.py    # Permissions & rate limits
├── config.py        # Configuration
└── health.py        # Health checks
```

## 🚀 Setup

### Slack App Configuration

1. Create app at [api.slack.com/apps](https://api.slack.com/apps)
2. Use manifest from `docs/slack-manifest.yaml`
3. Enable Socket Mode
4. Install to workspace
5. Copy tokens to `.env`

### Required Tokens

```bash
SLACK_BOT_TOKEN=xoxb-...      # Bot User OAuth Token
SLACK_APP_TOKEN=xapp-...      # Socket Mode connection
SLACK_SIGNING_SECRET=...      # Request verification
```

### Running the Bot

```bash
# Standalone
python slack_bot/main.py

# Docker
docker-compose up slack-bot

# Debug mode
LOG_LEVEL=DEBUG python slack_bot/main.py
```

## 📱 Supported Interactions

### Event Types

| Event | Trigger | Example | Response |
|-------|---------|---------|----------|
| **app_mention** | @bot in channel | `@AI Assistant show revenue` | Acknowledges and processes |
| **message.im** | Direct message | `what's our MRR?` | Immediate response |
| **/ask** | Slash command | `/ask top customers` | Queued for processing |
| **/sql** | Admin command | `/sql SELECT COUNT(*)...` | Requires admin role |

### Interactive Components

| Component | Purpose | User Action |
|-----------|---------|-------------|
| **Confirmation buttons** | Expensive queries | Approve/Cancel execution |
| **Download button** | Large results | Get CSV export |
| **Retry button** | Failed queries | Try again |
| **Feedback buttons** | Result quality | 👍 👎 reactions |

## 🎨 Message Formatting

### Response Types

```
Acknowledgment → Progress Update → Result/Error → Follow-up Options
      ↓               ↓                ↓              ↓
  [Immediate]    [Status Bar]    [Block Kit]     [Buttons]
```

### Block Kit Templates

| Template | Use Case | Components |
|----------|----------|------------|
| **Query Preview** | Cost estimation | Header, SQL, metrics, buttons |
| **Results Table** | Data display | Header, stats, data preview, download |
| **Error Message** | Failures | Error type, message, suggestions |
| **Progress Bar** | Long queries | Status, elapsed time, stage |

### Result Formatting

- **Small results (<10 rows)** - Inline table
- **Medium results (10-100)** - Preview + thread
- **Large results (>100)** - Summary + CSV file

## 🔒 Security & Permissions

### Access Control Layers

```
Channel Check → User Check → Rate Limit → Content Validation → Queue
      ↓             ↓            ↓              ↓              ↓
 [Allowlist]   [Allowlist]   [Redis]       [Regex]        [Accept]
```

### Permission Levels

| Level | Capabilities | Configuration |
|-------|-------------|---------------|
| **Admin** | Direct SQL, bypass limits | `SLACK_ADMIN_USERS` |
| **Power User** | Higher rate limits | `SLACK_POWER_USERS` |
| **Standard** | Normal queries | Default |
| **Restricted** | Read-only channels | Channel-specific |

### Rate Limiting

| Limit Type | Default | Reset Period | Key |
|------------|---------|--------------|-----|
| Per minute | 10 | 60 seconds | `rate:user:minute` |
| Per hour | 50 | 1 hour | `rate:user:hour` |
| Per day | 200 | 24 hours | `rate:user:day` |
| Concurrent | 5 | On completion | `concurrent:workspace` |

## 📨 Pub/Sub Updates

### Message Types

| Update Type | Purpose | Payload | Action |
|------------|---------|---------|--------|
| `status_update` | Progress tracking | Stage, message | Update original message |
| `query_result` | Final results | Data, metrics | Format and send |
| `request_confirmation` | Cost approval | SQL, cost estimate | Add buttons |
| `error` | Failure notification | Error type, suggestions | Show error block |

### Update Processing

```
Redis Pub/Sub → Parse Message → Route to Handler → Update Slack
       ↓              ↓                ↓              ↓
  [Subscribe]    [JSON Parse]    [Type Check]    [Chat API]
```

## 🏥 Health Monitoring

### Health Check Endpoint

```
GET /health

{
  "status": "healthy",
  "uptime": 3600,
  "components": {
    "slack": "connected",
    "redis": "connected",
    "pubsub": "listening"
  },
  "metrics": {
    "messages_processed": 1234,
    "queue_depth": 5,
    "error_rate": 0.02
  }
}
```

### Key Metrics

| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| Response time | <3s | >5s |
| Queue depth | <10 | >50 |
| Error rate | <1% | >5% |
| Socket reconnects | <5/hour | >10/hour |

## 🐛 Error Handling

### Error Strategies

| Error Type | Strategy | User Experience |
|------------|----------|----------------|
| **Rate Limited** | Show remaining time | "Try again in X minutes" |
| **Invalid Channel** | Log and reject | "Bot not allowed here" |
| **Redis Down** | In-memory queue | Slight delay |
| **Slack API Error** | Exponential backoff | Retry automatically |
| **Socket Disconnect** | Auto-reconnect | Brief interruption |

### Graceful Degradation

```
Primary Path Failed → Check Fallback → Use Alternative → Notify Ops
        ↓                  ↓                ↓               ↓
   [Redis Queue]    [Memory Queue]    [Process]      [Alert]
```

## 📊 Configuration

### Essential Settings

```bash
# Access Control
SLACK_ALLOWED_CHANNELS=C123,C456
SLACK_ADMIN_USERS=U789

# Rate Limits
RATE_LIMIT_PER_MIN=10
CONCURRENT_LIMIT=5

# Timeouts
ACK_TIMEOUT=3          # Slack requirement
REDIS_TIMEOUT=5
MESSAGE_TIMEOUT=30

# Logging
LOG_LEVEL=INFO
LOG_JSON=false
```

## 🧪 Testing

```bash
# Run all tests
pytest slack_bot/tests/ -v

# Test event handling
pytest slack_bot/tests/test_handlers.py

# Test formatting
pytest slack_bot/tests/test_formatters.py

# Load test
python slack_bot/tests/load_test.py --messages=100
```

## 🔧 Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Bot not responding | Socket disconnected | Check APP_TOKEN, restart |
| Messages queuing slowly | Redis connection | Check Redis health |
| Rate limit errors | Too many requests | Adjust limits or add caching |
| Missing events | Permissions | Check OAuth scopes |
| Formatting broken | Slack API changes | Update Block Kit templates |

### Debug Commands

```bash
# Check Socket Mode
curl -X POST https://slack.com/api/apps.connections.open \
  -H "Authorization: Bearer $SLACK_APP_TOKEN"

# Test bot auth
curl -X POST https://slack.com/api/auth.test \
  -H "Authorization: Bearer $SLACK_BOT_TOKEN"

# Monitor Redis
redis-cli MONITOR | grep slack
```

## 📈 Performance Optimization

| Optimization | Impact | Implementation |
|--------------|--------|----------------|
| Message batching | -40% API calls | Group updates |
| Connection pooling | -30% latency | Reuse clients |
| Async processing | -50% response time | Non-blocking I/O |
| Smart caching | -60% duplicate work | Cache permissions |
| Thread management | -70% message clutter | Use threads |

## 🔗 Related Documentation

- [Main README](../README.md) - System overview
- [Workers](../workers/README.md) - Query processing
- [Slack API](https://api.slack.com/docs) - Official docs
- [Block Kit](https://api.slack.com/block-kit) - Message formatting
- [Socket Mode](https://api.slack.com/apis/connections/socket) - Connection guide

---

*The Slack Bot is the friendly face of the AI Analytics Assistant, making data accessible to everyone through natural conversation.*