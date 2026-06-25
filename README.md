# Kafka Failure Orchestrator

A sophisticated AI-powered failure management system that intelligently orchestrates Kafka message failures using Claude AI, MCP (Model Context Protocol), and intelligent retry logic.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [API Endpoints](#api-endpoints)
- [Kafka Topics](#kafka-topics)
- [Database Schema](#database-schema)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Monitoring](#monitoring)
- [Decision Engine](#decision-engine)
- [Contributing](#contributing)

---

## 🎯 Overview

### What We're Building

Kafka Failure Orchestrator is an enterprise-grade system designed to **intelligently handle Kafka consumer failures** without manual intervention. Instead of messages being silently dropped or repeatedly retried, they're analyzed by an AI-powered decision engine that determines whether to:

- **RETRY**: Network timeout? Database deadlock? Fix it automatically.
- **DLQ**: Logic error? Poison pill? Send to Dead Letter Queue for developer review.
- **PENDING**: Unknown error type? Escalate to human review queue.

### What We've Achieved

✅ **Phase 1-2: Failure Detection & Ingestion**
- Kafka consumers detect failures and publish to the Failure Topic
- REST API captures failure context (exception type, error message, retry count)
- PostgreSQL stores complete failure audit trail

✅ **Phase 3: AI-Powered Decision Engine**
- MCP Brain analyzes failure context using Claude AI
- Rule-based decision tree with 5+ decision paths
- Automatic decision execution via Go API

✅ **Phase 4: Automated Recovery**
- Retry failed messages to primary topic
- Dead Letter Queue (DLQ) for poison pills
- Complete audit log for compliance & debugging

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Kafka Cluster                             │
│  ┌─────────────────┐          ┌─────────────────┐            │
│  │ Primary Topics  │          │  Failure Topic  │            │
│  │ (application)   │          │   (failures)    │            │
│  └────────┬────────┘          └────────▲────────┘            │
│           │                            │                     │
│           │ consumer error             │ publish failure     │
│           └────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│         Ingestion API (Go/Gin) :8080                         │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ POST /failures           - Register failed event       │  │
│  │ GET  /tools/failures/:id - Fetch context for MCP       │  │
│  │ POST /tools/decisions    - Execute MCP decision        │  │
│  │ GET  /health            - Health check                 │  │
│  └────────────────────────────────────────────────────────┘  │
│        │                 │                 │                 │
│        ▼                 ▼                 ▼                 │
│   ┌─────────┐       ┌────────┐       ┌─────────┐             │
│   │Postgres │       │ Kafka  │                            │
│   │   DB    │       │Producer│                            │
│   └─────────┘       └────────┘                            │
└──────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│      MCP Brain (Python/FastAPI) :8000                        │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Decision Engine (Claude AI)                            │  │
│  │  • Analyze exception type                              │  │
│  │  • Check retry count vs thresholds                     │  │
│  │  • Apply business rules                                │  │
│  │  • Return RETRY/DLQ/PENDING decision                   │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                      │
                      ▼
         ┌───────────────────────────────┐
         │  Retry Topic / DLQ Topic      │
         │ (for downstream processing)   │
         └───────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Orchestration API** | Go (Gin) | 1.25.6 |
| **AI Decision Engine** | Python (FastAPI + MCP) | 3.11+ |
| **Message Queue** | Apache Kafka | 7.5.0 |
| **Database** | PostgreSQL | 15 |
| **Migrations** | Flyway | 10 |
| **Monitoring** | Kafka UI | latest |

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Go 1.25+
- PostgreSQL 15 (via Docker)

### Local Development

```bash
# 1. Clone and navigate to project
cd /Users/nithinraaj/Documents/Projects/GoLang/KafkaFailureOrchestrator

# 2. Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install Python dependencies
pip install mcp requests fastapi uvicorn

# 4. Start all services with Docker Compose
docker-compose up -d --build

# 5. Verify services are running
docker-compose ps
```

### Initial Setup

```bash
# 1. Wait for Postgres to be ready
docker-compose exec postgres pg_isready -U orchestrator

# 2. Run Flyway migrations (automatic via Docker Compose)
docker-compose exec postgres psql -U orchestrator -d orchestrator_db -c "\dt"

# 3. Verify Kafka is ready
docker-compose exec kafka bash -lc 'kafka-topics --bootstrap-server localhost:9092 --list'

# 4. Run the app components only if you are not using the Compose services
#    (the compose file already starts ingestion-api and mcp-brain).
#    To run them locally, stop the matching containers first, then:
# cd ingestion-api
# go mod download
# go run main.go
#
# cd mcp-brain
# python server.py
```

---

## 📡 API Endpoints

### Ingestion API (Port 8080)

#### 1. Register Failed Event

```json
POST /failures
Content-Type: application/json

{
  "event_id": "evt_001",
  "topic": "orders",
  "partition_id": 0,
  "offset_id": 12345,
  "consumer_name": "order-processor",
  "exception_type": "TimeoutException",
  "error_message": "Connection timeout after 30s",
  "status": "FAILED",
  "original_payload": "{...}"
}

Response: 201 Created
{
  "id": "evt_001",
  "status": "registered",
  "message": "Event saved to DB and published to Kafka"
}
```

#### 2. Get Failure Context

```json
GET /tools/failures/:eventId

Response: 200 OK
{
  "event": {
    "event_id": "evt_001",
    "topic": "orders",
    "exception_type": "TimeoutException",
    "error_message": "Connection timeout after 30s"
  },
  "retry_count": 2,
  "decision_history": [
    {
      "decision": "RETRY",
      "reason": "Transient network failure",
      "decided_at": "2026-01-27T10:30:00Z"
    }
  ]
}
```

#### 3. Execute Decision

```json
POST /tools/decisions
Content-Type: application/json

{
  "event_id": "evt_001",
  "decision": "RETRY",
  "reason": "Transient network failure detected. Attempting recovery."
}

Response: 200 OK
{
  "status": "Decision recorded and action triggered",
  "action": "RETRY"
}
```

#### 4. Health Check

```json
GET /health

Response: 200 OK
{
  "status": "ready",
  "mcp_enabled": true
}
```

### MCP Brain API (Port 8000)

#### 1. Trigger Failure Analysis

```json
POST /tools/handle_failure_event
Content-Type: application/json

{
  "event_id": "evt_001"
}

Response: 202 Accepted
{
  "status": "accepted",
  "message": "Brain is now analyzing evt_001 in the background.",
  "event_id": "evt_001"
}
```

#### 2. Brain Health Check

```json
GET /health

Response: 200 OK
{
  "status": "brain_active",
  "mcp_version": "1.0.0"
}
```

---

## 📨 Kafka Topics

### Topic Configuration

| Topic Name | Partitions | Replication | Purpose |
|-----------|-----------|-------------|---------|
| **failed-events-topic** | 3+ | 1+ | Primary failure ingestion topic |
| **retry-events-topic** | 3+ | 1+ | Retry queue for transient failures |
| **failed-events-dlq** | 3+ | 1+ | Dead Letter Queue for poison pills |

### Topic Message Schema

#### failed-events-topic

```json
{
  "event_id": "evt_001",
  "topic": "orders",
  "partition_id": 0,
  "offset_id": 12345,
  "consumer_name": "order-processor",
  "exception_type": "TimeoutException",
  "error_message": "Connection timeout after 30s",
  "status": "FAILED",
  "original_payload": "{...}",
  "timestamp": "2026-01-27T10:30:00Z"
}
```

#### retry-events-topic

```json
{
  "event_id": "evt_001",
  "original_payload": "{...}",
  "retry_attempt": 2,
  "backoff_ms": 5000,
  "scheduled_for": "2026-01-27T10:35:00Z"
}
```

#### failed-events-dlq

```json
{
  "event_id": "evt_001",
  "reason": "CRITICAL_LOGIC_ERROR: NullPointerException is a code bug",
  "exception_type": "NullPointerException",
  "original_topic": "orders",
  "requires_developer_fix": true
}
```

---

## 🗄️ Database Schema

### PostgreSQL: orchestrator_db

#### Table: failed_events

```sql
CREATE TABLE failed_events (
    event_id           VARCHAR(100) PRIMARY KEY,
    topic              VARCHAR(100) NOT NULL,
    partition_id       INT NOT NULL,
    offset_id          BIGINT NOT NULL,
    consumer_name      VARCHAR(100) NOT NULL,
    exception_type     VARCHAR(150) NOT NULL,
    error_message      TEXT,
    status             VARCHAR(30) NOT NULL,
    original_payload   TEXT,
    first_failed_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_failed_events_status ON failed_events(status);
CREATE INDEX idx_failed_events_exception ON failed_events(exception_type);
```

**Purpose**: Stores all failed event records with complete metadata

#### Table: retry_history

```sql
CREATE TABLE retry_history (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) REFERENCES failed_events(event_id),
    retry_attempt INT NOT NULL,
    retry_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decision_source VARCHAR(50) NOT NULL
);
```

**Purpose**: Audit trail of all retry attempts

#### Table: decision_audit

```sql
CREATE TABLE decision_audit (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) REFERENCES failed_events(event_id),
    decision VARCHAR(50) NOT NULL,
    reason TEXT,
    decided_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

**Purpose**: Complete history of all decisions (RETRY/DLQ/PENDING)

---

## ⚙️ Configuration

### Environment Variables

#### Go API (.env or docker-compose.yml)

```bash
# Database
DB_URL=postgres://orchestrator:orchestrator@postgres:5432/orchestrator_db?sslmode=disable

# Kafka (Docker internal)
KAFKA_BROKERS=kafka:29092

# MCP Brain URL
BRAIN_URL=http://mcp-brain:8000

# Server Port
PORT=8080
```

#### MCP Brain (.env or docker-compose.yml)

```bash
# Go API URL (Docker internal)
GO_API_URL=http://ingestion-api:8080

# Server Port
MCP_PORT=8000

# Claude AI (Optional)
ANTHROPIC_API_KEY=your_key_here
```

---

## 🐳 Deployment

### Docker Compose (Recommended)

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f ingestion-api
docker-compose logs -f mcp-brain

# Stop services
docker-compose down

# Clean slate (remove volumes)
docker-compose down -v
```

### Services in docker-compose.yml

- **Zookeeper** (2181): Kafka coordination
- **Kafka** (9092): Message broker
- **PostgreSQL** (5433): Main database
- **Flyway**: Database migrations
- **Kafka UI** (8085): Kafka monitoring dashboard
- **Ingestion API** (8080): Go service
- **MCP Brain** (8000): Python decision engine

---

## 📊 Monitoring & Debugging

### View Logs

```bash
# Follow logs
docker-compose logs -f ingestion-api
docker-compose logs -f mcp-brain

# View with timestamps
docker-compose logs -f --timestamps ingestion-api
```

### Database Queries

```bash
# Connect to Postgres
docker-compose exec postgres psql -U orchestrator -d orchestrator_db

# Useful queries
SELECT * FROM failed_events WHERE status = 'FAILED' LIMIT 10;
SELECT * FROM decision_audit WHERE event_id = 'evt_001';
SELECT exception_type, COUNT(*) as count FROM failed_events GROUP BY exception_type;
```

### Kafka Monitoring

```bash
# List topics
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
docker-compose exec kafka kafka-topics.sh --describe --topic failed-events-topic --bootstrap-server localhost:9092

# Consume messages
docker-compose exec kafka kafka-console-consumer.sh --topic failed-events-topic --from-beginning --max-messages 10 --bootstrap-server localhost:9092
```

#### Kafka UI Dashboard

Access at `http://localhost:8085`

---

## 🧠 Decision Engine

### Priority-Based Decision Tree

The MCP Brain applies these rules **in order**:

#### Rule 1: Max Retries Exceeded

```
IF retry_count >= 3:
  ✓ DECISION: DLQ
  ✓ REASON: "MAX_RETRIES_EXCEEDED: Already attempted N times. Quarantining."
```

#### Rule 2: Poison Pills (Logic Errors)

```
IF exception_type IN [NullPointerException, ValidationError, SyntaxError, IndexOutOfBoundsException]:
  ✓ DECISION: DLQ
  ✓ REASON: "CRITICAL_LOGIC_ERROR: {exception_type} is a code bug."
```

#### Rule 3: Transient Network Issues

```
IF exception_type IN [TimeoutException, ConnectionException, NetworkException, BrokerException]:
  ✓ DECISION: RETRY
  ✓ REASON: "TRANSIENT_FAILURE: {exception_type} detected. Attempting recovery."
```

#### Rule 4: Database Contention

```
IF "deadlock" IN error_message OR "database" IN exception_type:
  ✓ DECISION: RETRY
  ✓ REASON: "RESOURCE_CONTENTION: Database deadlock detected. Retrying with backoff."
```

#### Rule 5: Unknown Errors

```
ELSE:
  ✓ DECISION: PENDING
  ✓ REASON: "UNKNOWN_EXCEPTION: No rule for {exception_type}. Escalating for human review."
```

### Retry Strategy - Current Behavior

```
Attempt 1: Brain returns RETRY
Attempt 2: Go logs retry history and publishes to retry-events-topic
Attempt 3: Healer consumes retry event and marks the failure RESOLVED
Attempt 4: If the brain decides DLQ, Go publishes to failed-events-dlq and marks QUARANTINED
```

---

## 🔄 Message Flow

```
1. Consumer Processing
   └─> Encounters Exception
       └─> Publishes to "failed-events-topic"

2. Ingestion API
   └─> POST /failures
       └─> Saves to PostgreSQL
           └─> Publishes to Kafka
               └─> Returns 201 Created

3. MCP Brain
   └─> GET /tools/failures/:eventId
       └─> Analyzes Exception
           └─> Applies Decision Tree
               └─> POST /tools/decisions

4. Executor (Go API)
   └─> Processes Decision
       └─> DLQ: Send to failed-events-dlq
       └─> RETRY: Send to retry-events-topic
       └─> PENDING: Mark for human review

5. Recovery
   └─> Retry Topic Consumer
       └─> Marks the failure RESOLVED after the retry event is consumed
           └─> Current implementation simulates recovery rather than republishing to the original topic
```

---

## 🧪 Testing

### Test a Failure Event

```bash
# Register a failure
curl -X POST http://localhost:8080/failures \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_test_001",
    "topic": "orders",
    "partition_id": 0,
    "offset_id": 12345,
    "consumer_name": "test-consumer",
    "exception_type": "TimeoutException",
    "error_message": "Connection timeout after 30s",
    "status": "FAILED",
    "original_payload": "{}"
  }'

# Get failure context
curl http://localhost:8080/tools/failures/evt_test_001

# Check decision
curl -X POST http://localhost:8080/tools/decisions \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_test_001",
    "decision": "RETRY",
    "reason": "Test retry"
  }'
```

---

## 🐛 Troubleshooting

### Port Already in Use

```bash
lsof -i :8080
kill -9 <PID>
```

### Postgres Connection Failed

```bash
docker-compose restart postgres
docker-compose logs postgres
```

### Go Packages Not Found

```bash
cd ingestion-api
go mod download
go run main.go
```
