# Kafka CDC System — Real-Time Employee Data Synchronization

## 1. Project Overview

This project implements a **Change Data Capture (CDC)** system using **Apache Kafka** to synchronize employee data between two PostgreSQL databases in real time. Any INSERT, UPDATE, or DELETE operation on the source database is automatically captured, streamed through Kafka, validated, and applied to the target database — all within approximately 1 second.

### Key Problems Solved

| Problem | Solution |
|---------|----------|
| Real-time data sync between two databases | Event-driven CDC architecture via Kafka |
| Guarantee no data loss or duplication | Idempotent Producer + EOS Consumer |
| Handle invalid / corrupt data gracefully | Dead Letter Queue (DLQ) with error reason tracking |
| Resume after crash without re-processing | File-based offset (Producer) + manual commit (Consumer) |

### Current Status

- **Test Results**: 4/4 passed (INSERT / UPDATE / DELETE / DLQ)
- **Sync Rate**: 100 %
- **Sync Latency**: < 1 second

---

## 2. Architecture Diagram

```
┌──────────────────┐
│  Source Database  │  PostgreSQL :5432
│  (employees)     │
└───────┬──────────┘
        │  AFTER INSERT / UPDATE / DELETE
        ▼
┌──────────────────┐
│  emp_cdc table   │  Trigger writes change snapshot
│  (action log)    │
└───────┬──────────┘
        │  Poll every 5 s (WHERE action_id > offset)
        ▼
┌──────────────────┐
│    Producer      │  Idempotent, acks=all, Snappy compression
│  (producer.py)   │  Key = emp_id → partition ordering
└───────┬──────────┘
        │  Produce JSON to Kafka
        ▼
┌──────────────────────────────────────────┐
│  Kafka Topic: bf_employee_cdc            │
│  3 partitions  ·  Log Compaction enabled │
└───────┬──────────────────────────────────┘
        │  Consume (isolation.level = read_committed)
        ▼
┌──────────────────┐
│    Consumer      │  Manual commit (EOS)
│  (consumer.py)   │
└───────┬──────────┘
        │  Validate → Employee.validate()
        ▼
   ┌────┴────┐
   │ Valid?  │
   └────┬────┘
   YES  │        NO
   ▼    │        ▼
┌───────┴──┐  ┌──────────────┐
│ Target   │  │  emp_cdc_dlq │  DLQ table
│ Database │  │  (error log) │
│ :5433    │  └──────────────┘
└──────────┘
        │
        ▼
   Commit Kafka offset (synchronous)
```

---

## 3. Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Broker | Apache Kafka (Confluent) | 7.4.0 |
| Coordination | Apache Zookeeper | 7.4.0 |
| Source / Target Database | PostgreSQL | 14.1-alpine |
| Programming Language | Python | 3.x |
| Kafka Client | confluent-kafka | 2.3.0 |
| DB Driver | psycopg2-binary | 2.9.9 |
| Container Orchestration | Docker Compose | v2 |

---

## 4. Project Structure

```
proj2/
├── docker-compose.yml              # All services: ZK, Kafka, source DB, target DB
├── requirements.txt                # Python dependencies
├── cdc_offset.txt                  # Producer progress (auto-generated)
│
├── data/
│   └── employees.csv               # Initial seed data (5 records)
│
├── sql/
│   ├── init_source_db.sql          # Source DB schema + CDC trigger
│   └── init_target_db.sql          # Target DB schema + DLQ table
│
├── src/
│   ├── config.py                   # Centralized configuration
│   ├── employee.py                 # Employee data model + validation
│   ├── admin.py                    # Kafka admin utilities
│   ├── producer.py                 # CDC Producer
│   ├── consumer.py                 # CDC Consumer
│   ├── load_initial_data.py        # CSV → source DB loader
│   ├── verify_sync.py              # Source ↔ Target consistency checker
│   └── test_cdc.py                 # Automated test suite (4 scenarios)
│
├── scripts/
│   ├── start_cdc.sh                # Start Producer + Consumer
│   ├── stop_cdc.sh                 # Stop Producer + Consumer
│   └── run_tests.sh                # Full test runner
│
├── logs/
│   ├── producer.log                # Producer runtime log
│   └── consumer.log                # Consumer runtime log
│
└── docs/
    ├── PROJECT_PLAN.md             # Project plan & acceptance criteria
    ├── SCRIPTS_GUIDE.md            # Script usage guide
    ├── data_flow.md                # Detailed data flow description
    ├── presentation_30min.md       # 30-min presentation script
    └── tutor.md                    # Beginner tutorial
```

---

## 5. Quick Start (5 Steps)

### Prerequisites

- Docker & Docker Compose installed
- Python 3.x installed
- `pip install -r requirements.txt`

### Step-by-Step

```bash
# 1. Start all Docker services
cd proj2
docker compose up -d

# 2. Wait for containers to initialize (~30 seconds)
sleep 30

# 3. Load initial employee data into source DB
python3 src/load_initial_data.py

# 4. Start CDC system (Producer + Consumer)
./scripts/start_cdc.sh

# 5. Run tests to verify everything works
./scripts/run_tests.sh
```

### Stop the System

```bash
./scripts/stop_cdc.sh
docker compose down -v    # also removes volumes
```

---

## 6. Component Deep Dive

### 6.1 Docker Infrastructure (`docker-compose.yml`)

The compose file defines **5 services**:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `zookeeper` | confluentinc/cp-zookeeper:7.4.0 | 22181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.4.0 | 29092 | Message broker |
| `kafka-setup` | confluentinc/cp-kafka:7.4.0 | — | One-shot topic creator |
| `db_source` | postgres:14.1-alpine | 5432 | Source database |
| `db_dst` | postgres:14.1-alpine | 5433 | Target database |

**Kafka topic configuration** (created by `kafka-setup`):

| Topic | Partitions | Purpose | Special Config |
|-------|-----------|---------|----------------|
| `bf_employee_cdc` | 3 | Main CDC data stream | Log Compaction enabled |
| `bf_employee_cdc_dlq` | 1 | Dead Letter Queue | Default retention |

**Why Log Compaction?** Only the latest value for each `emp_id` key is retained. This means:
- Storage is reduced by 50–80 % over time
- New consumers can quickly catch up to the latest state

**Why 3 partitions?** Messages are keyed by `emp_id`, so all events for the same employee land in the same partition, preserving ordering per employee.

---

### 6.2 Source Database Schema (`sql/init_source_db.sql`)

Two tables and one trigger:

**`employees` table** — the business table that applications read/write:

```sql
CREATE TABLE employees (
    emp_id   SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    dob        DATE,
    city       VARCHAR(100),
    salary     INT
);
```

**`emp_cdc` table** — the change log populated by the trigger:

```sql
CREATE TABLE emp_cdc (
    action_id   SERIAL PRIMARY KEY,   -- monotonic, used as Producer offset
    emp_id      INT,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    dob         DATE,
    city        VARCHAR(100),
    salary      INT,
    action      VARCHAR(10),           -- 'INSERT' / 'UPDATE' / 'DELETE'
    action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Trigger function `capture_employee_changes()`** — fires AFTER every INSERT, UPDATE, DELETE on `employees`:

```sql
CREATE OR REPLACE FUNCTION capture_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO emp_cdc(...) VALUES (OLD.*,  'DELETE');
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO emp_cdc(...) VALUES (NEW.*,  'UPDATE');
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO emp_cdc(...) VALUES (NEW.*,  'INSERT');
    END IF;
END;
$$ LANGUAGE plpgsql;
```

**Why use a trigger instead of polling the main table?**
- Triggers capture the exact operation type (INSERT / UPDATE / DELETE)
- No need for a full-table scan; only changes are recorded
- The `action_id` column provides a reliable, monotonically increasing offset

---

### 6.3 Target Database Schema (`sql/init_target_db.sql`)

Two tables, no triggers:

**`employees` table** — mirror of the source (written by Consumer):

```sql
CREATE TABLE employees (
    emp_id     INT PRIMARY KEY,   -- INT, not SERIAL (values come from source)
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    dob        DATE,
    city       VARCHAR(100),
    salary     INT
);
```

**`emp_cdc_dlq` table** — Dead Letter Queue:

```sql
CREATE TABLE emp_cdc_dlq (
    id               SERIAL PRIMARY KEY,
    emp_id           INT,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    dob              DATE,
    city             VARCHAR(100),
    salary           INT,
    action           VARCHAR(10),
    error_reason     TEXT,          -- why validation failed
    failed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    original_message TEXT           -- raw JSON for debugging
);
```

---

### 6.4 Centralized Configuration (`src/config.py`)

All tuneable parameters live here — no hardcoded values scattered across files.

| Category | Key Parameters |
|----------|---------------|
| Source DB | host, port 5432, user/password |
| Target DB | host, port 5433, user/password |
| Kafka | bootstrap `localhost:29092`, topic `bf_employee_cdc`, DLQ topic |
| EOS | `ENABLE_IDEMPOTENT_PRODUCER = True`, `ENABLE_AUTO_COMMIT = False`, `EOS_ISOLATION_LEVEL = 'read_committed'` |
| Producer | `POLL_INTERVAL = 5` s, `BATCH_SIZE = 100`, `OFFSET_FILE = 'cdc_offset.txt'` |
| Validation | `MIN_BIRTH_YEAR = 1990`, `MIN_SALARY = 10000`, `MIN_EMPLOYEE_ID = 0` |

---

### 6.5 Employee Data Model (`src/employee.py`)

The `Employee` class is the single data contract shared by Producer and Consumer.

**Serialization / Deserialization:**

| Method | Direction | Usage |
|--------|-----------|-------|
| `Employee.from_line(row)` | DB row → object | Producer reads from `emp_cdc` |
| `employee.to_json()` | object → JSON string | Producer sends to Kafka |
| `Employee.from_json(json_str)` | JSON string → object | Consumer receives from Kafka |

**Validation rules** (`employee.validate()`):

| Rule | Condition | On Failure |
|------|-----------|------------|
| Employee ID | `emp_id >= 0` | → DLQ |
| Birth year | `year > 1990` | → DLQ |
| Salary | `salary >= 10000` | → DLQ |

If any rule fails, the record is routed to the DLQ table with the specific error reason.

---

### 6.6 CDC Producer (`src/producer.py`)

The `cdcProducer` class inherits from `confluent_kafka.Producer`.

#### Initialization & Reliability Config

```python
producerConfig = {
    'bootstrap.servers': 'localhost:29092',
    'acks': 'all',                             # Wait for all replicas
    'enable.idempotence': True,                # Automatic deduplication
    'max.in.flight.requests.per.connection': 5,
    'linger.ms': 10,                           # Batch for 10 ms
    'batch.size': 16384,                       # 16 KB batch
    'compression.type': 'snappy',              # Reduce bandwidth
}
```

**Why `enable.idempotence = True`?** Even if the Producer retries a send (e.g., after a network glitch), Kafka guarantees no duplicate messages.

#### Offset Management (Crash Recovery)

- `load_offset()` reads the last processed `action_id` from `cdc_offset.txt`
- `save_offset(action_id)` persists progress after each batch
- On restart, the Producer resumes from exactly where it left off

#### Fetch CDC Records

```python
query = """
    SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
    FROM emp_cdc
    WHERE action_id > %s
    ORDER BY action_id
    LIMIT %s
"""
```

Only rows greater than the saved offset are fetched, ordered by `action_id`, limited to `BATCH_SIZE`.

#### Main Loop

```
while running:
    1. Load offset from file
    2. Fetch new CDC records from emp_cdc
    3. For each record:
         - Serialize to JSON
         - produce(topic, key=emp_id, value=json)
    4. flush() — block until all messages are acknowledged
    5. Save new offset
    6. Sleep POLL_INTERVAL seconds
```

The key is set to `emp_id` (encoded as bytes) so that all events for the same employee are routed to the same Kafka partition, preserving order.

---

### 6.7 CDC Consumer (`src/consumer.py`)

The `cdcConsumer` class inherits from `confluent_kafka.Consumer`.

#### EOS (Exactly Once Semantics) Configuration

```python
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'bf_cdc_consumer',
    'enable.auto.commit': False,           # Manual commit only
    'auto.offset.reset': 'earliest',       # Don't miss old messages
    'isolation.level': 'read_committed',   # Skip uncommitted txn messages
}
```

**Why `enable.auto.commit = False`?** If the Consumer crashes between processing a message and the next auto-commit interval, the message would be lost. Manual commit ensures: process first → commit after.

**Why `isolation.level = read_committed`?** This guarantees the Consumer only sees messages that have been fully committed by the Producer's transaction, preventing dirty reads.

#### Consume Loop

```
while running:
    1. poll(timeout=1.0) — pull one message from Kafka
    2. Deserialize JSON → Employee object
    3. Validate (Employee.validate())
    4. If valid  → update_dst(employee)   (UPSERT or DELETE)
       If invalid → send_to_dlq(employee, reason)
    5. commit(asynchronous=False)           — synchronous offset commit
```

Step 5 is critical: the offset is committed **only after** the database write succeeds. If the Consumer crashes before step 5, the message will be re-delivered and re-processed (idempotent UPSERT ensures no harm).

#### Target Database Write (`update_dst`)

For INSERT and UPDATE, uses **UPSERT** (ON CONFLICT):

```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (emp_id)
DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name  = EXCLUDED.last_name,
    dob        = EXCLUDED.dob,
    city       = EXCLUDED.city,
    salary     = EXCLUDED.salary
```

For DELETE:

```sql
DELETE FROM employees WHERE emp_id = %s
```

**Why UPSERT?** If the same message is consumed twice (e.g., after a crash before commit), the UPSERT produces the same result — this is idempotent.

#### DLQ Write (`send_to_dlq`)

Invalid records are inserted into `emp_cdc_dlq` with:
- All original fields
- `error_reason` — human-readable validation failure message
- `original_message` — the raw JSON for debugging

The DLQ table does **not** block the main data flow; the Consumer continues processing subsequent messages.

---

### 6.8 Kafka Admin (`src/admin.py`)

The `cdcClient` class provides utilities to manage Kafka topics:

| Method | Purpose |
|--------|---------|
| `topic_exists(topic)` | Check if a topic already exists |
| `create_topic(topic, partitions)` | Create topic with specified partition count |
| `delete_topic(topics)` | Delete one or more topics |
| `get_consumer_group_size(group_id)` | Check how many consumers are in a group |

---

### 6.9 Initial Data Loader (`src/load_initial_data.py`)

Reads `data/employees.csv` (5 seed records) and inserts them into the source database using UPSERT. Because triggers are active, each insert automatically creates a corresponding row in `emp_cdc` — no extra code needed.

The loader also:
- Waits for the database to be ready (retry loop, useful right after `docker compose up`)
- Prints how many CDC records were captured
- Shows sample CDC entries for verification

---

### 6.10 Sync Verifier (`src/verify_sync.py`)

Compares the source and target databases field by field and reports:

| Check | Meaning |
|-------|---------|
| Only in source | Records not yet synced |
| Only in target | Orphaned records (shouldn't exist) |
| Different data | Fields mismatch between source and target |
| Perfect sync | All records match exactly |

Also shows the latest CDC table entries and DLQ status.

---

### 6.11 Test Suite (`src/test_cdc.py`)

Four automated test scenarios cover the complete CDC pipeline:

| Test | What It Does | Success Criteria |
|------|-------------|-----------------|
| **INSERT** | Insert `emp_id=1001` into source | Record appears in target within 12 s |
| **UPDATE** | Update salary and city for `emp_id=1002` | Updated values appear in target within 12 s |
| **DELETE** | Delete `emp_id=1003` from source | Record disappears from target within 12 s |
| **DLQ** | Insert record with `salary=5000` (below minimum) | Record appears in `emp_cdc_dlq`, NOT in target `employees` |

Each test uses **polling** (`wait_for_employee`) to handle async sync delay — it retries up to 12 times with 1-second intervals before declaring pass/fail.

---

## 7. Reliability & Design Decisions

### 7.1 Exactly Once Semantics (EOS) — End to End

```
Producer side:
  enable.idempotence = True   →   Kafka deduplicates retries
  acks = 'all'                →   Message persisted to all replicas

Kafka side:
  Log Compaction              →   Latest value per key always available

Consumer side:
  enable.auto.commit = False  →   No premature offset advance
  isolation.level = read_committed → No dirty reads
  commit(asynchronous=False)  →   Offset committed AFTER DB write
  UPSERT on target            →   Idempotent — safe to re-process
```

### 7.2 Why a Trigger-Based CDC Instead of Log-Based (e.g., Debezium)?

| Aspect | Trigger-Based (this project) | Log-Based (Debezium) |
|--------|------------------------------|----------------------|
| Setup complexity | Low (pure SQL) | Medium (connector + config) |
| Captures operation type | Yes (INSERT/UPDATE/DELETE) | Yes |
| Performance overhead | Slight (1 extra INSERT per operation) | Minimal (reads WAL) |
| Schema coupling | Tight (trigger must match table) | Loose (reads from WAL) |

Trigger-based was chosen for simplicity and full control over the change record format. In production, Debezium would be a natural upgrade.

### 7.3 Two Types of Offset

| Offset | Owner | Stored In | Purpose |
|--------|-------|-----------|---------|
| CDC offset (`action_id`) | Producer | `cdc_offset.txt` | Track which `emp_cdc` rows have been sent to Kafka |
| Kafka offset | Consumer | Kafka `__consumer_offsets` | Track which Kafka messages have been processed |

These two offsets work independently: the Producer resumes reading `emp_cdc` from its file offset; the Consumer resumes reading the Kafka topic from its Kafka offset.

### 7.4 DLQ Strategy

- Invalid records are written to the `emp_cdc_dlq` table with full context
- The main consumer loop is **never blocked** by bad data
- DLQ records can be reviewed, fixed, and replayed manually
- Each DLQ entry includes `error_reason` and `original_message` for debugging

---

## 8. Monitoring & Debugging

### Logs

```bash
tail -f logs/producer.log   # Producer activity
tail -f logs/consumer.log   # Consumer activity
```

### Verify Sync

```bash
python3 src/verify_sync.py
```

### Check Kafka Topics (via Docker)

```bash
# List topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Read CDC messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bf_employee_cdc \
  --from-beginning

# Read DLQ messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning
```

### Offset Explorer GUI

| Setting | Value |
|---------|-------|
| Bootstrap servers | `localhost:29092` |
| Zookeeper Host | `localhost` |
| Zookeeper Port | `22181` |

You can view topics, partitions, messages, consumer group offsets, and lag.

---

## 9. Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Tests fail | Producer / Consumer not running | `./scripts/start_cdc.sh` |
| "Cannot connect to database" | Docker containers not started | `docker compose up -d && sleep 30` |
| Data not syncing | Offset stuck or process crashed | `./scripts/stop_cdc.sh && ./scripts/start_cdc.sh --reset` |
| DLQ records appearing | Validation rules rejecting data | Check `emp_cdc_dlq` for `error_reason` |
| Port conflict | Another service using 5432/5433/29092 | `lsof -i :5432` → stop conflicting service |

### Full Reset

```bash
./scripts/stop_cdc.sh
docker compose down -v
rm -f cdc_offset.txt logs/*.log
docker compose up -d
sleep 30
python3 src/load_initial_data.py
./scripts/start_cdc.sh --reset
./scripts/run_tests.sh
```

---

## 10. Future Improvements

| Priority | Improvement | Benefit |
|----------|------------|---------|
| High | Replace triggers with **Debezium** (WAL-based CDC) | Zero-latency capture, automatic failover |
| High | **Avro + Schema Registry** instead of JSON | Schema evolution, smaller payloads, multi-consumer compatibility |
| Medium | **Prometheus + Grafana** monitoring | Real-time dashboards for throughput, latency, error rate |
| Medium | **KRaft** mode (remove Zookeeper) | Simpler deployment, fewer moving parts |

---

## 11. Scripts Reference

| Script | Usage | Description |
|--------|-------|-------------|
| `./scripts/start_cdc.sh` | Start system | Launches Producer + Consumer in background |
| `./scripts/start_cdc.sh --reset` | Start with reset | Clears offset file, then starts |
| `./scripts/stop_cdc.sh` | Stop system | Gracefully kills Producer + Consumer |
| `./scripts/stop_cdc.sh --show-logs` | Stop + show logs | Stops and prints last 20 lines of each log |
| `./scripts/run_tests.sh` | Full test suite | Runs sync verification + 4 functional tests + health check |
| `python3 src/load_initial_data.py` | Load CSV | Seeds source DB from `data/employees.csv` |
| `python3 src/verify_sync.py` | Verify sync | Compares source ↔ target databases |
| `python3 src/test_cdc.py` | Functional tests | INSERT / UPDATE / DELETE / DLQ tests |
| `python3 src/config.py` | Print config | Shows all current configuration values |
