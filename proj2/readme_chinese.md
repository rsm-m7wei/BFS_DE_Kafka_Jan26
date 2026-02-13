# Kafka CDC 系统 — 员工数据实时同步

## 1. 项目概述

本项目使用 **Apache Kafka** 实现了一套 **变更数据捕获（CDC）** 系统，在两个 PostgreSQL 数据库之间实时同步员工数据。源数据库上的任何 INSERT、UPDATE 或 DELETE 操作都会被自动捕获，通过 Kafka 传输、验证后写入目标数据库——整个过程延迟约 1 秒。

### 解决的核心问题

| 问题 | 解决方案 |
|------|----------|
| 两个数据库之间的实时数据同步 | 基于 Kafka 的事件驱动 CDC 架构 |
| 保证数据不丢失、不重复 | 幂等 Producer + EOS Consumer |
| 优雅处理无效/异常数据 | 死信队列（DLQ）+ 错误原因记录 |
| 崩溃后恢复不需要重新处理 | 基于文件的偏移量（Producer）+ 手动提交（Consumer） |

### 当前状态

- **测试结果**：4/4 通过（INSERT / UPDATE / DELETE / DLQ）
- **同步率**：100%
- **同步延迟**：< 1 秒

---

## 2. 架构图

```
┌──────────────────┐
│    源数据库       │  PostgreSQL :5432
│  (employees)     │
└───────┬──────────┘
        │  AFTER INSERT / UPDATE / DELETE
        ▼
┌──────────────────┐
│  emp_cdc 表      │  触发器写入变更快照
│  (变更日志)       │
└───────┬──────────┘
        │  每 5 秒轮询 (WHERE action_id > offset)
        ▼
┌──────────────────┐
│    Producer      │  幂等, acks=all, Snappy 压缩
│  (producer.py)   │  Key = emp_id → 分区有序
└───────┬──────────┘
        │  生产 JSON 到 Kafka
        ▼
┌──────────────────────────────────────────┐
│  Kafka Topic: bf_employee_cdc            │
│  3 个分区  ·  日志压缩（Log Compaction）   │
└───────┬──────────────────────────────────┘
        │  消费 (isolation.level = read_committed)
        ▼
┌──────────────────┐
│    Consumer      │  手动提交（EOS）
│  (consumer.py)   │
└───────┬──────────┘
        │  验证 → Employee.validate()
        ▼
   ┌────┴────┐
   │ 验证通过？│
   └────┬────┘
   是    │        否
   ▼    │        ▼
┌───────┴──┐  ┌──────────────┐
│ 目标数据库│  │  emp_cdc_dlq │  死信队列表
│ :5433    │  │  (错误记录)   │
└──────────┘  └──────────────┘
        │
        ▼
   提交 Kafka 偏移量（同步提交）
```

---

## 3. 技术栈

| 组件 | 技术 | 版本 |
|------|------|------|
| 消息中间件 | Apache Kafka (Confluent) | 7.4.0 |
| 协调服务 | Apache Zookeeper | 7.4.0 |
| 源/目标数据库 | PostgreSQL | 14.1-alpine |
| 编程语言 | Python | 3.x |
| Kafka 客户端 | confluent-kafka | 2.3.0 |
| 数据库驱动 | psycopg2-binary | 2.9.9 |
| 容器编排 | Docker Compose | v2 |

---

## 4. 项目结构

```
proj2/
├── docker-compose.yml              # 所有服务：ZK、Kafka、源 DB、目标 DB
├── requirements.txt                # Python 依赖
├── cdc_offset.txt                  # Producer 进度（自动生成）
│
├── data/
│   └── employees.csv               # 初始种子数据（5 条记录）
│
├── sql/
│   ├── init_source_db.sql          # 源数据库 Schema + CDC 触发器
│   └── init_target_db.sql          # 目标数据库 Schema + DLQ 表
│
├── src/
│   ├── config.py                   # 集中配置管理
│   ├── employee.py                 # Employee 数据模型 + 验证
│   ├── admin.py                    # Kafka 管理工具
│   ├── producer.py                 # CDC Producer
│   ├── consumer.py                 # CDC Consumer
│   ├── load_initial_data.py        # CSV → 源数据库加载器
│   ├── verify_sync.py              # 源 ↔ 目标 一致性检查
│   └── test_cdc.py                 # 自动化测试套件（4 个场景）
│
├── scripts/
│   ├── start_cdc.sh                # 启动 Producer + Consumer
│   ├── stop_cdc.sh                 # 停止 Producer + Consumer
│   └── run_tests.sh                # 完整测试运行器
│
├── logs/
│   ├── producer.log                # Producer 运行日志
│   └── consumer.log                # Consumer 运行日志
│
└── docs/
    ├── PROJECT_PLAN.md             # 项目计划和验收标准
    ├── SCRIPTS_GUIDE.md            # 脚本使用指南
    ├── data_flow.md                # 详细数据流说明
    ├── presentation_30min.md       # 30 分钟演示脚本
    └── tutor.md                    # 初学者教程
```

---

## 5. 快速启动（5 步）

### 前置条件

- 已安装 Docker 和 Docker Compose
- 已安装 Python 3.x
- `pip install -r requirements.txt`

### 逐步操作

```bash
# 1. 启动所有 Docker 服务
cd proj2
docker compose up -d

# 2. 等待容器初始化（约 30 秒）
sleep 30

# 3. 将初始员工数据加载到源数据库
python3 src/load_initial_data.py

# 4. 启动 CDC 系统（Producer + Consumer）
./scripts/start_cdc.sh

# 5. 运行测试验证一切正常
./scripts/run_tests.sh
```

### 停止系统

```bash
./scripts/stop_cdc.sh
docker compose down -v    # 同时删除数据卷
```

---

## 6. 组件详解

### 6.1 Docker 基础设施（`docker-compose.yml`）

Compose 文件定义了 **5 个服务**：

| 服务 | 镜像 | 端口 | 用途 |
|------|------|------|------|
| `zookeeper` | confluentinc/cp-zookeeper:7.4.0 | 22181 | Kafka 协调服务 |
| `kafka` | confluentinc/cp-kafka:7.4.0 | 29092 | 消息代理 |
| `kafka-setup` | confluentinc/cp-kafka:7.4.0 | — | 一次性 Topic 创建容器 |
| `db_source` | postgres:14.1-alpine | 5432 | 源数据库 |
| `db_dst` | postgres:14.1-alpine | 5433 | 目标数据库 |

**Kafka Topic 配置**（由 `kafka-setup` 创建）：

| Topic | 分区数 | 用途 | 特殊配置 |
|-------|--------|------|----------|
| `bf_employee_cdc` | 3 | 主 CDC 数据流 | 启用日志压缩 |
| `bf_employee_cdc_dlq` | 1 | 死信队列 | 默认保留策略 |

**为什么使用日志压缩（Log Compaction）？** 每个 `emp_id` 键只保留最新值，这意味着：
- 随时间推移存储减少 50–80%
- 新 Consumer 可以快速追上最新状态

**为什么 3 个分区？** 消息以 `emp_id` 为 Key，同一员工的所有事件会路由到同一分区，保证每个员工的事件有序。

---

### 6.2 源数据库 Schema（`sql/init_source_db.sql`）

两张表和一个触发器：

**`employees` 表** — 业务表，应用读写的主表：

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

**`emp_cdc` 表** — 由触发器填充的变更日志表：

```sql
CREATE TABLE emp_cdc (
    action_id   SERIAL PRIMARY KEY,   -- 单调递增，用作 Producer 偏移量
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

**触发器函数 `capture_employee_changes()`** — 在 `employees` 表的每次 INSERT、UPDATE、DELETE 后触发：

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

**为什么用触发器而不是轮询主表？**
- 触发器能精确捕获操作类型（INSERT / UPDATE / DELETE）
- 无需全表扫描，只记录变更
- `action_id` 列提供了可靠的、单调递增的偏移量

---

### 6.3 目标数据库 Schema（`sql/init_target_db.sql`）

两张表，没有触发器：

**`employees` 表** — 源数据库的镜像（由 Consumer 写入）：

```sql
CREATE TABLE employees (
    emp_id     INT PRIMARY KEY,   -- INT，不是 SERIAL（值来自源数据库）
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    dob        DATE,
    city       VARCHAR(100),
    salary     INT
);
```

**`emp_cdc_dlq` 表** — 死信队列：

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
    error_reason     TEXT,          -- 验证失败原因
    failed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    original_message TEXT           -- 原始 JSON，用于调试
);
```

---

### 6.4 集中配置（`src/config.py`）

所有可调参数集中在这里——没有硬编码值散落在各文件中。

| 类别 | 关键参数 |
|------|----------|
| 源数据库 | host, port 5432, user/password |
| 目标数据库 | host, port 5433, user/password |
| Kafka | bootstrap `localhost:29092`, topic `bf_employee_cdc`, DLQ topic |
| EOS | `ENABLE_IDEMPOTENT_PRODUCER = True`, `ENABLE_AUTO_COMMIT = False`, `EOS_ISOLATION_LEVEL = 'read_committed'` |
| Producer | `POLL_INTERVAL = 5` 秒, `BATCH_SIZE = 100`, `OFFSET_FILE = 'cdc_offset.txt'` |
| 验证规则 | `MIN_BIRTH_YEAR = 1990`, `MIN_SALARY = 10000`, `MIN_EMPLOYEE_ID = 0` |

---

### 6.5 Employee 数据模型（`src/employee.py`）

`Employee` 类是 Producer 和 Consumer 共享的唯一数据契约。

**序列化 / 反序列化：**

| 方法 | 方向 | 使用场景 |
|------|------|----------|
| `Employee.from_line(row)` | 数据库行 → 对象 | Producer 从 `emp_cdc` 读取 |
| `employee.to_json()` | 对象 → JSON 字符串 | Producer 发送到 Kafka |
| `Employee.from_json(json_str)` | JSON 字符串 → 对象 | Consumer 从 Kafka 接收 |

**验证规则**（`employee.validate()`）：

| 规则 | 条件 | 失败时 |
|------|------|--------|
| 员工 ID | `emp_id >= 0` | → DLQ |
| 出生年份 | `year > 1990` | → DLQ |
| 薪资 | `salary >= 10000` | → DLQ |

任何规则验证失败，记录会被路由到 DLQ 表，并附带具体的错误原因。

---

### 6.6 CDC Producer（`src/producer.py`）

`cdcProducer` 类继承自 `confluent_kafka.Producer`。

#### 初始化与可靠性配置

```python
producerConfig = {
    'bootstrap.servers': 'localhost:29092',
    'acks': 'all',                             # 等待所有副本确认
    'enable.idempotence': True,                # 自动去重
    'max.in.flight.requests.per.connection': 5,
    'linger.ms': 10,                           # 批量等待 10 ms
    'batch.size': 16384,                       # 16 KB 批量大小
    'compression.type': 'snappy',              # 减少带宽
}
```

**为什么 `enable.idempotence = True`？** 即使 Producer 重试发送（例如网络故障后），Kafka 也能保证不产生重复消息。

#### 偏移量管理（崩溃恢复）

- `load_offset()` 从 `cdc_offset.txt` 读取最后处理的 `action_id`
- `save_offset(action_id)` 每批处理后持久化进度
- 重启时，Producer 从上次中断的位置精确恢复

#### 获取 CDC 记录

```python
query = """
    SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
    FROM emp_cdc
    WHERE action_id > %s
    ORDER BY action_id
    LIMIT %s
"""
```

只获取大于已保存偏移量的行，按 `action_id` 排序，限制为 `BATCH_SIZE` 条。

#### 主循环

```
while running:
    1. 从文件加载偏移量
    2. 从 emp_cdc 获取新的 CDC 记录
    3. 对每条记录：
         - 序列化为 JSON
         - produce(topic, key=emp_id, value=json)
    4. flush() — 阻塞直到所有消息被确认
    5. 保存新的偏移量
    6. 休眠 POLL_INTERVAL 秒
```

Key 设置为 `emp_id`（编码为 bytes），确保同一员工的所有事件路由到同一 Kafka 分区，保证有序。

---

### 6.7 CDC Consumer（`src/consumer.py`）

`cdcConsumer` 类继承自 `confluent_kafka.Consumer`。

#### EOS（精确一次语义）配置

```python
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'bf_cdc_consumer',
    'enable.auto.commit': False,           # 仅手动提交
    'auto.offset.reset': 'earliest',       # 不遗漏旧消息
    'isolation.level': 'read_committed',   # 跳过未提交的事务消息
}
```

**为什么 `enable.auto.commit = False`？** 如果 Consumer 在处理消息和下一次自动提交之间崩溃，消息会丢失。手动提交确保：先处理 → 再提交。

**为什么 `isolation.level = read_committed`？** 保证 Consumer 只能看到已完全提交的消息，防止脏读。

#### 消费循环

```
while running:
    1. poll(timeout=1.0) — 从 Kafka 拉取一条消息
    2. 反序列化 JSON → Employee 对象
    3. 验证（Employee.validate()）
    4. 如果有效  → update_dst(employee)    （UPSERT 或 DELETE）
       如果无效  → send_to_dlq(employee, reason)
    5. commit(asynchronous=False)            — 同步偏移量提交
```

第 5 步至关重要：偏移量**只在**数据库写入成功后才提交。如果 Consumer 在第 5 步之前崩溃，消息会被重新投递并重新处理（幂等 UPSERT 确保无害）。

#### 目标数据库写入（`update_dst`）

对于 INSERT 和 UPDATE，使用 **UPSERT**（ON CONFLICT）：

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

对于 DELETE：

```sql
DELETE FROM employees WHERE emp_id = %s
```

**为什么用 UPSERT？** 如果同一条消息被消费两次（例如崩溃后提交前），UPSERT 产生相同结果——这就是幂等性。

#### DLQ 写入（`send_to_dlq`）

无效记录被插入 `emp_cdc_dlq` 表，包含：
- 所有原始字段
- `error_reason` — 人类可读的验证失败信息
- `original_message` — 原始 JSON，用于调试

DLQ 表**不会**阻塞主数据流；Consumer 继续处理后续消息。

---

### 6.8 Kafka 管理工具（`src/admin.py`）

`cdcClient` 类提供管理 Kafka Topic 的工具方法：

| 方法 | 用途 |
|------|------|
| `topic_exists(topic)` | 检查 Topic 是否已存在 |
| `create_topic(topic, partitions)` | 创建指定分区数的 Topic |
| `delete_topic(topics)` | 删除一个或多个 Topic |
| `get_consumer_group_size(group_id)` | 检查消费者组中有多少 Consumer |

---

### 6.9 初始数据加载器（`src/load_initial_data.py`）

读取 `data/employees.csv`（5 条种子记录）并使用 UPSERT 插入源数据库。因为触发器已激活，每次插入都会自动在 `emp_cdc` 中创建相应记录——无需额外代码。

加载器还会：
- 等待数据库就绪（重试循环，适用于 `docker compose up` 之后）
- 打印捕获了多少条 CDC 记录
- 显示示例 CDC 条目用于验证

---

### 6.10 同步校验器（`src/verify_sync.py`）

逐字段比较源数据库和目标数据库并报告：

| 检查项 | 含义 |
|--------|------|
| 仅在源库中 | 尚未同步的记录 |
| 仅在目标库中 | 孤立记录（不应存在） |
| 数据不一致 | 源和目标之间字段不匹配 |
| 完美同步 | 所有记录完全匹配 |

还会显示最新的 CDC 表条目和 DLQ 状态。

---

### 6.11 测试套件（`src/test_cdc.py`）

四个自动化测试场景覆盖完整的 CDC 流程：

| 测试 | 操作内容 | 成功标准 |
|------|----------|----------|
| **INSERT** | 向源库插入 `emp_id=1001` | 记录在 12 秒内出现在目标库 |
| **UPDATE** | 更新 `emp_id=1002` 的薪资和城市 | 更新值在 12 秒内出现在目标库 |
| **DELETE** | 从源库删除 `emp_id=1003` | 记录在 12 秒内从目标库消失 |
| **DLQ** | 插入 `salary=5000` 的记录（低于最低值） | 记录出现在 `emp_cdc_dlq`，不在目标库 `employees` 中 |

每个测试使用**轮询**（`wait_for_employee`）来处理异步同步延迟——最多重试 12 次，每次间隔 1 秒，超时则判定失败。

---

## 7. 可靠性与设计决策

### 7.1 精确一次语义（EOS）— 端到端

```
Producer 端：
  enable.idempotence = True   →   Kafka 对重试去重
  acks = 'all'                →   消息持久化到所有副本

Kafka 端：
  Log Compaction              →   每个 Key 的最新值始终可用

Consumer 端：
  enable.auto.commit = False  →   不会提前推进偏移量
  isolation.level = read_committed → 不读取脏数据
  commit(asynchronous=False)  →   偏移量在数据库写入后才提交
  UPSERT 写入目标库            →   幂等——可安全重复处理
```

### 7.2 为什么用触发器 CDC 而不是基于日志的方案（如 Debezium）？

| 方面 | 触发器方案（本项目） | 基于日志方案（Debezium） |
|------|---------------------|------------------------|
| 部署复杂度 | 低（纯 SQL） | 中等（需要 Connector + 配置） |
| 捕获操作类型 | 是（INSERT/UPDATE/DELETE） | 是 |
| 性能开销 | 轻微（每次操作多一个 INSERT） | 极小（读取 WAL） |
| Schema 耦合 | 紧耦合（触发器必须匹配表结构） | 松耦合（从 WAL 读取） |

选择触发器方案是为了简单性和对变更记录格式的完全控制。在生产环境中，Debezium 是一个自然的升级方向。

### 7.3 两种偏移量

| 偏移量 | 所有者 | 存储位置 | 用途 |
|--------|--------|----------|------|
| CDC 偏移量（`action_id`） | Producer | `cdc_offset.txt` | 追踪哪些 `emp_cdc` 行已发送到 Kafka |
| Kafka 偏移量 | Consumer | Kafka `__consumer_offsets` | 追踪哪些 Kafka 消息已被处理 |

这两种偏移量独立工作：Producer 从文件偏移量恢复读取 `emp_cdc`；Consumer 从 Kafka 偏移量恢复读取 Kafka Topic。

### 7.4 DLQ 策略

- 无效记录被写入 `emp_cdc_dlq` 表，包含完整上下文
- 主消费循环**永远不会**被坏数据阻塞
- DLQ 记录可以人工审查、修复后重放
- 每条 DLQ 条目包含 `error_reason` 和 `original_message` 用于调试

---

## 8. 监控与调试

### 日志

```bash
tail -f logs/producer.log   # Producer 活动日志
tail -f logs/consumer.log   # Consumer 活动日志
```

### 验证同步

```bash
python3 src/verify_sync.py
```

### 查看 Kafka Topics（通过 Docker）

```bash
# 列出所有 Topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 读取 CDC 消息
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bf_employee_cdc \
  --from-beginning

# 读取 DLQ 消息
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning
```

### Offset Explorer GUI

| 配置项 | 值 |
|--------|-----|
| Bootstrap servers | `localhost:29092` |
| Zookeeper Host | `localhost` |
| Zookeeper Port | `22181` |

可以查看 Topics、分区、消息内容、Consumer Group 偏移量和延迟（Lag）。

---

## 9. 故障排查

| 现象 | 可能原因 | 解决方法 |
|------|----------|----------|
| 测试失败 | Producer / Consumer 未运行 | `./scripts/start_cdc.sh` |
| "无法连接数据库" | Docker 容器未启动 | `docker compose up -d && sleep 30` |
| 数据未同步 | 偏移量卡住或进程崩溃 | `./scripts/stop_cdc.sh && ./scripts/start_cdc.sh --reset` |
| 出现 DLQ 记录 | 验证规则拒绝了数据 | 检查 `emp_cdc_dlq` 的 `error_reason` |
| 端口冲突 | 其他服务占用了 5432/5433/29092 | `lsof -i :5432` → 停止冲突的服务 |

### 完整重置

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

## 10. 未来改进方向

| 优先级 | 改进 | 收益 |
|--------|------|------|
| 高 | 用 **Debezium** 替换触发器（基于 WAL 的 CDC） | 零延迟捕获、自动故障转移 |
| 高 | 用 **Avro + Schema Registry** 替代 JSON | Schema 演进、更小的消息体、多 Consumer 兼容 |
| 中 | **Prometheus + Grafana** 监控 | 实时仪表盘：吞吐量、延迟、错误率 |
| 中 | **KRaft** 模式（移除 Zookeeper） | 更简单的部署、更少的组件 |

---

## 11. 脚本速查表

| 脚本 | 用法 | 说明 |
|------|------|------|
| `./scripts/start_cdc.sh` | 启动系统 | 后台启动 Producer + Consumer |
| `./scripts/start_cdc.sh --reset` | 重置启动 | 清除偏移量文件后启动 |
| `./scripts/stop_cdc.sh` | 停止系统 | 优雅停止 Producer + Consumer |
| `./scripts/stop_cdc.sh --show-logs` | 停止 + 显示日志 | 停止并打印每个日志的最后 20 行 |
| `./scripts/run_tests.sh` | 完整测试 | 运行同步验证 + 4 个功能测试 + 健康检查 |
| `python3 src/load_initial_data.py` | 加载 CSV | 从 `data/employees.csv` 导入种子数据 |
| `python3 src/verify_sync.py` | 验证同步 | 比较源 ↔ 目标数据库 |
| `python3 src/test_cdc.py` | 功能测试 | INSERT / UPDATE / DELETE / DLQ 测试 |
| `python3 src/config.py` | 打印配置 | 显示当前所有配置值 |
