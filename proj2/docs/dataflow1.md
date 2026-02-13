# 数据流程完全指南 — 从零到同步完成

> 本文档面向完全不了解本项目的人，详细讲解数据从插入源数据库到同步至目标数据库的完整流程，包括每个步骤的具体操作和对应的代码文件。

---

## 目录

1. [系统架构概览](#1-系统架构概览)
2. [阶段 1：基础设施启动](#2-阶段-1基础设施启动)
3. [阶段 2：初始数据加载](#3-阶段-2初始数据加载)
4. [阶段 3：CDC 系统启动](#4-阶段-3cdc-系统启动)
5. [阶段 4：Producer 数据捕获与发送](#5-阶段-4producer-数据捕获与发送)
6. [阶段 5：Consumer 数据消费与同步](#6-阶段-5consumer-数据消费与同步)
7. [阶段 6：数据验证](#7-阶段-6数据验证)
8. [异常流程：DLQ 处理](#8-异常流程dlq-处理)
9. [完整流程图](#9-完整流程图)
10. [关键文件速查表](#10-关键文件速查表)

---

## 1. 系统架构概览

本系统是一个基于 Kafka 的变更数据捕获（CDC）系统，核心组件包括：

```
源数据库 (PostgreSQL)
    ↓ (触发器捕获变更)
变更日志表 (emp_cdc)
    ↓ (Producer 轮询)
Kafka Topic (bf_employee_cdc)
    ↓ (Consumer 消费)
目标数据库 (PostgreSQL)
```

**核心理念**：
- 用户在源数据库操作（INSERT/UPDATE/DELETE）
- 系统自动同步这些变更到目标数据库
- 整个过程延迟 < 1 秒
- 保证不丢失、不重复、顺序正确

---

## 2. 阶段 1：基础设施启动

### 用户操作

```bash
cd proj2
docker compose up -d
sleep 30  # 等待所有容器就绪
```

### 系统内部发生了什么？

#### 2.1 启动 5 个 Docker 容器

**文件位置**：[../docker-compose.yml](../docker-compose.yml)

| 容器 | 端口 | 作用 |
|------|------|------|
| zookeeper | 22181 | Kafka 的协调服务 |
| kafka | 29092 | 消息中间件 |
| kafka-setup | — | 一次性任务：创建 Topic |
| db_source | 5432 | 源数据库 |
| db_dst | 5433 | 目标数据库 |

#### 2.2 kafka-setup 容器做了什么？

自动执行以下命令创建 2 个 Kafka Topic：

```bash
# Topic 1: 主数据流
kafka-topics --create \
  --topic bf_employee_cdc \
  --partitions 3 \
  --config cleanup.policy=compact  # 日志压缩

# Topic 2: 死信队列
kafka-topics --create \
  --topic bf_employee_cdc_dlq \
  --partitions 1
```

**为什么 3 个分区？**  
消息按 `emp_id` 分区，同一员工的所有事件进入同一分区，保证有序。

**什么是日志压缩？**  
Kafka 只保留每个 `emp_id` 的最新值，节省 50-80% 存储空间。

#### 2.3 源数据库初始化

**文件位置**：[../sql/init_source_db.sql](../sql/init_source_db.sql)

容器启动时自动执行此 SQL 文件，创建：

**表 1: employees（业务表）**
```sql
CREATE TABLE employees (
    emp_id     SERIAL PRIMARY KEY,  -- 自增主键
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    dob        DATE,
    city       VARCHAR(100),
    salary     INT
);
```

**表 2: emp_cdc（变更日志表）**
```sql
CREATE TABLE emp_cdc (
    action_id   SERIAL PRIMARY KEY,   -- 单调递增，用作偏移量
    emp_id      INT,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    dob         DATE,
    city        VARCHAR(100),
    salary      INT,
    action      VARCHAR(10),          -- 'INSERT'/'UPDATE'/'DELETE'
    action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**触发器：capture_employee_changes()**
```sql
CREATE OR REPLACE FUNCTION capture_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO emp_cdc(emp_id, ..., action) 
        VALUES (OLD.emp_id, ..., 'DELETE');
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO emp_cdc(emp_id, ..., action) 
        VALUES (NEW.emp_id, ..., 'UPDATE');
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO emp_cdc(emp_id, ..., action) 
        VALUES (NEW.emp_id, ..., 'INSERT');
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER employee_changes
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION capture_employee_changes();
```

**关键理解**：任何对 `employees` 表的操作都会自动在 `emp_cdc` 表中留下一条记录。

#### 2.4 目标数据库初始化

**文件位置**：[../sql/init_target_db.sql](../sql/init_target_db.sql)

**表 1: employees（目标表）**
```sql
CREATE TABLE employees (
    emp_id     INT PRIMARY KEY,  -- 注意：不是 SERIAL，值来自源库
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    dob        DATE,
    city       VARCHAR(100),
    salary     INT
);
```

**表 2: emp_cdc_dlq（死信队列表）**
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
    error_reason     TEXT,           -- 验证失败原因
    failed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    original_message TEXT            -- 原始 JSON 消息
);
```

**此时系统状态**：
- ✅ 5 个容器运行中
- ✅ 2 个 Kafka Topic 已创建
- ✅ 源数据库：2 张表 + 1 个触发器
- ✅ 目标数据库：2 张空表

---

## 3. 阶段 2：初始数据加载

### 用户操作

```bash
python3 src/load_initial_data.py
```

### 系统内部发生了什么？

**文件位置**：[../src/load_initial_data.py](../src/load_initial_data.py)

#### 3.1 读取 CSV 文件

**数据源**：[../data/employees.csv](../data/employees.csv)

```csv
emp_id,first_name,last_name,dob,city,salary
1,Max,Smith,1995-03-15,New York,75000
2,Karl,Summers,1992-07-20,Boston,82000
3,Sam,Wilde,1988-11-05,Chicago,68000
4,Linda,Chen,1991-02-28,Seattle,91000
```

**代码片段**（第 60-80 行）：
```python
with open('data/employees.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        employees.append(row)
```

#### 3.2 等待数据库就绪

**代码片段**（第 30-50 行）：
```python
def wait_for_database(conn_config, max_retries=30):
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**conn_config)
            conn.close()
            return True
        except psycopg2.OperationalError:
            time.sleep(1)
    return False
```

**为什么需要等待？**  
Docker 容器启动后，PostgreSQL 服务可能还在初始化，直接连接会失败。

#### 3.3 插入数据到源数据库

**代码片段**（第 85-100 行）：
```python
query = """
    INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (emp_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name  = EXCLUDED.last_name,
        dob        = EXCLUDED.dob,
        city       = EXCLUDED.city,
        salary     = EXCLUDED.salary
"""
for emp in employees:
    cursor.execute(query, (emp['emp_id'], emp['first_name'], ...))
conn.commit()
```

**UPSERT 语义**：如果 emp_id 已存在则更新，否则插入。

#### 3.4 触发器自动捕获变更

每次执行 `INSERT` 后，触发器自动触发：

```
INSERT INTO employees (emp_id=1, first_name='Max', ...)
    ↓ 触发器触发
INSERT INTO emp_cdc (action_id=1, emp_id=1, first_name='Max', action='INSERT', action_time=now())
```

4 条 INSERT 操作 → `emp_cdc` 表生成 4 条记录：

| action_id | emp_id | first_name | action | action_time |
|-----------|--------|------------|--------|-------------|
| 1 | 1 | Max | INSERT | 2026-02-12 10:00:00 |
| 2 | 2 | Karl | INSERT | 2026-02-12 10:00:01 |
| 3 | 3 | Sam | INSERT | 2026-02-12 10:00:02 |
| 4 | 4 | Linda | INSERT | 2026-02-12 10:00:03 |

**此时系统状态**：
- ✅ 源数据库 `employees` 表：4 条记录
- ✅ 源数据库 `emp_cdc` 表：4 条记录
- ⚠️ 目标数据库 `employees` 表：0 条记录（还未同步）

---

## 4. 阶段 3：CDC 系统启动

### 用户操作

```bash
./scripts/start_cdc.sh
```

### 系统内部发生了什么？

**文件位置**：[../scripts/start_cdc.sh](../scripts/start_cdc.sh)

#### 3.1 启动 Producer

**代码片段**（第 60-70 行）：
```bash
echo "Starting CDC Producer..."
nohup python3 -u src/producer.py > logs/producer.log 2>&1 &
PRODUCER_PID=$!
echo $PRODUCER_PID > producer.pid
echo "Producer started with PID: $PRODUCER_PID"
```

**解释**：
- `nohup`：即使终端关闭，进程继续运行
- `python3 -u`：无缓冲输出，日志实时写入
- `> logs/producer.log 2>&1`：标准输出和错误都写入日志文件
- `&`：后台运行
- `echo $PRODUCER_PID > producer.pid`：保存进程 ID，方便后续停止

#### 3.2 启动 Consumer

**代码片段**（第 75-85 行）：
```bash
echo "Starting CDC Consumer..."
nohup python3 -u src/consumer.py > logs/consumer.log 2>&1 &
CONSUMER_PID=$!
echo $CONSUMER_PID > consumer.pid
echo "Consumer started with PID: $CONSUMER_PID"
```

**此时系统状态**：
- ✅ Producer 进程运行中（后台）
- ✅ Consumer 进程运行中（后台）
- 📝 日志输出到 `logs/producer.log` 和 `logs/consumer.log`

---

## 5. 阶段 4：Producer 数据捕获与发送

**文件位置**：[../src/producer.py](../src/producer.py)

Producer 是一个持续运行的进程，每 5 秒执行一次循环。

### 5.1 初始化配置

**代码片段**（第 80-110 行）：
```python
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_CDC,
    DB_SOURCE_CONFIG,
    PRODUCER_CONFIG,
    POLL_INTERVAL,
    BATCH_SIZE,
    OFFSET_FILE
)

class cdcProducer(Producer):
    def __init__(self):
        # 配置 Kafka Producer
        producer_config = {
            'bootstrap.servers': 'localhost:29092',
            'acks': 'all',                      # 等待所有副本确认
            'enable.idempotence': True,         # 幂等保证，防重复
            'compression.type': 'snappy',       # 压缩减少网络传输
            'linger.ms': 10,                    # 批量等待 10ms
            'batch.size': 16384,                # 16KB 批量大小
        }
        super().__init__(producer_config)
```

**配置文件位置**：[../src/config.py](../src/config.py)

**关键参数解释**：
- `acks='all'`：Producer 等待 Leader 和所有副本都确认后才认为发送成功
- `enable.idempotence=True`：即使网络故障重试，Kafka 也能自动去重

### 5.2 主循环：第一次迭代

#### 步骤 1：加载偏移量

**代码片段**（第 120-140 行）：
```python
def load_offset(self):
    """从文件读取上次处理到的 action_id"""
    if not os.path.exists(self.offset_file):
        return 0  # 首次运行，从 0 开始
    with open(self.offset_file, 'r') as f:
        content = f.read().strip()
        return int(content) if content else 0
```

**第一次运行**：`cdc_offset.txt` 不存在，返回 `offset = 0`

#### 步骤 2：查询 CDC 表

**代码片段**（第 150-170 行）：
```python
def fetch_cdc(self, offset):
    """从 emp_cdc 表获取新的变更记录"""
    query = """
        SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
        FROM emp_cdc
        WHERE action_id > %s    -- 只取大于当前偏移量的记录
        ORDER BY action_id      -- 按顺序处理
        LIMIT %s                -- 限制批量大小
    """
    cursor.execute(query, (offset, self.batch_size))
    return cursor.fetchall()
```

**第一次执行**：
- `offset = 0`
- `batch_size = 100`
- SQL: `SELECT ... FROM emp_cdc WHERE action_id > 0 ORDER BY action_id LIMIT 100`
- **返回 4 条记录**（action_id = 1, 2, 3, 4）

#### 步骤 3：转换为 Employee 对象

**代码片段**（第 200-210 行）：
```python
for record in cdc_records:
    # record = (1, 1, 'Max', 'Smith', '1995-03-15', 'New York', 75000, 'INSERT')
    employee = Employee.from_line(record)
```

**Employee.from_line() 位置**：[../src/employee.py](../src/employee.py) 第 60-85 行

```python
@classmethod
def from_line(cls, line):
    """从数据库查询结果构造 Employee 对象"""
    action_id, emp_id, first_name, last_name, dob, city, salary, action = line
    return cls(
        emp_id=emp_id,
        first_name=first_name,
        last_name=last_name,
        dob=dob,
        city=city,
        salary=salary,
        action=action
    )
```

#### 步骤 4：序列化为 JSON

**代码片段**（第 215 行）：
```python
json_value = employee.to_json()
```

**Employee.to_json() 位置**：[../src/employee.py](../src/employee.py) 第 100-115 行

```python
def to_json(self):
    """序列化为 JSON 字符串，用于发送到 Kafka"""
    return json.dumps({
        'emp_id': self.emp_id,
        'first_name': self.first_name,
        'last_name': self.last_name,
        'dob': self.dob.isoformat() if self.dob else None,
        'city': self.city,
        'salary': self.salary,
        'action': self.action
    })
```

**生成的 JSON**：
```json
{
  "emp_id": 1,
  "first_name": "Max",
  "last_name": "Smith",
  "dob": "1995-03-15",
  "city": "New York",
  "salary": 75000,
  "action": "INSERT"
}
```

#### 步骤 5：发送到 Kafka

**代码片段**（第 220-230 行）：
```python
self.produce(
    topic='bf_employee_cdc',
    key=str(employee.emp_id).encode('utf-8'),  # Key = "1"
    value=json_value.encode('utf-8'),          # JSON 编码为 bytes
    callback=self.delivery_callback
)
```

**关键理解**：
- **Key = emp_id**：Kafka 根据 Key 的哈希值决定分区
  ```python
  partition = hash("1") % 3  # 假设结果是 partition 1
  ```
- 所有 `emp_id=1` 的消息都进入 partition 1，保证有序

**Kafka 中的消息**：
```
Topic: bf_employee_cdc
Partition 1: 
  Offset 0: Key="1", Value={"emp_id":1,"first_name":"Max",...}
Partition 2:
  Offset 0: Key="2", Value={"emp_id":2,"first_name":"Karl",...}
Partition 0:
  Offset 0: Key="3", Value={"emp_id":3,"first_name":"Sam",...}
  Offset 1: Key="4", Value={"emp_id":4,"first_name":"Linda",...}
```

#### 步骤 6：等待确认并保存偏移量

**代码片段**（第 240-260 行）：
```python
# 发送完所有消息后，阻塞直到 Kafka 确认
self.flush()

# 保存新的偏移量
max_action_id = cdc_records[-1][0]  # 最后一条记录的 action_id = 4
self.save_offset(max_action_id)
```

**save_offset() 代码**（第 145-150 行）：
```python
def save_offset(self, offset):
    """持久化偏移量到文件"""
    with open(self.offset_file, 'w') as f:
        f.write(str(offset))
```

**cdc_offset.txt 内容**：
```
4
```

#### 步骤 7：休眠并准备下次循环

**代码片段**（第 265 行）：
```python
time.sleep(POLL_INTERVAL)  # 5 秒
```

**下次循环**：
- 加载 offset = 4
- SQL: `WHERE action_id > 4` → 如果没有新数据，返回空列表
- 不发送消息，继续休眠

**Producer 总结**：
- ✅ 4 条消息已发送到 Kafka
- ✅ 偏移量保存为 4
- ✅ 每 5 秒检查一次新的变更

---

## 6. 阶段 5：Consumer 数据消费与同步

**文件位置**：[../src/consumer.py](../src/consumer.py)

Consumer 也是持续运行的进程，不断从 Kafka 拉取消息。

### 6.1 初始化配置

**代码片段**（第 80-110 行）：
```python
class cdcConsumer(Consumer):
    def __init__(self):
        consumer_config = {
            'bootstrap.servers': 'localhost:29092',
            'group.id': 'bf_cdc_consumer',
            'enable.auto.commit': False,         # 手动提交偏移量
            'auto.offset.reset': 'earliest',     # 从头开始消费
            'isolation.level': 'read_committed', # 只读已提交消息
        }
        super().__init__(consumer_config)
        self.subscribe(['bf_employee_cdc'])
```

**关键配置解释**：
- `enable.auto.commit=False`：Consumer 必须手动调用 `commit()`，防止消息处理失败但偏移量已提交
- `auto.offset.reset='earliest'`：首次启动从 Topic 最早的消息开始消费
- `isolation.level='read_committed'`：跳过未提交的事务消息，防止读到脏数据

### 6.2 主循环：处理第一条消息

#### 步骤 1：拉取消息

**代码片段**（第 150-165 行）：
```python
def consume_messages(self):
    while self.running:
        # 从 Kafka 拉取一条消息，超时 1 秒
        msg = self.poll(timeout=1.0)
        
        if msg is None:
            continue  # 没有消息，继续循环
        
        if msg.error():
            # 处理错误...
            continue
```

**第一次拉取**：
```python
msg.topic() = 'bf_employee_cdc'
msg.partition() = 1
msg.offset() = 0
msg.key() = b'1'
msg.value() = b'{"emp_id":1,"first_name":"Max","last_name":"Smith",...}'
```

#### 步骤 2：反序列化

**代码片段**（第 180-195 行）：
```python
# 解码 JSON
json_str = msg.value().decode('utf-8')
data = json.loads(json_str)

# 转换为 Employee 对象
employee = Employee.from_json(json_str)
```

**Employee.from_json() 位置**：[../src/employee.py](../src/employee.py) 第 120-145 行

```python
@classmethod
def from_json(cls, json_str):
    """从 JSON 字符串构造 Employee 对象"""
    data = json.loads(json_str)
    return cls(
        emp_id=data['emp_id'],
        first_name=data['first_name'],
        last_name=data['last_name'],
        dob=datetime.strptime(data['dob'], '%Y-%m-%d').date(),
        city=data['city'],
        salary=data['salary'],
        action=data['action']
    )
```

**得到的对象**：
```python
employee = Employee(
    emp_id=1,
    first_name='Max',
    last_name='Smith',
    dob=date(1995, 3, 15),
    city='New York',
    salary=75000,
    action='INSERT'
)
```

#### 步骤 3：验证数据

**代码片段**（第 200-205 行）：
```python
is_valid, error_msg = employee.validate()

if not is_valid:
    self.send_to_dlq(employee, error_msg)
    self.commit(asynchronous=False)
    continue
```

**Employee.validate() 位置**：[../src/employee.py](../src/employee.py) 第 160-210 行

```python
def validate(self):
    """验证数据是否符合业务规则"""
    # 规则 1: emp_id 必须 >= 0
    if self.emp_id < MIN_EMPLOYEE_ID:
        return False, f"Employee ID {self.emp_id} is invalid"
    
    # 规则 2: 出生年份必须 > 1990
    if self.dob and self.dob.year <= MIN_BIRTH_YEAR:
        return False, f"Birth year {self.dob.year} must be after {MIN_BIRTH_YEAR}"
    
    # 规则 3: 薪资必须 >= 10000
    if self.salary < MIN_SALARY:
        return False, f"Salary {self.salary} below minimum {MIN_SALARY}"
    
    return True, None
```

**验证规则来源**：[../src/config.py](../src/config.py) 第 100-110 行
```python
MIN_EMPLOYEE_ID = 0
MIN_BIRTH_YEAR = 1990
MIN_SALARY = 10000
```

**Max 的数据验证**：
- emp_id=1 >= 0 ✅
- 出生年份=1995 > 1990 ✅
- salary=75000 >= 10000 ✅
- **验证通过**

#### 步骤 4：写入目标数据库

**代码片段**（第 220-260 行）：
```python
def update_dst(self, employee):
    """将数据写入目标数据库"""
    conn = psycopg2.connect(**DB_TARGET_CONFIG)
    cursor = conn.cursor()
    
    if employee.action == 'INSERT' or employee.action == 'UPDATE':
        # UPSERT 操作
        query = """
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (emp_id)
            DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name  = EXCLUDED.last_name,
                dob        = EXCLUDED.dob,
                city       = EXCLUDED.city,
                salary     = EXCLUDED.salary
        """
        cursor.execute(query, (
            employee.emp_id,
            employee.first_name,
            employee.last_name,
            employee.dob,
            employee.city,
            employee.salary
        ))
    
    elif employee.action == 'DELETE':
        query = "DELETE FROM employees WHERE emp_id = %s"
        cursor.execute(query, (employee.emp_id,))
    
    conn.commit()
    cursor.close()
    conn.close()
```

**UPSERT 解释**：
- `INSERT ... ON CONFLICT (emp_id) DO UPDATE`
- 如果 emp_id=1 不存在 → 执行 INSERT
- 如果 emp_id=1 已存在 → 执行 UPDATE（覆盖所有字段）

**为什么用 UPSERT？**  
保证幂等性：如果同一条消息被处理两次（例如崩溃后重启），结果是相同的。

**执行结果**：
```sql
-- 目标数据库执行的 SQL
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (1, 'Max', 'Smith', '1995-03-15', 'New York', 75000)
ON CONFLICT (emp_id) DO UPDATE SET ...
-- 因为 emp_id=1 之前不存在，所以执行 INSERT
```

**目标数据库状态**：

| emp_id | first_name | last_name | dob | city | salary |
|--------|------------|-----------|-----|------|--------|
| 1 | Max | Smith | 1995-03-15 | New York | 75000 |

#### 步骤 5：提交 Kafka 偏移量

**代码片段**（第 280-285 行）：
```python
# 同步提交偏移量
self.commit(asynchronous=False)
```

**关键理解**：
- 偏移量**只在**数据库写入成功后才提交
- 如果数据库写入失败（例如连接断开），不会提交偏移量
- Consumer 重启后会重新消费这条消息
- 因为使用了 UPSERT，重复处理是安全的

**Kafka 中 Consumer Group 的偏移量**：
```
Group: bf_cdc_consumer
Topic: bf_employee_cdc
  Partition 1: Offset 1 (已处理到 offset 0，下次从 offset 1 开始)
  Partition 2: Offset 0
  Partition 0: Offset 0
```

#### 步骤 6：继续处理剩余 3 条消息

循环回到步骤 1，依次处理：
- emp_id=2 (Karl) → 写入目标库 → 提交
- emp_id=3 (Sam) → 写入目标库 → 提交
- emp_id=4 (Linda) → 写入目标库 → 提交

**最终目标数据库状态**：

| emp_id | first_name | last_name | dob | city | salary |
|--------|------------|-----------|-----|------|--------|
| 1 | Max | Smith | 1995-03-15 | New York | 75000 |
| 2 | Karl | Summers | 1992-07-20 | Boston | 82000 |
| 3 | Sam | Wilde | 1988-11-05 | Chicago | 68000 |
| 4 | Linda | Chen | 1991-02-28 | Seattle | 91000 |

**Consumer 总结**：
- ✅ 4 条消息已消费
- ✅ 4 条记录已写入目标数据库
- ✅ Kafka 偏移量已提交
- ✅ Consumer 进入等待状态，准备处理新消息

---

## 7. 阶段 6：数据验证

### 用户操作

```bash
python3 src/verify_sync.py
```

### 系统内部发生了什么？

**文件位置**：[../src/verify_sync.py](../src/verify_sync.py)

#### 7.1 查询两个数据库

**代码片段**（第 60-90 行）：
```python
# 查询源数据库
conn_src = psycopg2.connect(**DB_SOURCE_CONFIG)
cursor_src = conn_src.cursor()
cursor_src.execute("SELECT emp_id, first_name, last_name, dob, city, salary FROM employees ORDER BY emp_id")
source_data = {row[0]: row for row in cursor_src.fetchall()}

# 查询目标数据库
conn_dst = psycopg2.connect(**DB_TARGET_CONFIG)
cursor_dst = conn_dst.cursor()
cursor_dst.execute("SELECT emp_id, first_name, last_name, dob, city, salary FROM employees ORDER BY emp_id")
target_data = {row[0]: row for row in cursor_dst.fetchall()}
```

**source_data**：
```python
{
    1: (1, 'Max', 'Smith', date(1995, 3, 15), 'New York', 75000),
    2: (2, 'Karl', 'Summers', date(1992, 7, 20), 'Boston', 82000),
    3: (3, 'Sam', 'Wilde', date(1988, 11, 5), 'Chicago', 68000),
    4: (4, 'Linda', 'Chen', date(1991, 2, 28), 'Seattle', 91000)
}
```

**target_data**：（相同）

#### 7.2 逐条比较

**代码片段**（第 120-180 行）：
```python
only_in_source = []
only_in_target = []
different_data = []

# 检查源库中的记录
for emp_id, src_row in source_data.items():
    if emp_id not in target_data:
        only_in_source.append(emp_id)
    elif src_row != target_data[emp_id]:
        different_data.append({
            'emp_id': emp_id,
            'source': src_row,
            'target': target_data[emp_id]
        })

# 检查目标库中的记录
for emp_id in target_data:
    if emp_id not in source_data:
        only_in_target.append(emp_id)
```

**比较结果**：
- `only_in_source` = [] （所有源库记录都在目标库中）
- `only_in_target` = [] （目标库没有多余记录）
- `different_data` = [] （所有字段完全匹配）

#### 7.3 打印结果

**输出**：
```
========================================
    CDC SYNC VERIFICATION REPORT
========================================

Total employees in source: 4
Total employees in target: 4
Sync rate: 100.00%

✓ All employees are in sync
✓ No data mismatches found

========================================
```

---

## 8. 异常流程：DLQ 处理

### 场景：插入不符合验证规则的数据

假设用户在源数据库执行：

```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (2000, 'Invalid', 'User', '1992-01-01', 'Los Angeles', 5000);
```

**问题**：`salary=5000 < 10000`（违反验证规则）

### 流程追踪

#### 8.1 触发器捕获

触发器自动触发，写入 `emp_cdc` 表：

| action_id | emp_id | first_name | last_name | salary | action |
|-----------|--------|------------|-----------|--------|--------|
| 5 | 2000 | Invalid | User | 5000 | INSERT |

#### 8.2 Producer 发送到 Kafka

Producer 下次轮询时（5 秒内）：
- 查询 `WHERE action_id > 4` → 取到 action_id=5
- 转换为 Employee 对象
- 序列化为 JSON
- 发送到 Kafka Topic `bf_employee_cdc`

#### 8.3 Consumer 拉取消息

Consumer 从 Kafka 拉取到这条消息。

#### 8.4 验证失败

**代码片段**（[../src/employee.py](../src/employee.py) 第 190 行）：
```python
if self.salary < MIN_SALARY:  # 5000 < 10000
    return False, f"Salary {self.salary} below minimum {MIN_SALARY}"
```

**返回**：`is_valid=False, error_msg="Salary 5000 below minimum 10000"`

#### 8.5 写入 DLQ 表

**代码片段**（[../src/consumer.py](../src/consumer.py) 第 300-340 行）：
```python
def send_to_dlq(self, employee, error_reason):
    """将无效记录写入死信队列"""
    conn = psycopg2.connect(**DB_TARGET_CONFIG)
    cursor = conn.cursor()
    
    query = """
        INSERT INTO emp_cdc_dlq 
        (emp_id, first_name, last_name, dob, city, salary, action, 
         error_reason, original_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    original_json = employee.to_json()
    
    cursor.execute(query, (
        employee.emp_id,
        employee.first_name,
        employee.last_name,
        employee.dob,
        employee.city,
        employee.salary,
        employee.action,
        error_reason,           # "Salary 5000 below minimum 10000"
        original_json           # 完整 JSON 消息
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
```

#### 8.6 提交偏移量

**代码片段**（第 350 行）：
```python
self.commit(asynchronous=False)  # 仍然正常提交
```

**关键理解**：
- DLQ 记录**不会阻塞**主流程
- Consumer 继续处理后续消息
- 管理员可以稍后查看 DLQ 表，修复数据后重放

#### 8.7 最终状态

**目标数据库 employees 表**：
- ❌ **没有** emp_id=2000 的记录

**目标数据库 emp_cdc_dlq 表**：

| id | emp_id | first_name | salary | action | error_reason | original_message |
|----|--------|------------|--------|--------|--------------|------------------|
| 1 | 2000 | Invalid | 5000 | INSERT | Salary 5000 below minimum 10000 | {"emp_id":2000,...} |

**查看 DLQ**：
```bash
# 连接目标数据库
psql -h localhost -p 5433 -U postgres -d postgres

# 查询 DLQ
SELECT emp_id, error_reason, failed_at FROM emp_cdc_dlq;
```

---

## 9. 完整流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                   阶段 1: 基础设施启动                             │
│                 docker compose up -d                             │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │   docker-compose.yml            │
        ├─────────────────────────────────┤
        │ • zookeeper (22181)             │
        │ • kafka (29092)                 │
        │ • kafka-setup → 创建 2 个 Topic  │
        │ • db_source (5432)              │
        │   └→ init_source_db.sql         │
        │      ├─ employees 表             │
        │      ├─ emp_cdc 表               │
        │      └─ 触发器                   │
        │ • db_dst (5433)                 │
        │   └→ init_target_db.sql         │
        │      ├─ employees 表             │
        │      └─ emp_cdc_dlq 表           │
        └─────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                   阶段 2: 初始数据加载                             │
│              python3 src/load_initial_data.py                   │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │   data/employees.csv            │
        │   读取 4 条员工记录              │
        └────────────────┬────────────────┘
                         ↓
                 INSERT INTO employees
                         ↓ (触发器触发)
        ┌────────────────┴────────────────┐
        │   emp_cdc 表                    │
        │   action_id | emp_id | action   │
        │   1         | 1      | INSERT   │
        │   2         | 2      | INSERT   │
        │   3         | 3      | INSERT   │
        │   4         | 4      | INSERT   │
        └─────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                   阶段 3: CDC 系统启动                             │
│                  ./scripts/start_cdc.sh                         │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │   启动 Producer 和 Consumer      │
        │   后台运行，日志输出到 logs/     │
        └─────────────────────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │         并行运行                 │
        ├──────────────┬──────────────────┤
        ▼              ▼                  │
   ┌─────────┐   ┌──────────┐            │
   │Producer │   │Consumer  │            │
   │  每5秒   │   │  持续拉取 │            │
   └─────────┘   └──────────┘            │
        │              │                  │
        └──────┬───────┘                  │
               ↓                          │
┌──────────────┴──────────────────────────┴───────────────────────┐
│                   阶段 4: Producer 捕获与发送                      │
│                       src/producer.py                           │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  1. load_offset()               │
        │     cdc_offset.txt → offset=0   │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  2. fetch_cdc(offset)           │
        │     SELECT * FROM emp_cdc       │
        │     WHERE action_id > 0         │
        │     → 返回 4 条记录              │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  3. for each record:            │
        │     • Employee.from_line()      │
        │       (src/employee.py)         │
        │     • employee.to_json()        │
        │       (src/employee.py)         │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  4. produce()                   │
        │     topic = bf_employee_cdc     │
        │     key = emp_id                │
        │     value = JSON                │
        │                                 │
        │     emp_id → partition:         │
        │     1 → partition 1             │
        │     2 → partition 2             │
        │     3 → partition 0             │
        │     4 → partition 0             │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  5. flush() + save_offset(4)    │
        │     cdc_offset.txt ← 4          │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  6. sleep(5) → 循环             │
        └─────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                   阶段 5: Consumer 消费与同步                      │
│                       src/consumer.py                           │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  1. poll(timeout=1.0)           │
        │     从 Kafka 拉取一条消息        │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  2. Employee.from_json()        │
        │     (src/employee.py)           │
        │     JSON → Employee 对象         │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  3. employee.validate()         │
        │     (src/employee.py)           │
        │     检查业务规则 (config.py)     │
        └────────────────┬────────────────┘
                         ↓
               ┌─────────┴──────────┐
               │    验证通过？       │
               └──┬───────────────┬─┘
            是    ↓               ↓ 否
        ┌─────────┴──────┐  ┌─────┴──────────┐
        │ 4a. update_dst()│  │ 4b. send_to_dlq()
        │                │  │                 │
        │ INSERT INTO    │  │ INSERT INTO     │
        │ employees ...  │  │ emp_cdc_dlq ... │
        │ ON CONFLICT    │  │                 │
        │ DO UPDATE      │  │ error_reason    │
        │                │  │ original_message│
        └────────┬───────┘  └─────┬──────────┘
                 │                │
                 └────────┬───────┘
                          ↓
        ┌─────────────────┴───────────────┐
        │  5. commit(asynchronous=False)  │
        │     提交 Kafka 偏移量            │
        └─────────────────┬───────────────┘
                          ↓
        ┌─────────────────┴───────────────┐
        │  6. 循环 → 处理下一条消息        │
        └─────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│                   阶段 6: 数据验证                                 │
│                  python3 src/verify_sync.py                     │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  1. 查询源数据库                 │
        │     SELECT * FROM employees     │
        │     → source_data               │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  2. 查询目标数据库               │
        │     SELECT * FROM employees     │
        │     → target_data               │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  3. 逐条比较                    │
        │     • 只在源库？                │
        │     • 只在目标库？              │
        │     • 数据不一致？              │
        └────────────────┬────────────────┘
                         ↓
        ┌────────────────┴────────────────┐
        │  4. 打印报告                    │
        │     Sync rate: 100%             │
        │     ✓ All employees match       │
        └─────────────────────────────────┘
```

---

## 10. 关键文件速查表

| 阶段 | 文件 | 核心代码行 | 功能说明 |
|------|------|-----------|----------|
| **基础设施** | [../docker-compose.yml](../docker-compose.yml) | 1-88 | 定义所有容器和网络 |
| | [../sql/init_source_db.sql](../sql/init_source_db.sql) | 1-30 | 创建 employees 和 emp_cdc 表 |
| | | 40-62 | 定义触发器 capture_employee_changes() |
| | [../sql/init_target_db.sql](../sql/init_target_db.sql) | 1-20 | 创建 employees 表（镜像） |
| | | 20-34 | 创建 emp_cdc_dlq 表（死信队列） |
| **配置** | [../src/config.py](../src/config.py) | 20-40 | 数据库连接配置 |
| | | 40-60 | Kafka 配置 |
| | | 80-95 | Producer 配置 |
| | | 100-110 | 验证规则（MIN_SALARY, MIN_BIRTH_YEAR） |
| **数据模型** | [../src/employee.py](../src/employee.py) | 20-50 | Employee 类定义 |
| | | 60-85 | from_line() - 数据库行 → 对象 |
| | | 100-115 | to_json() - 对象 → JSON 字符串 |
| | | 120-145 | from_json() - JSON 字符串 → 对象 |
| | | 160-210 | validate() - 业务规则验证 |
| **数据加载** | [../src/load_initial_data.py](../src/load_initial_data.py) | 30-50 | wait_for_database() 等待 DB 就绪 |
| | | 60-80 | 读取 CSV 文件 |
| | | 85-105 | INSERT INTO employees (UPSERT) |
| **Producer** | [../src/producer.py](../src/producer.py) | 80-110 | 初始化 Kafka Producer |
| | | 120-140 | load_offset() / save_offset() |
| | | 150-170 | fetch_cdc() - 查询 emp_cdc 表 |
| | | 200-230 | 主循环：序列化 + produce() |
| | | 240-260 | flush() + 保存偏移量 + sleep(5) |
| **Consumer** | [../src/consumer.py](../src/consumer.py) | 80-110 | 初始化 Kafka Consumer |
| | | 150-165 | poll() 拉取消息 |
| | | 180-195 | 反序列化 JSON → Employee |
| | | 200-205 | 调用 validate() |
| | | 220-260 | update_dst() - UPSERT 写入目标库 |
| | | 280-285 | commit() 提交偏移量 |
| | | 300-340 | send_to_dlq() - 写入死信队列 |
| **启动脚本** | [../scripts/start_cdc.sh](../scripts/start_cdc.sh) | 60-70 | 启动 Producer 进程 |
| | | 75-85 | 启动 Consumer 进程 |
| | | 90-110 | 保存 PID，打印状态 |
| **停止脚本** | [../scripts/stop_cdc.sh](../scripts/stop_cdc.sh) | 30-50 | 读取 PID，优雅停止进程 |
| **验证脚本** | [../src/verify_sync.py](../src/verify_sync.py) | 60-90 | 查询源库和目标库 |
| | | 120-180 | 逐条比较数据 |
| | | 200-250 | 打印同步报告 + CDC 状态 + DLQ 状态 |
| **测试脚本** | [../src/test_cdc.py](../src/test_cdc.py) | 100-150 | test_insert() - 测试 INSERT 同步 |
| | | 180-230 | test_update() - 测试 UPDATE 同步 |
| | | 260-310 | test_delete() - 测试 DELETE 同步 |
| | | 340-400 | test_dlq() - 测试 DLQ 机制 |

---

## 总结

整个数据流程可以用一句话概括：

> **用户在源数据库操作 → 触发器捕获到 emp_cdc 表 → Producer 每 5 秒轮询并发送到 Kafka → Consumer 持续消费，验证后写入目标数据库（或 DLQ）→ 手动提交偏移量**

**核心设计原则**：
1. **解耦**：源库和目标库通过 Kafka 解耦，互不影响
2. **可靠**：幂等 Producer + EOS Consumer + UPSERT，保证不丢不重
3. **有序**：同一 emp_id 的消息进入同一分区，保证有序
4. **容错**：DLQ 机制确保坏数据不阻塞主流程
5. **可恢复**：文件偏移量 + Kafka 偏移量，崩溃后精确恢复

这就是本项目从启动到数据同步完成的**完整、详细的数据流程**！
