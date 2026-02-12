# Kafka CDC Project 2 - Implementation Plan

## 项目概述
实现一个基于 Kafka 的 Change Data Capture (CDC) 系统，在两个 PostgreSQL 数据库之间同步员工数据，要求变更在 1 秒内完成同步。

---

## 最终文件结构

```
proj2/
├── docker-compose.yml           # ✅ Kafka + PostgreSQL
├── requirements.txt             # ✅ Python依赖
├── cdc_offset.txt               # Producer进度记录
│
├── data/
│   └── employees.csv            # ✅ 初始数据
│
├── sql/
│   ├── init_source_db.sql       # ✅ 源数据库初始化
│   └── init_target_db.sql       # ✅ 目标数据库初始化
│
├── src/
│   ├── config.py                # ✅ 配置管理
│   ├── employee.py              # ✅ 数据模型
│   ├── producer.py              # ✅ Producer逻辑
│   ├── consumer.py              # ✅ Consumer逻辑
│   ├── admin.py                 # ✅ Kafka Admin
│   ├── load_initial_data.py     # ✅ 加载CSV数据
│   ├── test_cdc.py              # ✅ CDC功能测试
│   └── verify_sync.py           # ✅ 数据一致性验证
│
├── scripts/
│   ├── start_cdc.sh             # ✅ 启动脚本
│   ├── stop_cdc.sh              # ✅ 停止脚本
│   └── run_tests.sh             # ✅ 测试脚本
│
└── docs/
  ├── PROJECT_PLAN.md          # 本计划文档
  ├── SCRIPTS_GUIDE.md         # 脚本说明
  └── tutor.md                 # 新手教程
```

---

## 实施阶段

### 🟦 阶段 1：数据库设置与初始化（必需）

**目标**：搭建两个独立的 PostgreSQL 数据库，并创建表结构和触发器

#### 1.1 创建源数据库初始化脚本 `sql/init_source_db.sql`

**内容**：
- 创建 `employees` 表
  ```sql
  CREATE TABLE employees (
      emp_id SERIAL PRIMARY KEY,
      first_name VARCHAR(100),
      last_name VARCHAR(100),
      dob DATE,
      city VARCHAR(100),
      salary INT
  );
  ```

- 创建 `emp_cdc` 变更捕获表
  ```sql
  CREATE TABLE emp_cdc (
      action_id SERIAL PRIMARY KEY,
      emp_id INT,
      first_name VARCHAR(100),
      last_name VARCHAR(100),
      dob DATE,
      city VARCHAR(100),
      salary INT,
      action VARCHAR(10),
      action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```

- 创建触发器函数
  ```sql
  CREATE OR REPLACE FUNCTION capture_employee_changes()
  RETURNS TRIGGER AS $$
  BEGIN
      IF (TG_OP = 'DELETE') THEN
          INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
          VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
          RETURN OLD;
      ELSIF (TG_OP = 'UPDATE') THEN
          INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
          VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
          RETURN NEW;
      ELSIF (TG_OP = 'INSERT') THEN
          INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
          VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
          RETURN NEW;
      END IF;
  END;
  $$ LANGUAGE plpgsql;
  ```

- 创建触发器
  ```sql
  CREATE TRIGGER employee_cdc_trigger
  AFTER INSERT OR UPDATE OR DELETE ON employees
  FOR EACH ROW EXECUTE FUNCTION capture_employee_changes();
  ```

#### 1.2 创建目标数据库初始化脚本 `sql/init_target_db.sql`

**内容**：
- 创建 `employees` 表（结构同源库）
- 创建 DLQ 表用于存储验证失败的记录
  ```sql
  CREATE TABLE emp_cdc_dlq (
      id SERIAL PRIMARY KEY,
      emp_id INT,
      first_name VARCHAR(100),
      last_name VARCHAR(100),
      dob DATE,
      city VARCHAR(100),
      salary INT,
      action VARCHAR(10),
      error_reason TEXT,
      failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```

#### 1.3 更新 `docker-compose.yml`

**修改**：
```yaml
db_source:
  image: postgres:14.1-alpine
  restart: always
  environment:
    - POSTGRES_USER=postgres
    - POSTGRES_PASSWORD=postgres
  ports:
    - '5432:5432'
  volumes:
    - db_source:/var/lib/bf_kafka_proj2_source/data
    - ./sql/init_source_db.sql:/docker-entrypoint-initdb.d/init.sql  # 新增

db_dst:
  image: postgres:14.1-alpine
  restart: always
  environment:
    - POSTGRES_USER=postgres
    - POSTGRES_PASSWORD=postgres
  ports:
    - '5433:5432'
  volumes:
    - db_dst:/var/lib/bf_kafka_proj2_dst/data
    - ./sql/init_target_db.sql:/docker-entrypoint-initdb.d/init.sql  # 新增
```

#### 1.4 创建数据加载脚本 `src/load_initial_data.py`

**功能**：读取 `employees.csv` 并插入到源数据库

---

### 🟩 阶段 2：配置管理与代码框架（必需）

#### 2.1 创建 `config.py` 配置文件

**内容**：
```python
# 数据库配置
DB_SOURCE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'postgres'
}

DB_TARGET_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'postgres'
}

# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC_CDC = 'bf_employee_cdc'
KAFKA_TOPIC_DLQ = 'bf_employee_cdc_dlq'
CONSUMER_GROUP_ID = 'bf_cdc_consumer'

# CDC 配置
POLL_INTERVAL = 5  # Producer 轮询间隔（秒）
OFFSET_FILE = 'cdc_offset.txt'  # 记录已处理的 action_id
```

#### 2.2 改进 `employee.py` 数据模型

**新增功能**：
- `to_json()` 方法：序列化为 JSON
- `from_json()` 类方法：从 JSON 反序列化
- `validate()` 方法：数据验证逻辑
  - 出生年份 > 2007
  - 薪资 > 10000
  - emp_id >= 0

#### 2.3 创建 `requirements.txt`

**内容**：
```
confluent-kafka==2.3.0
psycopg2-binary==2.9.9
```

---

### 🟨 阶段 3：实现 Producer（生产者）（必需）

#### 3.1 完善 `producer.py` 核心功能

**需要实现的方法**：

1. **`fetch_cdc(last_action_id)` 方法**
   - 连接源数据库
   - 查询 `emp_cdc` 表：`SELECT * FROM emp_cdc WHERE action_id > %s ORDER BY action_id`
   - 返回变更记录列表

2. **`load_offset()` 方法**
   - 从文件读取上次处理的 action_id

3. **`save_offset(action_id)` 方法**
   - 保存最新处理的 action_id 到文件

4. **`run()` 主循环**
   - 无限循环，每隔 5 秒轮询一次
   - 调用 `fetch_cdc()` 获取新变更
   - 遍历每条记录：
     - 序列化为 JSON
     - 使用 `self.produce()` 发送到 Kafka
     - 调用 `self.flush()` 确保发送成功
   - 更新 offset

**错误处理**：
- 捕获数据库连接异常
- 捕获 Kafka 发送失败
- 记录详细日志

---

### 🟧 阶段 4：实现 Consumer（消费者）（必需）

#### 4.1 完善 `consumer.py` 核心功能

**需要实现的方法**：

1. **`consume()` 方法**
   - 订阅 Kafka 主题 `bf_employee_cdc`
   - 无限循环消费消息
   - 反序列化 JSON → Employee 对象
   - 调用 `validate()` 验证数据
   - 验证通过 → `update_dst()`
   - 验证失败 → `send_to_dlq()`

2. **`update_dst(employee)` 方法**
   - 连接目标数据库
   - 根据 `action` 字段执行 SQL：
     - `INSERT`: `INSERT INTO employees (...) VALUES (...)`
     - `UPDATE`: `UPDATE employees SET ... WHERE emp_id = %s`
     - `DELETE`: `DELETE FROM employees WHERE emp_id = %s`
   - 使用 UPSERT 语法处理幂等性：
     ```sql
     INSERT INTO employees (...) VALUES (...)
     ON CONFLICT (emp_id) DO UPDATE SET ...
     ```

3. **`send_to_dlq(employee, error_reason)` 方法**
   - 将验证失败的记录插入 `emp_cdc_dlq` 表

**错误处理**：
- 捕获 Kafka 消费异常
- 捕获数据库操作异常
- 记录详细日志

---

### 🟪 阶段 5：测试与验证（必需）

#### 5.1 创建 `test_cdc.py` 测试脚本

**功能**：
- 连接源数据库
- 执行测试操作：
  - 插入新员工
  - 更新现有员工
  - 删除员工
- 等待 1 秒
- 验证目标数据库的数据一致性

#### 5.2 创建 `verify_sync.py` 验证脚本

**功能**：
- 比较源库和目标库的 `employees` 表
- 输出差异报告

#### 5.3 运行流程

1. 启动所有容器：
   ```bash
   cd proj2
   docker compose down -v  # 清理旧数据
   docker compose up -d
   ```

2. 等待容器启动（约 30 秒）

3. 加载初始数据：
   ```bash
  python3 src/load_initial_data.py
   ```

4. 启动 Producer：
   ```bash
  python3 src/producer.py
   ```

5. 启动 Consumer（新终端）：
   ```bash
  python3 src/consumer.py
   ```

6. 运行测试（新终端）：
   ```bash
  python3 src/test_cdc.py
   ```

7. 验证结果：
   ```bash
  python3 src/verify_sync.py
   ```

---

### 🟥 阶段 6：DLQ 和高级功能（可选加分项）

#### 6.1 DLQ 验证逻辑（已计划在阶段 4）

- ✅ 在 Employee.validate() 中实现
- ✅ 在 Consumer 中调用并处理失败

#### 6.2 监控和日志（可选）

- 使用 Python logging 模块
- 记录：
  - Producer 发送消息数量
  - Consumer 处理消息数量
  - DLQ 失败记录数量
  - 延迟时间

#### 6.3 Offset Explorer（可选）

- 安装 Offset Explorer
- 连接到 `localhost:29092`
- 查看主题消息、消费者组状态

---

## 验收标准

### ✅ 必需功能
1. Docker 容器全部正常运行（`docker compose ps`）
2. 源数据库触发器正常工作（变更自动记录到 emp_cdc）
3. Producer 能从 emp_cdc 读取并发送到 Kafka
4. Consumer 能消费 Kafka 消息并更新目标数据库
5. 数据同步延迟 < 1 秒
6. DLQ 表记录验证失败的消息

### ⭐ 加分项
- 使用 OOP 设计
- 完善的异常处理
- 详细的日志记录
- 测试脚本自动化验证
- Git 版本控制

---

## 时间估算

| 阶段 | 预计时间 |
|------|----------|
| 阶段 1：数据库设置 | 1-2 小时 |
| 阶段 2：配置框架 | 0.5 小时 |
| 阶段 3：Producer 实现 | 1-2 小时 |
| 阶段 4：Consumer 实现 | 1-2 小时 |
| 阶段 5：测试验证 | 1 小时 |
| 阶段 6：优化加分项 | 1 小时 |
| **总计** | **5.5-8.5 小时** |

---

## 常见问题与解决方案

### Q1: Docker 容器无法启动？
**A**: 检查端口占用，清理旧数据：`docker compose down -v`

### Q2: 触发器没有触发？
**A**: 检查触发器创建语法，查看数据库日志

### Q3: Consumer 消费不到消息？
**A**: 检查 Kafka 主题是否创建成功，Consumer Group ID 是否正确

### Q4: 数据同步延迟 > 1 秒？
**A**: 减少 Producer 轮询间隔，增加 Kafka 分区数

---

## 下一步

准备好后，按照阶段 1 开始实施。如需帮助实现任何阶段，请告知！
