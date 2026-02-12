# Kafka CDC System - Quick Start Guide

## 📋 脚本说明

### 1. start_cdc.sh - 启动 CDC 系统
启动 Producer 和 Consumer 进程。

**基本用法：**
```bash
./scripts/start_cdc.sh
```

**重置 offset 并启动：**
```bash
./scripts/start_cdc.sh --reset
```

**功能：**
- ✅ 检查 Docker 容器是否运行
- ✅ 清理旧的进程
- ✅ 启动 Producer（后台运行）
- ✅ 启动 Consumer（后台运行）
- ✅ 显示最新日志
- ✅ 可选：运行数据验证

---

### 2. stop_cdc.sh - 停止 CDC 系统
优雅地停止 Producer 和 Consumer。

**基本用法：**
```bash
./scripts/stop_cdc.sh
```

**停止并显示日志：**
```bash
./scripts/stop_cdc.sh --show-logs
```

**功能：**
- ✅ 停止 Producer 进程
- ✅ 停止 Consumer 进程
- ✅ 清理 PID 文件
- ✅ 可选：显示最后的日志

---

### 3. run_tests.sh - 运行测试套件
运行完整的测试和验证。

**用法：**
```bash
./scripts/run_tests.sh
```

**功能：**
- ✅ 检查系统状态
- ✅ 运行数据同步验证
- ✅ 运行功能测试（INSERT/UPDATE/DELETE/DLQ）
- ✅ 检查 Docker 容器健康
- ✅ 显示数据库统计信息
- ✅ 生成测试报告

---

## 🚀 快速开始（5 分钟）

### Step 1: 启动 Docker 服务
```bash
docker compose up -d
```
等待 30 秒让服务启动。

### Step 2: 加载初始数据
```bash
python3 src/load_initial_data.py
```

### Step 3: 启动 CDC 系统
```bash
./scripts/start_cdc.sh
```

### Step 4: 运行测试
```bash
./scripts/run_tests.sh
```

---

## 📊 常用命令

### 查看实时日志
```bash
# Producer 日志
tail -f logs/producer.log

# Consumer 日志
tail -f logs/consumer.log
```

### 手动验证数据同步
```bash
python3 src/verify_sync.py
```

### 手动运行功能测试
```bash
python3 src/test_cdc.py
```

### 检查进程状态
```bash
ps aux | grep -E "(producer|consumer).py"
```

### 检查 Kafka topics
```bash
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 查看 Kafka 消息
```bash
# 查看 CDC 主题
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bf_employee_cdc \
  --from-beginning

# 查看 DLQ 主题
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning
```

---

## 🔧 故障排查

### 问题 1：端口已被占用
```bash
# 查看占用端口的进程
lsof -i :5432
lsof -i :5433
lsof -i :29092

# 停止旧容器
docker stop <container_id>
```

### 问题 2：Producer/Consumer 无法启动
```bash
# 查看错误日志
cat logs/producer.log
cat logs/consumer.log

# 确认数据库连接
python3 -c "import psycopg2; psycopg2.connect(host='localhost', port=5432, user='postgres', password='postgres'); print('✅ Source DB OK')"
python3 -c "import psycopg2; psycopg2.connect(host='localhost', port=5433, user='postgres', password='postgres'); print('✅ Target DB OK')"
```

### 问题 3：数据未同步
```bash
# 1. 检查 CDC 表是否有记录
psql -h localhost -p 5432 -U postgres -c "SELECT * FROM emp_cdc;"

# 2. 重置 offset 并重启
./scripts/stop_cdc.sh
rm -f cdc_offset.txt
./scripts/start_cdc.sh

# 3. 检查 DLQ（可能是验证失败）
python3 -c "
import psycopg2
conn = psycopg2.connect(host='localhost', port=5433, user='postgres', password='postgres')
cur = conn.cursor()
cur.execute('SELECT * FROM emp_cdc_dlq')
rows = cur.fetchall()
print(f'DLQ records: {len(rows)}')
for row in rows:
    print(row)
"
```

### 问题 4：完全重置系统
```bash
# 停止所有服务
./scripts/stop_cdc.sh
docker compose down -v

# 清理文件
rm -f cdc_offset.txt
rm -f logs/*.log

# 重新启动
docker compose up -d
sleep 30
python3 src/load_initial_data.py
./scripts/start_cdc.sh --reset
```

---

## 📁 文件结构

```
proj2/
├── scripts/
│   ├── start_cdc.sh          # 启动脚本
│   ├── stop_cdc.sh           # 停止脚本
│   └── run_tests.sh          # 测试脚本
├── src/
│   ├── producer.py           # CDC Producer
│   ├── consumer.py           # CDC Consumer
│   ├── config.py             # 配置文件
│   ├── employee.py           # 数据模型
│   ├── load_initial_data.py  # 数据加载工具
│   ├── verify_sync.py        # 同步验证工具
│   └── test_cdc.py           # 功能测试套件
├── sql/
│   ├── init_source_db.sql    # 源数据库初始化
│   └── init_target_db.sql    # 目标数据库初始化
├── data/
│   └── employees.csv         # 初始数据
├── docs/
│   ├── PROJECT_PLAN.md       # 项目计划
│   ├── SCRIPTS_GUIDE.md       # 脚本说明
│   └── tutor.md              # 新手教程
├── logs/                 # 日志目录
│   ├── producer.log
│   └── consumer.log
├── .producer.pid         # Producer 进程 ID
└── .consumer.pid         # Consumer 进程 ID
```

---

## 💡 提示

1. **首次运行**：使用 `./scripts/start_cdc.sh --reset` 确保从头开始处理
2. **调试模式**：查看实时日志 `tail -f logs/*.log`
3. **性能监控**：使用 `./scripts/run_tests.sh` 定期检查系统健康
4. **数据验证**：每次修改后运行 `python3 src/verify_sync.py`

---

## ✅ 成功标志

当你看到以下输出时，系统运行正常：

**Producer:**
```
📊 Fetched X CDC records
✅ Message delivered: emp_id=X
💾 Saved offset: X
```

**Consumer:**
```
✅ Processing INSERT for emp_id=X
✅ Processing UPDATE for emp_id=X
✅ Processing DELETE for emp_id=X
```

**验证:**
```
🎉 Perfect synchronization!
✅ All employees in source database are synced
```

---

**Happy Coding! 🎉**
