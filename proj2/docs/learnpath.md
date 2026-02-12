# proj2 学习路径

这条路径适合完全小白，按顺序学习即可。

## 1) 先理解项目目标（5-10 分钟）
阅读：docs/PROJECT_PLAN.md
目标：知道这是一个 CDC 流程：源库变更 -> Kafka -> 目标库同步。

## 2) 看清整体流程（10-15 分钟）
阅读：proj2_flow.mmd
目标：理解从开始到结束的流程，以及涉及哪些文件。

## 3) 学会怎么跑起来（10 分钟）
阅读：docs/SCRIPTS_GUIDE.md
目标：知道如何启动、停止、测试系统。

## 4) 认识数据长什么样（10 分钟）
阅读：data/employees.csv
目标：熟悉员工数据字段。

## 5) 理解数据库结构（15-20 分钟）
阅读：
- sql/init_source_db.sql
- sql/init_target_db.sql
目标：理解源库表、目标库表、CDC 表、DLQ 表分别是什么。

## 6) 理解数据模型和校验（15 分钟）
阅读：src/employee.py
目标：理解 Employee 模型和校验规则。

## 7) 理解 Producer（20-30 分钟）
阅读：src/producer.py
目标：理解 CDC 表如何被读取并发送到 Kafka，以及 offset 的作用。

## 8) 理解 Consumer（20-30 分钟）
阅读：src/consumer.py
目标：理解 Kafka 消息如何写入目标库或 DLQ。

## 9) 理解测试（15-20 分钟）
阅读：
- src/test_cdc.py
- src/verify_sync.py
目标：理解测试覆盖场景和如何验证同步。

## 10) 跑一遍完整流程（30 分钟）
命令：
1. docker-compose up -d
2. python3 src/load_initial_data.py
3. ./scripts/start_cdc.sh
4. ./scripts/run_tests.sh
5. ./scripts/stop_cdc.sh

## 可选：快速回顾
- 源库：数据变化发生的地方
- Kafka：承载变更消息
- 目标库：接收同步结果
- DLQ：记录无效或失败的数据
