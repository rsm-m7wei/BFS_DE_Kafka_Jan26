# 项目2 Tutor 手把手说明

本文面向完全新手，讲清楚我是如何从零思考结构、拆分步骤、逐步写代码，并最终形成现在的工程。

## 1. 先画出整体数据流（确定方向）
目标是“源库变更 -> Kafka -> 目标库同步”。最少要有四个环节：
1. 源数据库能记录变更（CDC 表和触发器）。
2. Producer 能把变更发到 Kafka。
3. Consumer 能从 Kafka 读消息并写入目标库。
4. 需要验证同步效果和失败数据的去向（DLQ）。

因此先定下数据流：
源库 employees 表发生 INSERT/UPDATE/DELETE -> 触发器把数据写入 emp_cdc -> Producer 读 emp_cdc -> Kafka 主题 -> Consumer 写入目标库 employees，验证失败写入 emp_cdc_dlq。

## 2. 先把“环境骨架”搭好
新手最容易卡在环境，所以第一步是让容器和数据库结构稳定。

### 2.1 Docker Compose 搭 Kafka + 两个数据库
核心原因：Kafka 和两个 PostgreSQL 必须一直稳定运行，方便后面脚本直接连接。
- 配置文件在 [proj2/docker-compose.yml](proj2/docker-compose.yml)
- 包含 Zookeeper、Kafka、Kafka 主题初始化、源库、目标库

### 2.2 数据库初始化脚本
原因：数据库需要一开始就有表和触发器，否则后续逻辑无法跑。
- 源库脚本在 [proj2/sql/init_source_db.sql](proj2/sql/init_source_db.sql)
  - 创建 employees 表
  - 创建 emp_cdc 表
  - 创建触发器，把所有变更记录到 emp_cdc
- 目标库脚本在 [proj2/sql/init_target_db.sql](proj2/sql/init_target_db.sql)
  - 创建 employees 表
  - 创建 emp_cdc_dlq 表（死信队列，存验证失败的记录）

## 3. 统一配置，避免到处写死
新手最容易出错的点是端口、用户名、主题名写散了。解决方法是统一配置文件。
- 配置文件在 [proj2/src/config.py](proj2/src/config.py)
- 包含：数据库连接、Kafka 主题名、轮询间隔、校验规则等
- 这样以后只改一个文件即可。

## 4. 定义“数据模型”，让 Producer/Consumer 有共同语言
Producer 和 Consumer 都要处理同一种数据，因此先定义一个 Employee 类。
- 数据模型在 [proj2/src/employee.py](proj2/src/employee.py)
- 关键设计：
  - from_line：数据库行转对象
  - to_json / from_json：Kafka 传输用 JSON
  - validate：数据校验规则，失败进入 DLQ

这样做的原因：避免每个脚本都在重复解析字段，也方便以后统一改规则。

## 5. 先把“数据来源”准备好
源库没有数据，CDC 就没有消息，所以先写 CSV 导入脚本。
- 数据加载脚本在 [proj2/src/load_initial_data.py](proj2/src/load_initial_data.py)
- 数据文件在 [proj2/data/employees.csv](proj2/data/employees.csv)
- 功能：
  - 等待数据库就绪
  - 读取 CSV
  - 插入源库 employees（触发器自动写 emp_cdc）

## 6. Producer：从 emp_cdc 读数据发到 Kafka
Producer 的职责是“只负责发送，不负责业务校验”。
- 代码在 [proj2/src/producer.py](proj2/src/producer.py)
- 核心思路：
  1. 读取 offset 文件（知道上次处理到哪一条）
  2. 从 emp_cdc 查询比 offset 新的数据
  3. 转成 JSON 发到 Kafka
  4. 保存 offset，避免重复发送

为什么要 offset：重启时能继续处理，不会重复。

## 7. Consumer：消费消息并更新目标库
Consumer 要保证“数据写入正确 + 不合法数据进 DLQ”。
- 代码在 [proj2/src/consumer.py](proj2/src/consumer.py)
- 核心思路：
  1. 从 Kafka 消费 JSON
  2. 还原成 Employee 对象并 validate
  3. 合法：写入目标库（INSERT/UPDATE/DELETE）
  4. 不合法：写入 emp_cdc_dlq

为什么要用 UPSERT：消息可能重复消费，目标库不能因为主键冲突报错。

## 8. 一键启动和停止（降低新手操作成本）
命令太多，新手容易出错，所以写了脚本。
- 启动脚本 [proj2/scripts/start_cdc.sh](proj2/scripts/start_cdc.sh)
  - 检查 Docker
  - 启动 Producer/Consumer 并保存 PID
  - 自动打印日志
- 停止脚本 [proj2/scripts/stop_cdc.sh](proj2/scripts/stop_cdc.sh)
  - 根据 PID 停止进程

## 9. 自动测试与验证
新手最怕“我以为成功，其实没同步”。因此提供两个验证工具。
- 功能测试脚本 [proj2/src/test_cdc.py](proj2/src/test_cdc.py)
  - 测 INSERT/UPDATE/DELETE/DLQ
- 一致性验证脚本 [proj2/src/verify_sync.py](proj2/src/verify_sync.py)
  - 比较源库和目标库是否一致
- 一键测试脚本 [proj2/scripts/run_tests.sh](proj2/scripts/run_tests.sh)
  - 调用 verify_sync + test_cdc，并检查容器状态

## 10. 最终运行顺序（给新手的固定流程）
1. 进入项目目录，启动 Docker：
   docker compose up -d
2. 等待 30 秒，执行数据加载：
   python3 src/load_initial_data.py
3. 启动 CDC 系统：
   ./scripts/start_cdc.sh
4. 运行测试：
   ./scripts/run_tests.sh

## 11. 这个工程的“结构思路”总结
- 先解决环境和数据库结构（可运行的基础）
- 再定义统一配置和数据模型（减少重复和错误）
- 然后实现 Producer/Consumer 主流程（核心功能）
- 最后补齐脚本和测试（让新手能稳定使用）

如果你是第一次接触这个项目，请从第 10 节的运行顺序开始做一遍，就能理解整体流程。
