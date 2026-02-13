# CDC 项目 30min 汇报脚本（讲逻辑 + 展示代码）

> 目标：每一段都给出可直接照读的话术，并明确要展示的代码与功能说明。

## 0-2 min 开场与目标
**话术（可直接读）**
- 大家好，我今天汇报的是一个基于 Kafka 的 CDC 系统。核心目标是把源库的员工表变更，实时同步到目标库。
- 我们重点解决三件事：实时性、数据一致性、以及异常数据的隔离处理，也就是 DLQ。
- 接下来我会按“基础设施 → 变更捕获 → Producer → Kafka → Consumer → 校验与 DLQ → 测试与验证”的顺序讲，并同步打开关键代码。

**展示代码与功能说明**
- 项目入口结构，用来说明组件分层（容器、数据库、代码、测试脚本）。
- [../docker-compose.yml](../docker-compose.yml#L1-L88) 负责整体容器化部署。
- [../sql/init_source_db.sql](../sql/init_source_db.sql#L1-L62) 负责源库结构和 CDC 触发器。
- [../sql/init_target_db.sql](../sql/init_target_db.sql#L1-L34) 负责目标库和 DLQ 表结构。

## 2-6 min 基础设施与 Kafka 主题
**话术（可直接读）**
- 这一段我先从基础设施讲起，所有组件都通过 Docker Compose 启动：Zookeeper、Kafka、源库和目标库。
- Kafka 里创建了两个主题：主 CDC 主题有 3 个分区，按 `emp_id` 做 key，保证同一员工的事件顺序；DLQ 主题只有 1 个分区，便于集中排查异常数据。
- 同时我们启用 Log Compaction，用来保留每个 key 的最新值，这样新消费者可以快速回放最新状态。

**展示代码与功能说明**
- 主题创建、分区设置与 Log Compaction 配置。
- [../docker-compose.yml](../docker-compose.yml#L10-L57)

## 6-10 min 数据捕获（数据库触发器）
**话术（可直接读）**
- 变更捕获在数据库层完成，业务主表是 `employees`，所有变更会被触发器写入 `emp_cdc` 表。
- 触发器会记录 INSERT、UPDATE、DELETE 的完整快照和操作类型，形成一个标准的变更日志。
- 这样做有两个好处：不需要扫描全表，成本低；并且保留操作语义，后续处理更清晰。

**展示代码与功能说明**
- 主表、CDC 表、触发器函数与触发器绑定逻辑。
- [../sql/init_source_db.sql](../sql/init_source_db.sql#L6-L62)

## 10-12 min 统一配置与规则
**话术（可直接读）**
- 所有关键配置都集中在 `config.py`，避免硬编码，方便一处修改全局生效。
- 这里包括数据库连接、Kafka 主题、Producer 轮询周期、批量大小，以及校验规则。
- 也把 EOS 相关的关键开关集中在一起，方便讲一致性策略。

**展示代码与功能说明**
- 配置项与验证规则的定义入口。
- [../src/config.py](../src/config.py#L11-L91)

## 12-18 min Producer：从 CDC 表到 Kafka
**话术（可直接读）**
- Producer 的职责是读取 `emp_cdc` 表的增量变更并发送到 Kafka。
- 这里用了幂等 Producer 和 `acks=all`，保证消息不丢失且不会重复写入。
- 通过 offset 文件记录处理进度，实现断点续传；批量发送 + 压缩提升吞吐。

**展示代码与功能说明**
- Producer 初始化与幂等配置，说明可靠性保证点。
- [../src/producer.py](../src/producer.py#L76-L101)

- offset 读取与保存，保证重启后不丢不重。
- [../src/producer.py](../src/producer.py#L121-L169)

- 拉取 CDC 记录的 SQL 与批量读取逻辑。
- [../src/producer.py](../src/producer.py#L201-L255)

- 主循环中“读变更 → 发送 Kafka → flush → 保存 offset”的完整链路。
- [../src/producer.py](../src/producer.py#L257-L321)

## 18-24 min Consumer：消费、校验与写入
**话术（可直接读）**
- Consumer 从 Kafka 消费消息，反序列化成 `Employee` 对象，然后做校验。
- 校验通过就写入目标库；校验失败就进入 DLQ，保证主链路不被污染。
- 处理完成后手动提交 offset，确保“处理成功才提交”，从而保证一致性。

**展示代码与功能说明**
- 消费循环、校验与手动提交 offset 的 EOS 逻辑。
- [../src/consumer.py](../src/consumer.py#L129-L205)

- 目标库写入逻辑：INSERT/UPDATE 用 UPSERT，DELETE 走删除，保证幂等。
- [../src/consumer.py](../src/consumer.py#L225-L285)

- DLQ 写入逻辑，保存失败原因和原始消息。
- [../src/consumer.py](../src/consumer.py#L296-L337)

## 24-26 min 校验规则与 DLQ 设计
**话术（可直接读）**
- 校验规则放在模型层，统一入口，后续扩展规则时只改一个地方。
- DLQ 表保存失败原因和原始消息，不影响主流程，同时方便人工修复。

**展示代码与功能说明**
- 校验规则实现入口。
- [../src/employee.py](../src/employee.py#L123-L165)

- DLQ 表结构定义。
- [../sql/init_target_db.sql](../sql/init_target_db.sql#L16-L34)

## 26-28 min 数据初始化与一致性验证
**话术（可直接读）**
- 初始数据通过 CSV 写入源库，触发器会自动生成 CDC 记录，所以无需额外处理。
- 一致性验证脚本会对比源库与目标库的全量数据，用来快速发现同步差异。

**展示代码与功能说明**
- CSV 初始化加载流程，强调“写源库即触发 CDC”。
- [../src/load_initial_data.py](../src/load_initial_data.py#L1-L102)

- 一致性验证对比逻辑。
- [../src/verify_sync.py](../src/verify_sync.py#L1-L180)

## 28-30 min 测试与收尾
**话术（可直接读）**
- 测试覆盖 INSERT、UPDATE、DELETE、DLQ 四个场景，验证主链路和异常链路都有效。
- 轮询等待是为了处理异步同步延迟，避免误判。
- 最后汇总输出一目了然，适合作为健康检查。

**展示代码与功能说明**
- INSERT / UPDATE 场景的写入与校验逻辑。
- [../src/test_cdc.py](../src/test_cdc.py#L99-L227)

- DELETE / DLQ 场景的验证逻辑。
- [../src/test_cdc.py](../src/test_cdc.py#L234-L386)

- 测试入口与汇总输出。
- [../src/test_cdc.py](../src/test_cdc.py#L393-L454)

---

## 备选 Q&A 速答（可直接读）
- 为什么用触发器而不是定时全表扫描？
  - 触发器只记录变更，避免全表扫描的 IO 成本，并且保留操作语义。
- 为什么要 DLQ？
  - 错误数据不阻塞主链路，同时保留原始消息方便修复和回放。
- 如何保障“恰好一次”？
  - Producer 端幂等 + Consumer 手动提交 offset，确保处理完成才提交。

## 演示顺序建议
1. 先讲架构与流程，再讲触发器和 Producer/Consumer 主链路。
2. 中段展示校验与 DLQ，最后用测试脚本收尾。
3. 如果时间足够，可打开 logs 演示实时同步效果。
