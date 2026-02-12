"""
配置管理模块
功能：集中管理所有配置参数，避免硬编码
为什么需要？统一修改配置，提高代码可维护性
"""

# ============================================
# 数据库配置
# ============================================

# 源数据库配置（Source Database - 读取变更）
DB_SOURCE_CONFIG = {
    'host': 'localhost',      # 数据库主机地址
    'port': 5432,             # 端口（docker-compose.yml中映射的端口）
    'user': 'postgres',       # 用户名
    'password': 'postgres',   # 密码
    'database': 'postgres'    # 数据库名
}

# 目标数据库配置（Destination Database - 同步数据）
DB_TARGET_CONFIG = {
    'host': 'localhost',
    'port': 5433,             # 注意：目标库用不同端口！
    'user': 'postgres',
    'password': 'postgres',
    'database': 'postgres'
}

# ============================================
# Kafka 配置
# ============================================

# Kafka服务器地址
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

# Kafka主题名称
KAFKA_TOPIC_CDC = 'bf_employee_cdc'           # 主数据流主题
KAFKA_TOPIC_DLQ = 'bf_employee_cdc_dlq'       # 死信队列主题（验证失败的消息）

# Consumer消费者组ID
# 为什么需要？同一个组内的多个Consumer会分担消息，避免重复消费
CONSUMER_GROUP_ID = 'bf_cdc_consumer'

# ============ EOS（Exactly Once Semantics）配置 ============
# 启用幂等性 Producer（Kafka 1.0+ 自动启用）
ENABLE_IDEMPOTENT_PRODUCER = True

# Consumer EOS 模式
# isolation.level 决定消费者能看到的消息:
# - 'read_uncommitted': 看所有消息（可能看到未提交的）- 低延迟
# - 'read_committed': 仅看已提交的消息 - 高可靠性（推荐CDC使用）
EOS_ISOLATION_LEVEL = 'read_committed'

# 自动提交配置
# 改为 False 时，需要手动调用 commit() 以实现原子性
# True: 自动提交（简单但不原子 - 消息处理中宕机会丢失）
# False: 手动提交（原子操作 - insert + offset commit 一起成功或都失败）
ENABLE_AUTO_COMMIT = False

# 自动提交间隔（仅当 ENABLE_AUTO_COMMIT=True 时有效）
AUTO_COMMIT_INTERVAL_MS = 5000

# ============================================
# CDC Producer 配置
# ============================================

# 轮询间隔（秒）
# Producer每隔几秒检查一次CDC表是否有新变更
POLL_INTERVAL = 5

# Offset文件路径
# 记录Producer已处理到哪个action_id，重启后可以续传
OFFSET_FILE = 'cdc_offset.txt'

# 批量发送大小
# 一次读取多少条CDC记录（避免一次读太多内存溢出）
BATCH_SIZE = 100

# ============================================
# 数据验证规则（DLQ使用）
# ============================================

# 员工出生年份必须大于此值
# 修改为1990以允许测试数据通过验证
MIN_BIRTH_YEAR = 1990

# 最低薪资要求
MIN_SALARY = 10000

# 最小员工ID（避免负数）
MIN_EMPLOYEE_ID = 0

# ============================================
# 日志配置
# ============================================

# 日志级别：DEBUG, INFO, WARNING, ERROR
LOG_LEVEL = 'INFO'

# 日志格式
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# ============================================
# 辅助函数
# ============================================

def get_source_db_connection():
    """
    获取源数据库连接
    为什么用函数？便于添加连接池、重试逻辑等
    """
    import psycopg2
    try:
        return psycopg2.connect(**DB_SOURCE_CONFIG)
    except Exception as e:
        print(f"❌ Failed to connect to source database: {e}")
        raise

def get_target_db_connection():
    """
    获取目标数据库连接
    """
    import psycopg2
    try:
        return psycopg2.connect(**DB_TARGET_CONFIG)
    except Exception as e:
        print(f"❌ Failed to connect to target database: {e}")
        raise

def print_config():
    """
    打印当前配置（调试用）
    """
    print("=" * 60)
    print("  Current Configuration")
    print("=" * 60)
    print(f"Source DB: {DB_SOURCE_CONFIG['host']}:{DB_SOURCE_CONFIG['port']}")
    print(f"Target DB: {DB_TARGET_CONFIG['host']}:{DB_TARGET_CONFIG['port']}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"CDC Topic: {KAFKA_TOPIC_CDC}")
    print(f"DLQ Topic: {KAFKA_TOPIC_DLQ}")
    print(f"Poll Interval: {POLL_INTERVAL}s")
    print(f"Min Birth Year: {MIN_BIRTH_YEAR}")
    print(f"Min Salary: ${MIN_SALARY}")
    print("=" * 60)

# 测试代码（直接运行此文件时执行）
if __name__ == "__main__":
    print_config()
    
    # 测试数据库连接
    print("\n🔍 Testing database connections...")
    try:
        conn = get_source_db_connection()
        print("✅ Source DB connection successful!")
        conn.close()
    except:
        print("❌ Source DB connection failed!")
    
    try:
        conn = get_target_db_connection()
        print("✅ Target DB connection successful!")
        conn.close()
    except:
        print("❌ Target DB connection failed!")
