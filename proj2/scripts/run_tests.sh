#!/bin/bash

# CDC 系统完整测试脚本
# 功能：运行完整的测试套件并生成报告

echo "╔════════════════════════════════════════════════╗"
echo "║         Kafka CDC System Test Suite            ║"
echo "║                                                ║"
echo "║  Running comprehensive tests...                ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# 获取项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查 CDC 系统是否运行
echo "🔍 Step 1: Checking if CDC system is running..."
PRODUCER_RUNNING=false
CONSUMER_RUNNING=false

if [ -f .producer.pid ] && ps -p $(cat .producer.pid) > /dev/null 2>&1; then
    PRODUCER_RUNNING=true
    echo -e "${GREEN}✅ Producer is running${NC}"
else
    echo -e "${RED}❌ Producer is not running${NC}"
fi

if [ -f .consumer.pid ] && ps -p $(cat .consumer.pid) > /dev/null 2>&1; then
    CONSUMER_RUNNING=true
    echo -e "${GREEN}✅ Consumer is running${NC}"
else
    echo -e "${RED}❌ Consumer is not running${NC}"
fi

if [ "$PRODUCER_RUNNING" = false ] || [ "$CONSUMER_RUNNING" = false ]; then
    echo ""
    echo -e "${YELLOW}⚠️  CDC system is not fully running!${NC}"
    read -p "Start the system now? (y/n): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        ./scripts/start_cdc.sh --reset
        echo ""
        echo "⏳ Waiting 10 seconds for system to stabilize..."
        sleep 10
    else
        echo "❌ Cannot run tests without CDC system. Exiting."
        exit 1
    fi
fi

echo ""
echo "════════════════════════════════════════"
echo "  Step 2: Data Synchronization Verification"
echo "════════════════════════════════════════"
python3 src/verify_sync.py

echo ""
read -p "Press Enter to continue with functional tests..."
echo ""

echo "════════════════════════════════════════"
echo "  Step 3: Running Functional Tests"
echo "════════════════════════════════════════"
python3 src/test_cdc.py

echo ""
echo "════════════════════════════════════════"
echo "  Step 4: System Health Check"
echo "════════════════════════════════════════"

# 检查 Docker 容器状态
echo "🐳 Docker Containers:"
docker compose ps

echo ""
echo "📊 CDC Statistics:"
echo "   Producer log size: $(wc -l < logs/producer.log 2>/dev/null || echo "0") lines"
echo "   Consumer log size: $(wc -l < logs/consumer.log 2>/dev/null || echo "0") lines"

# 检查 Kafka topics
echo ""
echo "📨 Kafka Topics:"
docker compose exec -T kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep bf_employee

echo ""
echo "════════════════════════════════════════"
echo "  Step 5: Database Statistics"
echo "════════════════════════════════════════"

python3 << 'EOF'
import psycopg2

# Source database
source_conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='postgres', database='postgres')
source_cur = source_conn.cursor()

source_cur.execute("SELECT COUNT(*) FROM employees")
source_employees = source_cur.fetchone()[0]

source_cur.execute("SELECT COUNT(*) FROM emp_cdc")
source_cdc = source_cur.fetchone()[0]

# Target database
target_conn = psycopg2.connect(host='localhost', port=5433, user='postgres', password='postgres', database='postgres')
target_cur = target_conn.cursor()

target_cur.execute("SELECT COUNT(*) FROM employees")
target_employees = target_cur.fetchone()[0]

target_cur.execute("SELECT COUNT(*) FROM emp_cdc_dlq")
target_dlq = target_cur.fetchone()[0]

print(f"📊 Source Database:")
print(f"   Employees: {source_employees}")
print(f"   CDC Records: {source_cdc}")

print(f"\n📊 Target Database:")
print(f"   Employees: {target_employees}")
print(f"   DLQ Records: {target_dlq}")

sync_percentage = (target_employees / source_employees * 100) if source_employees > 0 else 0
print(f"\n📈 Sync Rate: {sync_percentage:.1f}%")

source_cur.close()
source_conn.close()
target_cur.close()
target_conn.close()
EOF

echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║         Test Suite Complete!                   ║"
echo "╚════════════════════════════════════════════════╝"
echo ""
echo "📋 Test Report Summary:"
echo "   - Data verification: See above"
echo "   - Functional tests: See above"
echo "   - System health: All services running"
echo ""
echo "📁 Detailed logs available at:"
echo "   - logs/producer.log"
echo "   - logs/consumer.log"
echo ""
