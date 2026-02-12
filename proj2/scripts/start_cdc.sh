#!/bin/bash

# CDC 系统启动脚本
# 功能：自动启动 Producer 和 Consumer，并验证系统状态

echo "╔════════════════════════════════════════════════╗"
echo "║         Kafka CDC System Launcher              ║"
echo "║                                                ║"
echo "║  Starting Producer and Consumer...             ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# 获取项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# 检查 Docker 容器是否运行
echo "🔍 Checking Docker containers..."
if ! docker compose ps | grep -q "Up"; then
    echo "❌ Docker containers are not running!"
    echo "💡 Please run: docker compose up -d"
    exit 1
fi
echo "✅ Docker containers are running"
echo ""

# 检查必要的文件是否存在
required_files=("src/producer.py" "src/consumer.py" "src/config.py" "src/employee.py")
for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Required file not found: $file"
        exit 1
    fi
done
echo "✅ All required files found"
echo ""

# 停止已存在的 Producer/Consumer 进程
echo "🧹 Cleaning up existing processes..."
pkill -f "python.*producer.py" 2>/dev/null
pkill -f "python.*consumer.py" 2>/dev/null
sleep 1
echo "✅ Cleanup complete"
echo ""

# 可选：重置 offset（如果参数为 --reset）
if [ "$1" = "--reset" ]; then
    echo "🔄 Resetting offset..."
    rm -f cdc_offset.txt
    echo "✅ Offset reset"
    echo ""
fi

# 启动 Producer
echo "🚀 Starting Producer..."
nohup python3 src/producer.py > logs/producer.log 2>&1 &
PRODUCER_PID=$!
echo "✅ Producer started (PID: $PRODUCER_PID)"
sleep 2

# 检查 Producer 是否还在运行
if ! ps -p $PRODUCER_PID > /dev/null; then
    echo "❌ Producer failed to start! Check logs/producer.log"
    exit 1
fi

# 启动 Consumer
echo "🚀 Starting Consumer..."
nohup python3 src/consumer.py > logs/consumer.log 2>&1 &
CONSUMER_PID=$!
echo "✅ Consumer started (PID: $CONSUMER_PID)"
sleep 2

# 检查 Consumer 是否还在运行
if ! ps -p $CONSUMER_PID > /dev/null; then
    echo "❌ Consumer failed to start! Check logs/consumer.log"
    exit 1
fi

# 保存 PID 到文件
echo $PRODUCER_PID > .producer.pid
echo $CONSUMER_PID > .consumer.pid

echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║         CDC System Started Successfully!       ║"
echo "╚════════════════════════════════════════════════╝"
echo ""
echo "📊 System Status:"
echo "   Producer PID: $PRODUCER_PID"
echo "   Consumer PID: $CONSUMER_PID"
echo ""
echo "📝 Logs:"
echo "   Producer: tail -f logs/producer.log"
echo "   Consumer: tail -f logs/consumer.log"
echo ""
echo "🛑 To stop the system:"
echo "   ./scripts/stop_cdc.sh"
echo ""

# 等待系统稳定
echo "⏳ Waiting 5 seconds for system to stabilize..."
sleep 5

# 显示最新日志
echo ""
echo "════════════════════════════════════════"
echo "  Producer Log (last 10 lines)"
echo "════════════════════════════════════════"
tail -10 logs/producer.log
echo ""
echo "════════════════════════════════════════"
echo "  Consumer Log (last 10 lines)"
echo "════════════════════════════════════════"
tail -10 logs/consumer.log
echo ""

# 询问是否运行验证
echo "════════════════════════════════════════"
read -p "📊 Run data verification now? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    python3 src/verify_sync.py
fi

echo ""
echo "✅ Setup complete! The CDC system is now running."
echo ""
