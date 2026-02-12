#!/bin/bash

# CDC 系统停止脚本
# 功能：优雅地停止 Producer 和 Consumer

echo "╔════════════════════════════════════════════════╗"
echo "║         Stopping Kafka CDC System              ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# 获取项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# 从 PID 文件读取进程 ID
if [ -f .producer.pid ]; then
    PRODUCER_PID=$(cat .producer.pid)
    if ps -p $PRODUCER_PID > /dev/null 2>&1; then
        echo "🛑 Stopping Producer (PID: $PRODUCER_PID)..."
        kill $PRODUCER_PID
        sleep 1
        # 强制杀死如果还在运行
        if ps -p $PRODUCER_PID > /dev/null 2>&1; then
            kill -9 $PRODUCER_PID 2>/dev/null
        fi
        echo "✅ Producer stopped"
    else
        echo "⚠️  Producer not running (PID: $PRODUCER_PID)"
    fi
    rm -f .producer.pid
else
    echo "⚠️  Producer PID file not found"
    # 尝试通过进程名杀死
    pkill -f "python.*producer.py" 2>/dev/null && echo "✅ Producer stopped"
fi

if [ -f .consumer.pid ]; then
    CONSUMER_PID=$(cat .consumer.pid)
    if ps -p $CONSUMER_PID > /dev/null 2>&1; then
        echo "🛑 Stopping Consumer (PID: $CONSUMER_PID)..."
        kill $CONSUMER_PID
        sleep 1
        # 强制杀死如果还在运行
        if ps -p $CONSUMER_PID > /dev/null 2>&1; then
            kill -9 $CONSUMER_PID 2>/dev/null
        fi
        echo "✅ Consumer stopped"
    else
        echo "⚠️  Consumer not running (PID: $CONSUMER_PID)"
    fi
    rm -f .consumer.pid
else
    echo "⚠️  Consumer PID file not found"
    # 尝试通过进程名杀死
    pkill -f "python.*consumer.py" 2>/dev/null && echo "✅ Consumer stopped"
fi

echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║         CDC System Stopped                     ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# 显示最后的日志
if [ "$1" = "--show-logs" ]; then
    echo "════════════════════════════════════════"
    echo "  Producer Final Log (last 20 lines)"
    echo "════════════════════════════════════════"
    tail -20 logs/producer.log 2>/dev/null || echo "No logs found"
    echo ""
    echo "════════════════════════════════════════"
    echo "  Consumer Final Log (last 20 lines)"
    echo "════════════════════════════════════════"
    tail -20 logs/consumer.log 2>/dev/null || echo "No logs found"
    echo ""
fi

echo "💡 Tips:"
echo "   - To restart: ./scripts/start_cdc.sh"
echo "   - To reset offset: ./scripts/start_cdc.sh --reset"
echo "   - To view logs: ./scripts/stop_cdc.sh --show-logs"
echo ""
