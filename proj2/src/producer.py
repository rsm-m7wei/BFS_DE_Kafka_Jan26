"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang
Modified: Enhanced for CDC Project 2

This file is part of Oct DE Batch Kafka Project 2 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import json
import os
import time
import psycopg2
from confluent_kafka import Producer
from employee import Employee

# 导入配置
try:
    from config import (
        DB_SOURCE_CONFIG,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC_CDC,
        POLL_INTERVAL,
        OFFSET_FILE,
        BATCH_SIZE
    )
except ImportError:
    # 如果没有config.py，使用默认值
    print("⚠️  Warning: config.py not found, using default values")
    DB_SOURCE_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres',
        'database': 'postgres'
    }
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
    KAFKA_TOPIC_CDC = 'bf_employee_cdc'
    POLL_INTERVAL = 5
    OFFSET_FILE = 'cdc_offset.txt'
    BATCH_SIZE = 100

# 主题名称（保持兼容性）
employee_topic_name = KAFKA_TOPIC_CDC

class cdcProducer(Producer):
    """
    CDC Producer - 从源数据库读取变更并发送到Kafka
    
    功能：
    1. 轮询emp_cdc表获取新的数据变更
    2. 将变更序列化为JSON
    3. 发送到Kafka主题
    4. 管理offset（记住处理进度）
    
    继承自confluent_kafka.Producer，增加了CDC特定功能
    """
    
    def __init__(self, host="localhost", port="29092"):
        """
        初始化Producer
        
        参数:
            host: Kafka服务器地址
            port: Kafka服务器端口
                  - 29092: Docker外部访问（你的Python脚本在电脑上运行）
                  - 9092: Docker内部访问（如果Python在容器里运行）
        """
        self.host = host
        self.port = port
        
        # Kafka Producer配置
        # ============ EOS（Exactly Once Semantics）配置 ============
        producerConfig = {
            'bootstrap.servers': f"{self.host}:{self.port}",
            'acks': 'all',  # 等待所有副本确认（最安全）
            # --------- Idempotent Producer 配置 ---------
            # 原理：即使消息重复发送，Kafka也能自动去重不提次
            'enable.idempotence': True,  # 启用幂等性 Producer（Kafka 1.0+默认）
            'max.in.flight.requests.per.connection': 5,  # 不声明地发送的请求数
            'linger.ms': 10,  # 批量发送：每10ms或积攒100条消息一起发送（改善吞吐量）
            'batch.size': 16384,  # 批次大小27KB（默认16KB）
            'compression.type': 'snappy',  # 使用Snappy压缩（减少带宽流量）
        }
        
        # 调用父类（Producer）的初始化
        super().__init__(producerConfig)
        
        # 运行状态标志
        self.running = True
        
        # 统计信息
        self.total_sent = 0
        self.total_errors = 0
        
        print("=" * 60)
        print("  CDC Producer Initialized")
        print("=" * 60)
        print(f"Kafka: {self.host}:{self.port}")
        print(f"Topic: {employee_topic_name}")
        print(f"Offset File: {OFFSET_FILE}")
        print("=" * 60)
    
    def load_offset(self):
        """
        从文件加载offset（上次处理到的action_id）
        
        返回:
            int: 上次处理的action_id，如果文件不存在返回0
        
        为什么需要？
        - Producer可能会重启（崩溃、更新等）
        - 需要记住上次处理到哪里，避免重复处理或遗漏
        
        例子:
            第一次运行：没有offset文件 → 返回0 → 从头开始
            第二次运行：读取offset文件 → 返回1000 → 从action_id=1001开始
        """
        try:
            if os.path.exists(OFFSET_FILE):
                with open(OFFSET_FILE, 'r') as f:
                    offset = int(f.read().strip())
                    print(f"📖 Loaded offset: {offset}")
                    return offset
            else:
                print("📝 No offset file found, starting from 0")
                return 0
        except Exception as e:
            print(f"⚠️  Error loading offset: {e}, starting from 0")
            return 0
    
    def save_offset(self, offset):
        """
        保存offset到文件
        
        参数:
            offset: 已处理的最大action_id
        
        为什么需要？
        - Producer处理完一批数据后，保存进度
        - 下次启动时可以从这个位置继续
        
        例子:
            处理了action_id 1-100的记录
            调用save_offset(100)
            文件内容变成："100"
        """
        try:
            with open(OFFSET_FILE, 'w') as f:
                f.write(str(offset))
            # 不需要每次都打印，太吵了
            # print(f"💾 Saved offset: {offset}")
        except Exception as e:
            print(f"❌ Error saving offset: {e}")
    
    def delivery_callback(self, err, msg):
        """
        Kafka消息发送后的回调函数
        
        参数:
            err: 错误信息（如果发送失败）
            msg: 发送的消息对象
        
        什么是回调函数？
        - Producer发送消息是异步的（不等待结果）
        - 消息发送成功/失败后，Kafka会调用这个函数通知你
        
        比喻：
        - 你发快递（发送消息）
        - 不用等在柜台（异步）
        - 快递员发短信告诉你"已发货"或"发送失败"（回调）
        """
        if err is not None:
            # 发送失败
            print(f"❌ Message delivery failed: {err}")
            self.total_errors += 1
        else:
            # 发送成功
            self.total_sent += 1
            # 只在重要里程碑打印（每100条）
            if self.total_sent % 100 == 0:
                print(f"📤 Sent {self.total_sent} messages (partition={msg.partition()}, offset={msg.offset()})")
    
    def fetch_cdc(self, last_action_id=0):
        """
        从emp_cdc表获取新的变更记录
        
        参数:
            last_action_id: 上次处理的action_id，只获取大于这个值的记录
        
        返回:
            list[Employee]: Employee对象列表
        
        工作流程:
        1. 连接源数据库
        2. 查询 emp_cdc WHERE action_id > last_action_id
        3. 将查询结果转换为Employee对象
        4. 返回Employee列表
        """
        employees = []
        
        try:
            # 连接源数据库
            conn = psycopg2.connect(**DB_SOURCE_CONFIG)
            cur = conn.cursor()
            
            # SQL查询：获取比last_action_id大的所有变更
            # ORDER BY action_id：按顺序处理
            # LIMIT BATCH_SIZE：一次不要取太多，避免内存溢出
            query = """
                SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
                FROM emp_cdc
                WHERE action_id > %s
                ORDER BY action_id
                LIMIT %s
            """
            
            cur.execute(query, (last_action_id, BATCH_SIZE))
            rows = cur.fetchall()
            
            # 将数据库行转换为Employee对象
            for row in rows:
                employee = Employee.from_line(row)
                employees.append(employee)
            
            if len(employees) > 0:
                print(f"📊 Fetched {len(employees)} CDC records (action_id {employees[0].action_id} to {employees[-1].action_id})")
            
            cur.close()
            conn.close()
            
        except psycopg2.OperationalError as e:
            print(f"❌ Database connection error: {e}")
            print("💡 Make sure Docker containers are running: docker compose ps")
        except Exception as e:
            print(f"❌ Error fetching CDC data: {e}")
        
        return employees
    
    def run(self):
        """
        Producer主循环 - 持续监控CDC表并发送消息
        
        工作流程（无限循环）：
        1. 加载offset（知道上次处理到哪里）
        2. 查询CDC表获取新变更
        3. 遍历每条变更：
           a. 序列化为JSON
           b. 发送到Kafka
        4. 保存新的offset
        5. 等待POLL_INTERVAL秒
        6. 重复...
        
        终止条件：
        - Ctrl+C（KeyboardInterrupt）
        - self.running = False
        """
        print("\n🚀 Producer starting...")
        print(f"⏰ Polling interval: {POLL_INTERVAL} seconds")
        print("Press Ctrl+C to stop\n")
        
        try:
            while self.running:
                # 1. 加载offset（读取进度）
                last_action_id = self.load_offset()
                
                # 2. 获取新的CDC记录
                employees = self.fetch_cdc(last_action_id)
                
                if len(employees) == 0:
                    # 没有新数据
                    print(f"😴 No new CDC records, waiting {POLL_INTERVAL}s...")
                else:
                    # 有新数据，开始处理
                    print(f"📦 Processing {len(employees)} records...")
                    
                    for employee in employees:
                        # 3. 序列化为JSON
                        json_str = employee.to_json()
                        
                        # 4. 发送到Kafka
                        # produce()是异步的，不会阻塞
                        self.produce(
                            topic=employee_topic_name,
                            key=str(employee.emp_id).encode('utf-8'),
                            value=json_str.encode('utf-8'),  # Kafka需要bytes
                            callback=self.delivery_callback  # 发送完成后调用
                        )
                        
                        # 更新最新的action_id
                        last_action_id = employee.action_id
                    
                    # 5. 等待所有消息发送完成
                    # flush()会阻塞，直到所有消息都发送完毕
                    remaining = self.flush(timeout=10.0)
                    if remaining > 0:
                        print(f"⚠️  {remaining} messages failed to send")
                    
                    # 6. 保存offset（记录进度）
                    self.save_offset(last_action_id)
                    print(f"✅ Batch complete. Last action_id: {last_action_id}")
                
                # 7. 等待一段时间再检查
                time.sleep(POLL_INTERVAL)
        
        except KeyboardInterrupt:
            print("\n\n🛑 Received Ctrl+C, shutting down gracefully...")
            self.running = False
        
        except Exception as e:
            print(f"\n❌ Producer error: {e}")
            self.running = False
        
        finally:
            # 清理资源
            print("\n📊 Producer Statistics:")
            print(f"  Total sent: {self.total_sent}")
            print(f"  Total errors: {self.total_errors}")
            print("\n👋 Producer stopped. Goodbye!")


if __name__ == '__main__':
    """
    主程序入口
    
    使用方法：
        python producer.py
    
    要求：
    1. Docker容器已启动（Kafka和源数据库）
    2. 源数据库已初始化（有emp_cdc表）
    3. Kafka主题已创建（bf_employee_cdc）
    """
    
    print("""
    ╔════════════════════════════════════════════════╗
    ║      Kafka CDC Producer - Project 2           ║
    ║                                                ║
    ║  Monitors emp_cdc table and sends changes     ║
    ║  to Kafka for real-time data synchronization  ║
    ╚════════════════════════════════════════════════╝
    """)
    
    # 创建Producer实例
    producer = cdcProducer()
    
    # 启动主循环
    producer.run()
    
