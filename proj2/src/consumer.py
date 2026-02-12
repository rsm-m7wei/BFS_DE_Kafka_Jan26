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
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from employee import Employee

# 导入配置
try:
    from config import (
        DB_TARGET_CONFIG,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC_CDC,
        CONSUMER_GROUP_ID,
        EOS_ISOLATION_LEVEL,
        ENABLE_AUTO_COMMIT,
        AUTO_COMMIT_INTERVAL_MS
    )
except ImportError:
    # 如果没有config.py，使用默认值
    print("⚠️  Warning: config.py not found, using default values")
    DB_TARGET_CONFIG = {
        'host': 'localhost',
        'port': 5433,  # 目标数据库端口
        'user': 'postgres',
        'password': 'postgres',
        'database': 'postgres'
    }
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
    KAFKA_TOPIC_CDC = 'bf_employee_cdc'
    CONSUMER_GROUP_ID = 'bf_cdc_consumer'
    EOS_ISOLATION_LEVEL = 'read_committed'
    ENABLE_AUTO_COMMIT = False
    AUTO_COMMIT_INTERVAL_MS = 5000

# 主题名称
employee_topic_name = KAFKA_TOPIC_CDC

class cdcConsumer(Consumer):
    """
    CDC Consumer - 从Kafka消费变更并同步到目标数据库
    
    功能：
    1. 从Kafka Topic消费消息
    2. 反序列化JSON为Employee对象
    3. 验证数据
    4. 根据action类型（INSERT/UPDATE/DELETE）更新目标数据库
    5. 验证失败的消息写入DLQ表
    """
    
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = CONSUMER_GROUP_ID):
        """
        初始化Consumer
        
        参数:
            host: Kafka服务器地址
            port: Kafka服务器端口
            group_id: Consumer Group ID
                      - 同一组内的Consumer会负载均衡
                      - 不同组的Consumer各自独立消费所有消息
        
        关键配置（EOS - Exactly Once Semantics）：
        - isolation.level: read_committed - 仅读已提交消息（确保一致性）
        - enable.auto.commit: False - 禁用自动提交，改为手动提交（原子性）
        - auto.offset.reset: earliest - 从最早的消息开始（确保不漏）
        
        EOS 原理：
        1. 消息处理（INSERT/UPDATE/DELETE）
        2. 若成功：commit() 原子提交 offset + 消息处理
        3. 若失败：rollback()，offset 不变，消息重新消费
        """
        self.conf = {
            'bootstrap.servers': f'{host}:{port}',
            'group.id': group_id,
            'enable.auto.commit': ENABLE_AUTO_COMMIT,  # 禁用自动提交 → 手动提交 = 原子性
            'auto.offset.reset': 'earliest',  # 从最早的未消费消息开始
            'isolation.level': EOS_ISOLATION_LEVEL,  # read_committed = 仅读已提交的消息
            'max.poll.interval.ms': 300000,  # 处理消息超时（5分钟）
            'session.timeout.ms': 30000,  # 会话超时（30秒）
        }
        
        # 调用父类初始化
        super().__init__(self.conf)
        
        # 运行状态
        self.keep_running = True
        self.group_id = group_id
        
        # 统计信息
        self.total_processed = 0
        self.total_inserted = 0
        self.total_updated = 0
        self.total_deleted = 0
        self.total_dlq = 0
        self.total_errors = 0
        
        print("=" * 60)
        print("  CDC Consumer Initialized")
        print("=" * 60)
        print(f"Kafka: {host}:{port}")
        print(f"Group ID: {group_id}")
        print(f"Target DB: {DB_TARGET_CONFIG['host']}:{DB_TARGET_CONFIG['port']}")
        print("=" * 60)
    
    def consume_messages(self, topics):
        """
        消费Kafka消息的主循环
        
        参数:
            topics: 要订阅的主题列表（通常是[employee_topic_name]）
        
        工作流程：
        1. 订阅主题
        2. 无限循环：
           a. poll()从Kafka拉取消息
           b. 检查消息是否有效
           c. 反序列化JSON
           d. 验证数据
           e. 更新数据库或发送到DLQ
        3. Ctrl+C退出
        """
        try:
            # 订阅主题
            self.subscribe(topics)
            print(f"\n🎧 Subscribed to topics: {topics}")
            print("📥 Waiting for messages... (Press Ctrl+C to stop)\n")
            
            while self.keep_running:
                # 从Kafka拉取消息（超时1秒）
                # poll()会阻塞最多1秒，如果没有消息就返回None
                msg = self.poll(timeout=1.0)
                
                if msg is None:
                    # 没有新消息，继续等待
                    continue
                
                if msg.error():
                    # 消息有错误
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 已读到分区末尾（正常情况）
                        print(f"📍 Reached end of partition {msg.partition()}")
                    else:
                        # 其他错误
                        print(f"❌ Kafka error: {msg.error()}")
                        self.total_errors += 1
                    continue
                
                # 处理消息
                try:
                    # 反序列化JSON
                    json_str = msg.value().decode('utf-8')
                    employee = Employee.from_json(json_str)
                    
                    self.total_processed += 1
                    
                    # 验证数据
                    is_valid, error_msg = employee.validate()
                    
                    if is_valid:
                        # 数据有效，更新目标数据库
                        self.update_dst(employee)
                    else:
                        # 数据无效，发送到DLQ
                        print(f"⚠️  Validation failed for emp_id={employee.emp_id}: {error_msg}")
                        self.send_to_dlq(employee, error_msg, json_str)
                    
                    # ============ EOS：处理完成后手动提交 offset ============
                    # 这样做的优点：
                    # 1. 消息处理 + offset 提交是原子的（一起成功或都失败）
                    # 2. 如果处理中宕机，offset 不会提交，重启后会重新消费这条消息
                    # 3. 实现"恰好一次"语义，避免消息丢失或重复
                    try:
                        self.commit(asynchronous=False)  # 同步提交（确保成功）
                    except KafkaException as e:
                        print(f"❌ Commit failed: {e}")
                        self.total_errors += 1
                    
                    # 每100条打印一次统计
                    if self.total_processed % 100 == 0:
                        self.print_stats()
                
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                    self.total_errors += 1
                except Exception as e:
                    print(f"❌ Error processing message: {e}")
                    self.total_errors += 1
        
        except KeyboardInterrupt:
            print("\n\n🛑 Received Ctrl+C, shutting down gracefully...")
            self.keep_running = False
        
        finally:
            # 关闭Consumer，提交最后的offset
            print("\n📊 Final Statistics:")
            self.print_stats()
            print("\n👋 Closing consumer...")
            self.close()
            print("Consumer stopped. Goodbye!")
    
    def update_dst(self, employee: Employee):
        """
        根据action类型更新目标数据库
        
        参数:
            employee: Employee对象
        
        操作类型：
        - INSERT: 插入新记录（使用ON CONFLICT处理重复）
        - UPDATE: 更新现有记录
        - DELETE: 删除记录
        
        为什么用ON CONFLICT？
        - 实现幂等性：同一消息处理多次结果相同
        - 避免主键冲突错误
        """
        try:
            conn = psycopg2.connect(**DB_TARGET_CONFIG)
            cur = conn.cursor()
            
            if employee.action == 'INSERT' or employee.action == 'UPDATE':
                # INSERT和UPDATE都用UPSERT语句（ON CONFLICT）
                # 为什么？因为消息可能重复消费，需要幂等性
                sql = """
                    INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (emp_id) 
                    DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        dob = EXCLUDED.dob,
                        city = EXCLUDED.city,
                        salary = EXCLUDED.salary
                """
                cur.execute(sql, (
                    employee.emp_id,
                    employee.first_name,
                    employee.last_name,
                    employee.dob,
                    employee.city,
                    employee.salary
                ))
                
                if employee.action == 'INSERT':
                    self.total_inserted += 1
                    print(f"✅ INSERT: {employee}")
                else:
                    self.total_updated += 1
                    print(f"🔄 UPDATE: {employee}")
            
            elif employee.action == 'DELETE':
                # 删除记录
                sql = "DELETE FROM employees WHERE emp_id = %s"
                cur.execute(sql, (employee.emp_id,))
                self.total_deleted += 1
                print(f"🗑️  DELETE: emp_id={employee.emp_id}")
            
            else:
                print(f"⚠️  Unknown action: {employee.action}")
            
            conn.commit()
            cur.close()
            conn.close()
        
        except psycopg2.Error as e:
            print(f"❌ Database error updating emp_id={employee.emp_id}: {e}")
            self.total_errors += 1
        except Exception as e:
            print(f"❌ Error updating target database: {e}")
            self.total_errors += 1
    
    def send_to_dlq(self, employee: Employee, error_reason: str, original_json: str):
        """
        将验证失败的消息写入DLQ（Dead Letter Queue）表
        
        参数:
            employee: Employee对象
            error_reason: 验证失败的原因
            original_json: 原始JSON消息（用于调试）
        
        DLQ的作用：
        - 记录所有"问题数据"
        - 方便后续人工审查和修复
        - 不影响正常数据的处理
        """
        try:
            conn = psycopg2.connect(**DB_TARGET_CONFIG)
            cur = conn.cursor()
            
            sql = """
                INSERT INTO emp_cdc_dlq 
                (emp_id, first_name, last_name, dob, city, salary, action, error_reason, original_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cur.execute(sql, (
                employee.emp_id,
                employee.first_name,
                employee.last_name,
                employee.dob,
                employee.city,
                employee.salary,
                employee.action,
                error_reason,
                original_json
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.total_dlq += 1
            print(f"📨 Sent to DLQ: emp_id={employee.emp_id}, reason={error_reason}")
        
        except Exception as e:
            print(f"❌ Error writing to DLQ: {e}")
            self.total_errors += 1
    
    def print_stats(self):
        """打印统计信息"""
        print(f"\n{'='*60}")
        print(f"  Consumer Statistics")
        print(f"{'='*60}")
        print(f"  Total processed: {self.total_processed}")
        print(f"  ├─ Inserted: {self.total_inserted}")
        print(f"  ├─ Updated: {self.total_updated}")
        print(f"  ├─ Deleted: {self.total_deleted}")
        print(f"  ├─ Sent to DLQ: {self.total_dlq}")
        print(f"  └─ Errors: {self.total_errors}")
        print(f"{'='*60}\n")


if __name__ == '__main__':
    """
    主程序入口
    
    使用方法：
        python consumer.py
    
    要求：
    1. Docker容器已启动（Kafka和目标数据库）
    2. 目标数据库已初始化（有employees表和emp_cdc_dlq表）
    3. Producer正在运行（发送消息到Kafka）
    """
    
    print("""
    ╔════════════════════════════════════════════════╗
    ║      Kafka CDC Consumer - Project 2           ║
    ║                                                ║
    ║  Consumes CDC messages from Kafka and          ║
    ║  synchronizes to target database               ║
    ╚════════════════════════════════════════════════╝
    """)
    
    # 创建Consumer实例
    consumer = cdcConsumer(group_id=CONSUMER_GROUP_ID)
    
    # 开始消费消息
    consumer.consume_messages([employee_topic_name])