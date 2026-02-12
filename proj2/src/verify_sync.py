"""
数据一致性验证脚本
功能：比较源数据库和目标数据库的数据，找出差异
"""

import psycopg2
from datetime import datetime

# 导入配置
try:
    from config import DB_SOURCE_CONFIG, DB_TARGET_CONFIG
except ImportError:
    DB_SOURCE_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres',
        'database': 'postgres'
    }
    DB_TARGET_CONFIG = {
        'host': 'localhost',
        'port': 5433,
        'user': 'postgres',
        'password': 'postgres',
        'database': 'postgres'
    }


def get_all_employees(db_config):
    """
    获取数据库中所有员工数据
    
    返回:
        dict: {emp_id: (first_name, last_name, dob, city, salary)}
    """
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT emp_id, first_name, last_name, dob, city, salary 
            FROM employees 
            ORDER BY emp_id
        """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # 转换为字典，方便比较
        employees = {}
        for row in rows:
            emp_id = row[0]
            employees[emp_id] = {
                'first_name': row[1],
                'last_name': row[2],
                'dob': str(row[3]),  # 转换date为字符串
                'city': row[4],
                'salary': row[5]
            }
        
        return employees
    
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None


def compare_employees(source_emp, target_emp):
    """
    比较两个员工记录是否相同
    
    返回:
        (bool, list): (是否相同, 差异列表)
    """
    differences = []
    
    for key in source_emp:
        if source_emp[key] != target_emp[key]:
            differences.append(f"{key}: {source_emp[key]} != {target_emp[key]}")
    
    return len(differences) == 0, differences


def verify_data_sync():
    """
    验证源库和目标库的数据一致性
    """
    print("""
    ╔════════════════════════════════════════════════╗
    ║      Data Synchronization Verification        ║
    ║                                                ║
    ║  Comparing source and target databases        ║
    ╚════════════════════════════════════════════════╝
    """)
    
    # 1. 获取源库数据
    print("📊 Fetching source database data...")
    source_employees = get_all_employees(DB_SOURCE_CONFIG)
    
    if source_employees is None:
        print("❌ Failed to connect to source database")
        return
    
    print(f"✅ Source database: {len(source_employees)} employees")
    
    # 2. 获取目标库数据
    print("📊 Fetching target database data...")
    target_employees = get_all_employees(DB_TARGET_CONFIG)
    
    if target_employees is None:
        print("❌ Failed to connect to target database")
        return
    
    print(f"✅ Target database: {len(target_employees)} employees")
    
    # 3. 比较数据
    print("\n" + "="*60)
    print("  Comparison Results")
    print("="*60)
    
    # 找出只在源库存在的员工
    only_in_source = set(source_employees.keys()) - set(target_employees.keys())
    
    # 找出只在目标库存在的员工
    only_in_target = set(target_employees.keys()) - set(source_employees.keys())
    
    # 找出两边都有但数据不同的员工
    different_data = []
    
    common_ids = set(source_employees.keys()) & set(target_employees.keys())
    for emp_id in common_ids:
        is_same, diffs = compare_employees(source_employees[emp_id], target_employees[emp_id])
        if not is_same:
            different_data.append((emp_id, diffs))
    
    # 4. 输出结果
    if len(only_in_source) == 0 and len(only_in_target) == 0 and len(different_data) == 0:
        print("\n🎉 Perfect synchronization!")
        print("✅ All employees in source database are synced to target database")
        print("✅ All data matches exactly")
        print("\n" + "="*60)
        print(f"  Total employees verified: {len(source_employees)}")
        print("="*60)
        return True
    else:
        print("\n⚠️  Synchronization issues detected!")
        
        if only_in_source:
            print(f"\n❌ {len(only_in_source)} employees ONLY in source database:")
            for emp_id in sorted(only_in_source):
                emp = source_employees[emp_id]
                print(f"   emp_id={emp_id}: {emp['first_name']} {emp['last_name']}")
            print("   💡 These employees have not been synced to target yet")
        
        if only_in_target:
            print(f"\n❌ {len(only_in_target)} employees ONLY in target database:")
            for emp_id in sorted(only_in_target):
                emp = target_employees[emp_id]
                print(f"   emp_id={emp_id}: {emp['first_name']} {emp['last_name']}")
            print("   💡 These employees exist in target but not in source (unusual)")
        
        if different_data:
            print(f"\n❌ {len(different_data)} employees have DIFFERENT data:")
            for emp_id, diffs in different_data:
                print(f"   emp_id={emp_id}:")
                for diff in diffs:
                    print(f"      {diff}")
            print("   💡 Data has not synced correctly")
        
        print("\n" + "="*60)
        print("  Troubleshooting:")
        print("="*60)
        print("  1. Check if Producer is running")
        print("  2. Check if Consumer is running")
        print("  3. Check CDC table: SELECT * FROM emp_cdc ORDER BY action_id DESC LIMIT 10;")
        print("  4. Check Kafka messages")
        print("="*60)
        
        return False


def show_cdc_status():
    """
    显示CDC表的状态
    """
    print("\n" + "="*60)
    print("  CDC Table Status")
    print("="*60)
    
    try:
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        
        # CDC表记录数
        cur.execute("SELECT COUNT(*) FROM emp_cdc")
        cdc_count = cur.fetchone()[0]
        
        # 最新的几条CDC记录
        cur.execute("""
            SELECT action_id, emp_id, first_name, last_name, action, action_time 
            FROM emp_cdc 
            ORDER BY action_id DESC 
            LIMIT 5
        """)
        recent_changes = cur.fetchall()
        
        cur.close()
        conn.close()
        
        print(f"Total CDC records: {cdc_count}")
        
        if recent_changes:
            print(f"\nRecent changes (last 5):")
            print(f"{'ID':<6} {'Emp ID':<8} {'Name':<25} {'Action':<8} {'Time'}")
            print("-" * 70)
            for row in recent_changes:
                action_id, emp_id, first_name, last_name, action, action_time = row
                name = f"{first_name} {last_name}"
                print(f"{action_id:<6} {emp_id:<8} {name:<25} {action:<8} {action_time}")
        
    except Exception as e:
        print(f"❌ Error: {e}")


def show_dlq_status():
    """
    显示DLQ表的状态
    """
    print("\n" + "="*60)
    print("  DLQ (Dead Letter Queue) Status")
    print("="*60)
    
    try:
        conn = psycopg2.connect(**DB_TARGET_CONFIG)
        cur = conn.cursor()
        
        # DLQ记录数
        cur.execute("SELECT COUNT(*) FROM emp_cdc_dlq")
        dlq_count = cur.fetchone()[0]
        
        print(f"Total DLQ records: {dlq_count}")
        
        if dlq_count > 0:
            # 显示DLQ中的记录
            cur.execute("""
                SELECT id, emp_id, first_name, last_name, error_reason, failed_at 
                FROM emp_cdc_dlq 
                ORDER BY id DESC 
                LIMIT 5
            """)
            dlq_records = cur.fetchall()
            
            print(f"\nRecent DLQ records (last 5):")
            print(f"{'ID':<5} {'Emp ID':<8} {'Name':<25} {'Error Reason':<40} {'Time'}")
            print("-" * 100)
            for row in dlq_records:
                id, emp_id, first_name, last_name, error_reason, failed_at = row
                name = f"{first_name} {last_name}"
                print(f"{id:<5} {emp_id:<8} {name:<25} {error_reason:<40} {failed_at}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    # 验证数据同步
    result = verify_data_sync()
    
    # 显示CDC状态
    show_cdc_status()
    
    # 显示DLQ状态
    show_dlq_status()
    
    print("\n")
