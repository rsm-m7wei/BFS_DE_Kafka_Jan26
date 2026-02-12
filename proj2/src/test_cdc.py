"""
CDC系统测试脚本
功能：在源数据库执行CRUD操作，测试CDC数据流
"""

import psycopg2
import time
from datetime import datetime, timedelta

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


def wait_for_sync(seconds=2):
    """
    等待CDC同步完成
    为什么需要？Producer和Consumer需要时间处理
    """
    print(f"⏳ Waiting {seconds}s for CDC synchronization...")
    time.sleep(seconds)


def wait_for_employee(db_config, emp_id, exists=True, timeout=12, interval=1):
    """
    轮询等待员工记录出现或消失
    """
    attempts = int(timeout / interval)
    last_row = None
    for _ in range(attempts):
        last_row = get_employee_by_id(db_config, emp_id)
        if exists and last_row is not None:
            return last_row
        if not exists and last_row is None:
            return None
        time.sleep(interval)
    return last_row


def wait_for_employee_fields(db_config, emp_id, expected_city, expected_salary, timeout=12, interval=1):
    """
    轮询等待指定字段更新到目标值
    """
    attempts = int(timeout / interval)
    last_row = None
    for _ in range(attempts):
        last_row = get_employee_by_id(db_config, emp_id)
        if last_row and last_row[4] == expected_city and last_row[5] == expected_salary:
            return last_row
        time.sleep(interval)
    return last_row


def get_employee_count(db_config, table='employees'):
    """获取员工数量"""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"❌ Error: {e}")
        return -1


def get_employee_by_id(db_config, emp_id):
    """根据emp_id查询员工"""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute("SELECT * FROM employees WHERE emp_id = %s", (emp_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def test_insert():
    """
    测试1：INSERT操作
    在源库插入新员工，验证是否同步到目标库
    """
    print("\n" + "="*60)
    print("  TEST 1: INSERT Operation")
    print("="*60)
    
    emp_id = 1001
    first_name = "Alice"
    last_name = "Test"
    dob = "2015-06-15"
    city = "TestCity"
    salary = 75000
    
    try:
        # 清理旧数据，避免主键冲突
        conn = psycopg2.connect(**DB_TARGET_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM employees WHERE emp_id = %s", (emp_id,))
        conn.commit()
        cur.close()
        conn.close()

        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM employees WHERE emp_id = %s", (emp_id,))
        conn.commit()
        cur.close()
        conn.close()

        # 在源库插入
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        
        sql = """
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (emp_id, first_name, last_name, dob, city, salary))
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Inserted into source database: emp_id={emp_id}, name={first_name} {last_name}")
        
        # 等待同步
        wait_for_sync(2)
        
        # 验证目标库（轮询等待）
        target_emp = wait_for_employee(DB_TARGET_CONFIG, emp_id, exists=True)
        
        if target_emp:
            print(f"✅ Found in target database: {target_emp}")
            print("🎉 INSERT test PASSED!")
            return True
        else:
            print(f"❌ NOT found in target database!")
            print("💡 Check if Producer and Consumer are running")
            return False
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def test_update():
    """
    测试2：UPDATE操作
    在源库更新员工数据，验证是否同步到目标库
    """
    print("\n" + "="*60)
    print("  TEST 2: UPDATE Operation")
    print("="*60)
    
    emp_id = 1002
    new_salary = 85000
    new_city = "UpdatedCity"
    
    try:
        # 确保记录存在（先插入）
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (emp_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                dob = EXCLUDED.dob,
                city = EXCLUDED.city,
                salary = EXCLUDED.salary
            """,
            (emp_id, "Update", "User", "2015-01-01", "InitCity", 70000)
        )
        conn.commit()
        cur.close()
        conn.close()

        # 等待初始同步
        wait_for_employee(DB_TARGET_CONFIG, emp_id, exists=True)

        # 更新源库
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        sql = "UPDATE employees SET salary = %s, city = %s WHERE emp_id = %s"
        cur.execute(sql, (new_salary, new_city, emp_id))
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Updated in source database: emp_id={emp_id}, new_salary={new_salary}, new_city={new_city}")
        
        # 等待同步
        wait_for_sync(2)
        
        # 验证目标库（轮询等待字段更新）
        target_emp = wait_for_employee_fields(DB_TARGET_CONFIG, emp_id, new_city, new_salary)
        
        if target_emp and target_emp[5] == new_salary and target_emp[4] == new_city:
            print(f"✅ Update synced to target: {target_emp}")
            print("🎉 UPDATE test PASSED!")
            return True
        else:
            print(f"❌ Update NOT synced correctly!")
            print(f"   Target data: {target_emp}")
            return False
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def test_delete():
    """
    测试3：DELETE操作
    在源库删除员工，验证是否同步到目标库
    """
    print("\n" + "="*60)
    print("  TEST 3: DELETE Operation")
    print("="*60)
    
    emp_id = 1003
    
    try:
        # 确保记录存在（先插入）
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (emp_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                dob = EXCLUDED.dob,
                city = EXCLUDED.city,
                salary = EXCLUDED.salary
            """,
            (emp_id, "Delete", "User", "2015-01-01", "InitCity", 70000)
        )
        conn.commit()
        cur.close()
        conn.close()

        # 等待初始同步
        wait_for_employee(DB_TARGET_CONFIG, emp_id, exists=True)

        # 删除源库记录
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        
        sql = "DELETE FROM employees WHERE emp_id = %s"
        cur.execute(sql, (emp_id,))
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Deleted from source database: emp_id={emp_id}")
        
        # 等待同步
        wait_for_sync(2)
        
        # 验证目标库（轮询等待）
        target_emp = wait_for_employee(DB_TARGET_CONFIG, emp_id, exists=False)
        
        if target_emp is None:
            print(f"✅ Record deleted from target database")
            print("🎉 DELETE test PASSED!")
            return True
        else:
            print(f"❌ Record still exists in target: {target_emp}")
            return False
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def test_dlq():
    """
    测试4：DLQ（死信队列）
    插入无效数据，验证是否进入DLQ表
    """
    print("\n" + "="*60)
    print("  TEST 4: DLQ (Dead Letter Queue)")
    print("="*60)
    
    # 测试数据：薪资低于10000（应该被DLQ捕获）
    emp_id = 2000
    first_name = "Invalid"
    last_name = "Data"
    dob = "2015-01-01"
    city = "TestCity"
    salary = 5000  # ❌ 低于最低要求10000
    
    try:
        # 清理目标库中旧的DLQ记录，避免误判
        conn = psycopg2.connect(**DB_TARGET_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM emp_cdc_dlq WHERE emp_id = %s", (emp_id,))
        conn.commit()
        cur.close()
        conn.close()

        # 确保源库中没有重复 emp_id
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM employees WHERE emp_id = %s", (emp_id,))
        conn.commit()
        cur.close()
        conn.close()

        # 在源库插入无效数据
        conn = psycopg2.connect(**DB_SOURCE_CONFIG)
        cur = conn.cursor()
        
        sql = """
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (emp_id, first_name, last_name, dob, city, salary))
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Inserted invalid data into source: emp_id={emp_id}, salary={salary}")
        
        # 等待同步
        wait_for_sync(2)
        
        # 验证目标库（应该不存在）
        target_emp = wait_for_employee(DB_TARGET_CONFIG, emp_id, exists=False)
        
        # 检查DLQ表（轮询等待）
        dlq_record = None
        for _ in range(12):
            conn = psycopg2.connect(**DB_TARGET_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT * FROM emp_cdc_dlq WHERE emp_id = %s", (emp_id,))
            dlq_record = cur.fetchone()
            cur.close()
            conn.close()
            if dlq_record is not None:
                break
            time.sleep(1)
        
        if target_emp is None and dlq_record is not None:
            print(f"✅ Invalid data NOT in target database")
            print(f"✅ Found in DLQ table: {dlq_record}")
            print("🎉 DLQ test PASSED!")
            
            # 清理：删除源库的无效数据
            conn = psycopg2.connect(**DB_SOURCE_CONFIG)
            cur = conn.cursor()
            cur.execute("DELETE FROM employees WHERE emp_id = %s", (emp_id,))
            conn.commit()
            cur.close()
            conn.close()
            
            return True
        else:
            print(f"❌ DLQ test failed!")
            print(f"   Target data: {target_emp}")
            print(f"   DLQ record: {dlq_record}")
            return False
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    print("""
    ╔════════════════════════════════════════════════╗
    ║      CDC System Test Suite                    ║
    ║                                                ║
    ║  Before running tests, make sure:             ║
    ║  1. Docker containers are running             ║
    ║  2. Producer is running (python producer.py)  ║
    ║  3. Consumer is running (python consumer.py)  ║
    ╚════════════════════════════════════════════════╝
    """)
    
    # 检查数据库连接
    print("🔍 Checking database connections...")
    source_count = get_employee_count(DB_SOURCE_CONFIG)
    target_count = get_employee_count(DB_TARGET_CONFIG)
    
    if source_count == -1 or target_count == -1:
        print("❌ Cannot connect to databases!")
        print("💡 Make sure Docker containers are running: docker compose ps")
        return
    
    print(f"✅ Source DB connected (employees: {source_count})")
    print(f"✅ Target DB connected (employees: {target_count})")
    
    # 运行测试
    results = []
    
    results.append(("INSERT", test_insert()))
    results.append(("UPDATE", test_update()))
    results.append(("DELETE", test_delete()))
    results.append(("DLQ", test_dlq()))
    
    # 汇总结果
    print("\n" + "="*60)
    print("  TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name:15} {status}")
    
    print("="*60)
    print(f"  Total: {passed}/{total} tests passed")
    print("="*60)
    
    if passed == total:
        print("\n🎉🎉🎉 All tests PASSED! CDC system is working correctly! 🎉🎉🎉")
    else:
        print("\n⚠️  Some tests failed. Please check the logs above.")
        print("💡 Common issues:")
        print("   - Producer or Consumer not running")
        print("   - Docker containers not started")
        print("   - Database initialization not complete")


if __name__ == "__main__":
    run_all_tests()
