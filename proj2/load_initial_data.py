"""
CSV数据加载脚本
功能：从employees.csv读取数据并插入到源数据库的employees表
这会自动触发CDC触发器，在emp_cdc表中记录变更
"""

import csv
import psycopg2
from datetime import datetime
import time

# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'postgres'
}

def wait_for_database(max_retries=10, delay=3):
    """
    等待数据库就绪
    为什么需要这个？Docker容器启动需要时间
    """
    print("🔍 Waiting for database to be ready...")
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("✅ Database is ready!")
            return True
        except psycopg2.OperationalError:
            print(f"⏳ Attempt {attempt + 1}/{max_retries} - Database not ready, retrying in {delay}s...")
            time.sleep(delay)
    
    print("❌ Database not available after maximum retries")
    return False

def parse_date(date_str):
    """
    解析CSV中的日期格式
    例如："2/3/2002" -> datetime对象
    """
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        # 尝试其他日期格式
        return datetime.strptime(date_str, "%d/%m/%Y").date()

def load_employees_from_csv(csv_file='employees.csv'):
    """
    从CSV文件加载员工数据到源数据库
    """
    # 1. 等待数据库启动
    if not wait_for_database():
        return False
    
    # 2. 连接数据库
    print(f"\n📂 Reading from {csv_file}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 3. 读取CSV文件
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            
            inserted_count = 0
            for row in csv_reader:
                # 解析数据
                emp_id = int(row['Employee ID'])
                first_name = row['First Name'].strip()
                last_name = row['Last Name'].strip()
                dob = parse_date(row['Date of Birth'])
                city = row['City'].strip()
                salary = int(row['Salary'])
                
                # 插入数据
                cursor.execute("""
                    INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (emp_id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        dob = EXCLUDED.dob,
                        city = EXCLUDED.city,
                        salary = EXCLUDED.salary
                """, (emp_id, first_name, last_name, dob, city, salary))
                
                inserted_count += 1
                print(f"  ✓ Inserted: {first_name} {last_name} (ID: {emp_id})")
        
        # 4. 提交事务
        conn.commit()
        
        # 5. 验证数据
        cursor.execute("SELECT COUNT(*) FROM employees")
        count = cursor.fetchone()[0]
        print(f"\n✅ Successfully loaded {inserted_count} employees")
        print(f"📊 Total employees in database: {count}")
        
        # 6. 检查CDC表
        cursor.execute("SELECT COUNT(*) FROM emp_cdc")
        cdc_count = cursor.fetchone()[0]
        print(f"🔔 CDC records captured: {cdc_count}")
        
        if cdc_count > 0:
            print("\n📝 Sample CDC records:")
            cursor.execute("SELECT action_id, emp_id, first_name, last_name, action FROM emp_cdc LIMIT 5")
            for record in cursor.fetchall():
                print(f"  Action ID {record[0]}: {record[3]} {record[2]} (emp_id={record[1]}, action={record[4]})")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("  CSV Data Loader - Source Database")
    print("=" * 60)
    
    success = load_employees_from_csv()
    
    if success:
        print("\n" + "=" * 60)
        print("  ✅ Data loading completed successfully!")
        print("=" * 60)
        print("\n💡 Next steps:")
        print("  1. Start the Producer: python producer.py")
        print("  2. Start the Consumer: python consumer.py")
        print("  3. Producer will read from emp_cdc and send to Kafka")
    else:
        print("\n❌ Data loading failed")
        print("💡 Troubleshooting:")
        print("  1. Make sure Docker containers are running: docker compose ps")
        print("  2. Check database is accessible: docker compose logs db_source")
