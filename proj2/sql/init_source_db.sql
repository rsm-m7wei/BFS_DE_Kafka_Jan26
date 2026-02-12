-- ============================================
-- 源数据库初始化脚本
-- 功能：创建员工表、CDC变更捕获表和触发器
-- ============================================

-- 1. 创建员工表（业务主表）
CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,        -- 员工ID，自动递增
    first_name VARCHAR(100),          -- 名字
    last_name VARCHAR(100),           -- 姓氏
    dob DATE,                         -- 出生日期
    city VARCHAR(100),                -- 城市
    salary INT                        -- 薪资
);

-- 2. 创建CDC变更捕获表（记录所有变更）
CREATE TABLE IF NOT EXISTS emp_cdc (
    action_id SERIAL PRIMARY KEY,     -- 变更ID，自动递增（用于追踪已处理的变更）
    emp_id INT,                       -- 员工ID
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(10),               -- 操作类型：INSERT/UPDATE/DELETE
    action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 变更时间（自动记录）
);

-- 3. 创建触发器函数（定义触发时的行为）
CREATE OR REPLACE FUNCTION capture_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- 当删除员工时，记录旧数据
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
        RETURN OLD;
    
    -- 当更新员工时，记录新数据
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
        RETURN NEW;
    
    -- 当插入员工时，记录新数据
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 4. 在employees表上创建触发器（绑定触发器函数）
DROP TRIGGER IF EXISTS employee_cdc_trigger ON employees;
CREATE TRIGGER employee_cdc_trigger
    AFTER INSERT OR UPDATE OR DELETE ON employees
    FOR EACH ROW 
    EXECUTE FUNCTION capture_employee_changes();

-- 5. 创建索引以提升查询性能
CREATE INDEX IF NOT EXISTS idx_emp_cdc_action_id ON emp_cdc(action_id);

-- 完成提示
DO $$
BEGIN
    RAISE NOTICE '✅ Source database initialized successfully!';
    RAISE NOTICE '📊 Tables created: employees, emp_cdc';
    RAISE NOTICE '🔔 Trigger: employee_cdc_trigger is active';
END $$;
