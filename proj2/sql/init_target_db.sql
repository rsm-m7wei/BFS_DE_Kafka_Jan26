-- ============================================
-- 目标数据库初始化脚本
-- 功能：创建员工表和DLQ死信队列表
-- ============================================

-- 1. 创建员工表（与源库结构完全相同）
CREATE TABLE IF NOT EXISTS employees (
    emp_id INT PRIMARY KEY,           -- 注意：这里用INT而不是SERIAL
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

-- 2. 创建DLQ表（Dead Letter Queue - 死信队列）
-- 用途：存储验证失败的消息
CREATE TABLE IF NOT EXISTS emp_cdc_dlq (
    id SERIAL PRIMARY KEY,            -- DLQ记录ID
    emp_id INT,                       -- 原始员工ID
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(10),               -- 原始操作类型
    error_reason TEXT,                -- 失败原因（为什么验证没通过）
    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 失败时间
    original_message TEXT             -- 原始JSON消息（用于调试）
);

-- 3. 创建索引以提升查询性能
CREATE INDEX IF NOT EXISTS idx_employees_emp_id ON employees(emp_id);
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON emp_cdc_dlq(failed_at);

-- 完成提示
DO $$
BEGIN
    RAISE NOTICE '✅ Target database initialized successfully!';
    RAISE NOTICE '📊 Tables created: employees, emp_cdc_dlq';
    RAISE NOTICE '🚨 DLQ ready to capture validation failures';
END $$;
