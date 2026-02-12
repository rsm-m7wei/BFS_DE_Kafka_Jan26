"""
Employee数据模型
功能：表示员工数据，支持序列化、反序列化和验证
"""

import json
from datetime import datetime, date
from typing import Optional, Dict, Tuple

# 导入配置（用于验证规则）
try:
    from config import MIN_BIRTH_YEAR, MIN_SALARY, MIN_EMPLOYEE_ID
except ImportError:
    # 如果config.py不存在，使用默认值
    MIN_BIRTH_YEAR = 2007
    MIN_SALARY = 10000
    MIN_EMPLOYEE_ID = 0


class Employee:
    """
    员工数据类
    
    属性说明：
    - action_id: CDC记录ID（用于追踪处理进度）
    - emp_id: 员工ID
    - first_name: 名字（改进：之前是emp_FN，现在更清晰）
    - last_name: 姓氏
    - dob: 出生日期（Date of Birth）
    - city: 城市
    - salary: 薪资
    - action: 操作类型（INSERT/UPDATE/DELETE）
    """
    
    def __init__(self, 
                 action_id: int, 
                 emp_id: int, 
                 first_name: str, 
                 last_name: str, 
                 dob: str, 
                 city: str, 
                 salary: int, 
                 action: str):
        self.action_id = action_id
        self.emp_id = emp_id
        self.first_name = first_name
        self.last_name = last_name
        self.dob = dob  # 格式：'YYYY-MM-DD' 或 date对象
        self.city = city
        self.salary = salary
        self.action = action  # 'INSERT', 'UPDATE', 'DELETE'
    
    @staticmethod
    def from_line(line):
        """
        从数据库查询结果（元组）创建Employee对象
        
        参数:
            line: 数据库查询返回的一行数据（tuple）
                  格式：(action_id, emp_id, first_name, last_name, dob, city, salary, action)
        
        返回:
            Employee对象
        
        示例:
            row = (1, 42, 'John', 'Doe', '2010-01-01', 'NYC', 50000, 'INSERT')
            employee = Employee.from_line(row)
        """
        return Employee(
            action_id=line[0],
            emp_id=line[1],
            first_name=line[2],
            last_name=line[3],
            dob=str(line[4]),  # 转换date对象为字符串
            city=line[5],
            salary=line[6],
            action=line[7]
        )
    
    def to_json(self) -> str:
        """
        序列化为JSON字符串（发送到Kafka）
        
        返回:
            JSON字符串
        
        示例:
            {"action_id": 1, "emp_id": 42, "first_name": "John", ...}
        """
        # 转换date对象为字符串（如果需要）
        data = self.__dict__.copy()
        if isinstance(data['dob'], date):
            data['dob'] = data['dob'].strftime('%Y-%m-%d')
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Employee':
        """
        从JSON字符串反序列化为Employee对象（Consumer接收Kafka消息）
        
        参数:
            json_str: JSON字符串
        
        返回:
            Employee对象
        
        示例:
            json_str = '{"action_id": 1, "emp_id": 42, ...}'
            employee = Employee.from_json(json_str)
        """
        data = json.loads(json_str)
        return cls(
            action_id=data.get('action_id', 0),
            emp_id=data['emp_id'],
            first_name=data['first_name'],
            last_name=data['last_name'],
            dob=data['dob'],
            city=data['city'],
            salary=data['salary'],
            action=data['action']
        )
    
    def validate(self) -> Tuple[bool, Optional[str]]:
        """
        验证员工数据是否符合业务规则
        
        验证规则：
        1. 员工ID不能为负数
        2. 出生年份必须 > 2007
        3. 薪资必须 >= 10000
        
        返回:
            (是否有效, 错误信息)
            - (True, None): 验证通过
            - (False, "错误原因"): 验证失败
        
        示例:
            is_valid, error = employee.validate()
            if not is_valid:
                print(f"验证失败: {error}")
        """
        # 验证1：员工ID
        if self.emp_id < MIN_EMPLOYEE_ID:
            return False, f"Invalid emp_id: {self.emp_id} (must be >= {MIN_EMPLOYEE_ID})"
        
        # 验证2：出生年份
        try:
            # 解析日期字符串
            if isinstance(self.dob, str):
                birth_date = datetime.strptime(self.dob, '%Y-%m-%d').date()
            else:
                birth_date = self.dob
            
            birth_year = birth_date.year
            if birth_year <= MIN_BIRTH_YEAR:
                return False, f"Invalid birth year: {birth_year} (must be > {MIN_BIRTH_YEAR})"
        except ValueError as e:
            return False, f"Invalid date format: {self.dob} (error: {e})"
        
        # 验证3：薪资
        if self.salary < MIN_SALARY:
            return False, f"Invalid salary: {self.salary} (must be >= {MIN_SALARY})"
        
        # 所有验证通过
        return True, None
    
    def to_dict(self) -> Dict:
        """
        转换为字典（方便数据库插入）
        
        返回:
            字典格式的员工数据
        """
        return {
            'action_id': self.action_id,
            'emp_id': self.emp_id,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'dob': self.dob,
            'city': self.city,
            'salary': self.salary,
            'action': self.action
        }
    
    def __repr__(self) -> str:
        """
        字符串表示（调试打印用）
        
        示例:
            print(employee)
            # 输出：Employee(emp_id=42, name='John Doe', salary=50000, action='INSERT')
        """
        return (f"Employee(emp_id={self.emp_id}, "
                f"name='{self.first_name} {self.last_name}', "
                f"salary={self.salary}, action='{self.action}')")
    
    def __str__(self) -> str:
        """
        用户友好的字符串表示
        """
        return f"{self.first_name} {self.last_name} (ID: {self.emp_id})"


# 测试代码
if __name__ == "__main__":
    print("=" * 60)
    print("  Employee Class Testing")
    print("=" * 60)
    
    # 测试1：创建Employee对象
    print("\n1. 创建Employee对象:")
    emp1 = Employee(
        action_id=1,
        emp_id=42,
        first_name="John",
        last_name="Doe",
        dob="2010-01-01",
        city="NYC",
        salary=50000,
        action="INSERT"
    )
    print(f"   {emp1}")
    
    # 测试2：序列化为JSON
    print("\n2. 序列化为JSON:")
    json_str = emp1.to_json()
    print(f"   {json_str}")
    
    # 测试3：从JSON反序列化
    print("\n3. 从JSON反序列化:")
    emp2 = Employee.from_json(json_str)
    print(f"   {emp2}")
    
    # 测试4：数据验证（有效数据）
    print("\n4. 数据验证（有效数据）:")
    is_valid, error = emp1.validate()
    print(f"   Valid: {is_valid}, Error: {error}")
    
    # 测试5：数据验证（无效数据 - 薪资过低）
    print("\n5. 数据验证（无效数据）:")
    emp3 = Employee(1, 43, "Jane", "Smith", "2010-01-01", "LA", 5000, "INSERT")
    is_valid, error = emp3.validate()
    print(f"   Valid: {is_valid}, Error: {error}")
    
    # 测试6：数据验证（无效数据 - 出生年份早）
    print("\n6. 数据验证（出生年份早）:")
    emp4 = Employee(1, 44, "Bob", "Wilson", "2005-01-01", "SF", 60000, "INSERT")
    is_valid, error = emp4.validate()
    print(f"   Valid: {is_valid}, Error: {error}")
    
    print("\n" + "=" * 60)
    print("  Testing Complete!")
    print("=" * 60)
