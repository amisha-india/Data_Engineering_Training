pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .getOrCreate()

# Sample employee data
data = [
    (1, 'Arjun', 'IT', 75000),
    (2, 'Vijay', 'Finance', 85000),
    (3, 'Shalini', 'IT', 90000),
    (4, 'Sneha', 'HR', 50000),
    (5, 'Rahul', 'Finance', 60000),
    (6, 'Amit', 'IT', 55000)
]

# Define schema (columns)
columns = ['EmployeeID', 'EmployeeName', 'Department', 'Salary']

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

# Show the DataFrame
employee_df.show()


# Task-1: Filter employees by salary
filtered_employees = employee_df.filter(col('Salary') > 60000)
print("Employees having salary more than 60000: ")
filtered_employees.show()

# Task-2: Calculate the average salary by department
avg_salary_by_dept = employee_df.groupBy('Department').agg({'Salary': 'avg'})
print("Average salary by department: ")
avg_salary_by_dept.show()

# Task-3: Sort Employees by salary
sorted_employees = employee_df.orderBy(col('Salary').desc())
print("Employees sorted by salary in descending order: ")
sorted_employees.show()


# Add a bonus column
employee_df = employee_df.withColumn('Bonus', col('Salary') * 1.5)
employee_df.show()