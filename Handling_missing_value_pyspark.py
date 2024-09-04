from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Employee Data Handling") \
    .getOrCreate()

# Sample employee data with null values
data = [
    (1, 'Arjun', 'IT', 75000),
    (2, 'Vijay', 'Finance', 85000),
    (3, None, 'IT', 90000),
    (4, 'Sneha', 'HR', None),
    (5, 'Rahul', None, 60000),
    (6, 'Amit', 'IT', 55000)
]

# Define schema (columns)
columns = ['EmployeeID', 'EmployeeName', 'Department', 'Salary']

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

# Show the DataFrame
employee_df.show()

# Fill null values in 'EmployeeName' and 'Department' with 'Unknown'
filled_df = employee_df.fillna({'EmployeeName': 'Unknown', 'Department': 'Unknown'})
filled_df.show()

# Drop rows where 'Salary' is null
dropped_null_salary_df = employee_df.dropna(subset=['Salary'])
dropped_null_salary_df.show()

# Fill null values in 'Salary' with 50000
salary_filled_df = employee_df.fillna({'Salary': 50000})
salary_filled_df.show()

# Check for null values in the entire DataFrame
null_counts = employee_df.select([col(c).isNull().alias(c) for c in employee_df.columns]).show()

# Replace all null values in the DataFrame with 'N/A'
na_filled_df = employee_df.na.fill('N/A')
na_filled_df.show()