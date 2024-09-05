### Problem Statement: Employee Salary Data Transformation and Analysis

#A company has collected a CSV file containing employee data, including names, ages, genders, and salaries.The company’s management is interested in conducting a detailed analysis of their workforce, focusing on the salary structure.They need to implement an ETL(Extract, Transform, Load) pipeline to transform the raw employee data into a more usable format for business decision - making.

#** Objective **:
#The goal is to build an ETL pipeline using PySpark to transform the raw employee data by applying filtering, creating new salary - related metrics, and calculating salary statistics by gender.After the transformations, the processed data should be saved in an efficient file format(Parquet) for further analysis and reporting.

### **Task Requirements**:
#1. ** Extract **:
#- Load the employee data from a CSV file containing the following columns: `name`, `age`, `gender`, and `salary`.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import round
spark = SparkSession.builder.appName("EmployeeSalaryAnalysis").getOrCreate()
df=spark.read.csv("/content/sample_people.csv",header=True,inferSchema=True)

import pandas as pd

# Create a sample CSV data
data = {
    "name": ["John", "Jane", "Mike", "Emily", "Alex"],
    "age": [28, 32, 45, 23, 36],
    "gender": ["Male", "Female", "Male", "Female", "Male"],
    "salary": [60000, 72000, 84000, 52000, 67000]
}

df = pd.DataFrame(data)

# Save the DataFrame as a CSV file
csv_file_path = "/content/sample_people.csv"
df.to_csv(csv_file_path, index=False)

# Confirm the CSV file is created
print(f"CSV file created at: {csv_file_path}")

# 2. **Transform**:
#    - **Filter**: Only include employees aged 30 and above in the analysis.
#    - **Add New Column**: Calculate a 10% bonus on the current salary for each employee and add it as a new column (`salary_with_bonus`).
#    - **Aggregation**: Group the employees by gender and compute the average salary for each gender.

filtered_df = df.filter(col("age") >= 30)
filtered_df.show()

bonus_df = filtered_df.withColumn("salary_with_bonus", round(col("salary") * 1.1,2))
bonus_df.show()

avg_salary_by_gender = bonus_df.groupBy("gender").(avg("salary").withColumnRenamed("avg(Salary)","avg_salary")
avg_salary_by_gender.show()

# 3. **Load**:
#    - Save the transformed data (including the bonus salary) in a Parquet file format for efficient storage and retrieval.
#    - Ensure the data can be easily accessed for future analysis or reporting.

bonus_df.write.parquet("/content/convertedData.parquet")

bonus_df.show(truncate=False)

avg_salary_by_gender.show(truncate=False)