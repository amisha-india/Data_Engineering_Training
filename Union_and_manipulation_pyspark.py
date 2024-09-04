from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark=SparkSession.builder\
.appName("Advanced Dataframe operations")\
.getOrCreate()

data1=[
  (1, 'Arjun','IT',75000,'2022-01-15'),
  (2, 'Vijay','Finance',85000,'2022-03-12'),
  (3, 'Shalini','IT',90000,'2021-06-30')
]

data2=[
    (4, "Sneha",'HR',50000,'2022-05-01'),
    (5, "Rahul","Finance",60000,'2022-08-20'),
    (6,"Amit","IT",55000,'2021-12-15')
]

columns=['EmployeeID','EmployeeName','Department','Salary','JoiningDate']

employee_df1=spark.createDataFrame(data=data1,schema=columns)
employee_df2=spark.createDataFrame(data=data2,schema=columns)

employee_df1.show()
employee_df2.show()

# Union two dataframes (removes duplicates)
union_df=employee_df1.union(employee_df2).dropDuplicates()
print("Union of two dataframes (Remove duplicates): ")
union_df.show()

# Union of two dataframes (Includes dataframes) union everyhting
unionAll_df=employee_df1.union(employee_df2)
print("Union of two dataframes: ")
unionAll_df.show()

# rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,col
window_spec=Window.partitionBy('Department').orderBy(col('Salary').desc())
ranked_df=unionAll_df.withColumn("Rank",rank().over(window_spec))
ranked_df.show()