import pandas as pd
from pyspark.sql import SparkSession

# Create a sample DataFrame
data = {
    "name": ["John", "Jane", "Mike", "Emily"],
    "age": [28, 32, 45, 23],
    "gender": ["Male", "Female", "Male", "Female"],
    "city": ["New York", "San Francisco", "Los Angeles", "Chicago"]
}
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file in the Colab environment
csv_file_path = "/content/sample_people.csv"
df.to_csv(csv_file_path, index=False)

# Confirm the file has been created
print(f"CSV file created at: {csv_file_path}")

# Initialize Spark Session
spark = SparkSession.builder.appName("CreateViewExample").getOrCreate()

# Load the CSV file into a PySpark DataFrame
df_people = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_file_path)

# Show the DataFrame
df_people.show()

# Create a temporary view
df_people.createOrReplaceTempView("people_temp_view")

# Run an SQL query on the view
result_temp_view = spark.sql("SELECT name, age, gender, city FROM people_temp_view WHERE age > 30")

# Show the result
result_temp_view.show()

# Create a global temporary view
df_people.createOrReplaceGlobalTempView("people_global_view")

# Query the global temporary view
result_global_view = spark.sql("SELECT name, age, city FROM global_temp.people_global_view WHERE age < 30")

# Show the result
result_global_view.show()

# List all temporary views and tables
spark.catalog.listTables()

# Drop the local temporary view
spark.catalog.dropTempView("people_temp_view")

# Drop the global temporary view
spark.catalog.dropGlobalTempView("people_global_view")