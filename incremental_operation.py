# Full refresh: Load the entire dataset
df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/content/sales_data.csv")

# Apply transformation (if necessary)
df_transformed = df_sales.withColumn("total_sales", df_sales["quantity"] * df_sales["price"])

# Full refresh: Partition the data by 'date' and overwrite the existing data
output_path = "/content/partitioned_data"
df_transformed.write.partitionBy("date").mode("overwrite").parquet(output_path)

# Verify partitioned data
partitioned_df = spark.read.parquet(output_path)
partitioned_df.show()
from pyspark.sql import functions as F

# Incremental load: Define the last ETL run timestamp (this should be tracked externally)
last_etl_run = "2024-01-01 00:00:00"

# Load only new or updated records since the last ETL run
df_incremental = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/content/sales_data.csv") \
    .filter(F.col("updated_at") > last_etl_run)

# Apply transformations (if necessary)
df_transformed_incremental = df_incremental.withColumn("total_sales", df_incremental["quantity"] * df_incremental["price"])

# Incremental load: Append the new data to the existing partitioned dataset
output_path = "/content/partitioned_sales_data"
df_transformed_incremental.write.partitionBy("date").mode("append").parquet(output_path)

# Verify partitioned data after incremental load
partitioned_df = spark.read.parquet(output_path)
partitioned_df.show()