### Data Setup:
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Advanced DataFrame Operations - Different Dataset") \
    .getOrCreate()

# Create two sample DataFrames for Product Sales
data1 = [
    (1, 'Product A', 'Electronics', 1200, '2022-05-10'),
    (2, 'Product B', 'Clothing', 500, '2022-07-15'),
    (3, 'Product C', 'Electronics', 1800, '2021-11-05')
]

data2 = [
    (4, 'Product D', 'Furniture', 3000, '2022-03-25'),
    (5, 'Product E', 'Clothing', 800, '2022-09-12'),
    (6, 'Product F', 'Electronics', 1500, '2021-10-19')
]

# Define schema (columns)
columns = ['ProductID', 'ProductName', 'Category', 'Price', 'SaleDate']

# Create DataFrames
sales_df1 = spark.createDataFrame(data1, columns)
sales_df2 = spark.createDataFrame(data2, columns)

### Tasks:

#1. **Union of DataFrames (removing duplicates)**:
#  Combine the two DataFrames (`sales_df1` and `sales_df2`) using `union` and remove any duplicate rows.

combined_df = sales_df1.union(sales_df2).distinct()
combined_df.show()

#2. **Union of DataFrames (including duplicates)**:
#  Combine both DataFrames using `unionAll` (replaced by `union`) and include duplicate rows.

combined_df_with_duplicates = sales_df1.union(sales_df2)
combined_df_with_duplicates.show()

#3. **Rank products by price within their category**:
#  Use window functions to rank the products in each category by price in descending order.

window_spec = Window.partitionBy("Category").orderBy(F.desc("Price"))
ranked_df = combined_df.withColumn("Rank", F.rank().over(window_spec))
ranked_df.show()

#4. **Calculate cumulative price per category**:
#  Use window functions to calculate the cumulative price of products within each category.

cumulative_price_df = combined_df.withColumn("CumulativePrice", F.sum("Price").over(window_spec))
cumulative_price_df.show()

#5. **Convert `SaleDate` from string to date type**:
#   Convert the `SaleDate` column from string format to a PySpark date type.

converted_df = combined_df.withColumn("SaleDate", F.to_date("SaleDate", "yyyy-MM-dd"))
converted_df.show()

#6. **Calculate the number of days since each sale**:
#  Calculate the number of days since each product was sold using the current date.

current_date = F.current_date()
days_since_sale_df = converted_df.withColumn("DaysSinceSale", F.datediff(current_date, "SaleDate"))
days_since_sale_df.show()

#7. **Add a column for the next sale deadline**:
#  Add a new column `NextSaleDeadline`, which should be 30 days after the `SaleDate`.

next_sale_deadline_df = converted_df.withColumn("NextSaleDeadline", F.date_add("SaleDate", 30))
next_sale_deadline_df.show()

#8. **Calculate total revenue and average price per category**:
#  Find the total revenue (sum of prices) and the average price per category.

revenue_avg_df = combined_df.groupBy("Category").agg(
    F.sum("Price").alias("TotalRevenue"),
    F.avg("Price").alias("AveragePrice")
)
revenue_avg_df.show()

#9. **Convert all product names to lowercase**:
#  Create a new column with all product names in lowercase.

lowercase_names_df = combined_df.withColumn("ProductNameLowercase", F.lower("ProductName"))
lowercase_names_df.show()
