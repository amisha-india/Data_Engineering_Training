!pip install ipywidgets
from pyspark.sql import SparkSession
import ipywidgets as widgets
from IPython.display import display

# Step 1: Initialize a Spark session
spark = SparkSession.builder.appName("PySpark with Widgets Example").getOrCreate()

# Step 2: Create a simple DataFrame
data = [
    ("John", 28, "Male", 60000),
    ("Jane", 32, "Female", 72000),
    ("Mike", 45, "Male", 84000),
    ("Emily", 23, "Female", 52000),
    ("Alex", 36, "Male", 67000)
]

df = spark.createDataFrame(data, ["name", "age", "gender", "salary"])

# Show the DataFrame
df.show()
