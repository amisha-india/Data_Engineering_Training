from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the JSON file
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True)
    ]), True)
])
# Load the complex JSON file with the correct path
json_file_path = "/content/sample_data/sample.json"

#Read the JSON file with schema
df_json_complex = spark.read.schema(schema).json(json_file_path)

#Read the file as text to inspect its contents
with open(json_file_path, 'r') as f:
    data = f.read()
    print(data)