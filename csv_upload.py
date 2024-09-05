from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("DataIngesttion") \
    .getOrCreate()

csv_file_path = "/content/people.csv"
df_csv = spark.read.format("csv").option("header", "true").load(csv_file_path)
df_csv.show()