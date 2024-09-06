from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Movie Data Transformations") \
    .getOrCreate()

# 1. Load the Dataset
# Read the CSV file into a Pyspark DataFrame
df = spark.read.csv("/content/movies_data.csv", header=True, inferSchema=True)

# 2. Filter Movies by Genre
# Find all the movies in sci-fi genre
sci_fi_movies = df.filter(col("genre") == "Sci-Fi")
print("Movies in Sci-fi: ")
sci_fi_movies.show()

# 3. Top-Rated Movies
# Find the top 3 highest-rated movies.
top_rated_movies = df.orderBy(col("rating").desc()).limit(3)
print("Top 3 highest rated movies")
top_rated_movies.show()

# 4. Movies Released After 2010
# Find all movies released after 2010
movies_after_2010 = df.filter(col("date") > "2010-12-31")
print("Movies releases after 2010: ")
movies_after_2010.show()

# 5. Calculate Average Box Office Collection by Genre
# Group the movies by genre and calculate the average box office collection for each genre.
avg_box_office_by_genre = df.groupBy("genre").agg(avg("box_office").alias("avg_box_office"))
print("average box office collection for each genre: ")
avg_box_office_by_genre.show()

# 6. Add a New Column for Box Office in Billions
# Add a new column that shows the box office collection in billions
df_with_billion = df.withColumn("box_office_in_billions", col("box_office") / 1000000000)
print("box office collection in billions: ")
df_with_billion.show()

# 7. Sort Movies by Box Office Collection
# Sort the movies in descending order based on their box office collection.
sorted_movies = df.orderBy(col("box_office").desc())
print("movies order based on collections: ")
sorted_movies.show()

# 8. Count the Number of Movies per Genre
# Count the number of movies in each genre
count_movies_per_genre = df.groupBy("genre").count()
print("No.of movies in each genre: ")
count_movies_per_genre.show()