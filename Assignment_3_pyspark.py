#1 Fitness tracker

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,max
# Initialize spark session
spark= SparkSession.builder.appName('FitnessTracker').getOrCreate()

data = spark.read.csv("/content/pyspark_exe.csv", header=True, inferSchema=True)

# Exercise
# 1.Calculate total steps for each user
total_steps = data.groupBy("user_id").agg(sum("steps").alias("total_steps"))
print("Total steps of each user")
total_steps.show()

# 2.Filter Days Where a User Burned More Than 500 Calories
burned_500_calories = data.filter(data["calories"] > 500)
print("Days where user burned more than 500 calories")
burned_500_calories.show()

# 3. Calculate the Average Distance Traveled by Each User
average_distance = data.groupBy("user_id").agg(avg("distance_km").alias("avg_distance"))
print("Average distance traveled by each user")
average_distance.show()

# 4. Identify the Day with the Maximum Steps for Each User
max_steps_per_user = data.groupBy("user_id", "date").agg(max("steps").alias("max_steps"))
print("Day with maximum steps: ")
max_steps_per_user.show()

# 5. Find Users Who Were Active for More Than 100 Minutes on Any Day
active_users = data.filter(data["active_minutes"] > 100)
print("Users who were active for more than 100 minutes on any day")
active_users.show()

# 6.  Calculate the Total Calories Burned per Day
total_calories_per_day = data.groupBy("date").agg(sum("calories").alias("total_calories"))
print("Total calories burned per day")
total_calories_per_day.show()

# 7. Calculate the Average Steps per Day
average_steps_per_day = data.groupBy("date").agg(avg("steps").alias("avg_steps"))
print("Average steps per day")
average_steps_per_day.show()

# 8.Rank Users by Total Distance Travelled
from pyspark.sql import functions as F
from pyspark.sql.window import Window
total_distance = data.groupBy("user_id").agg(F.sum("distance_km").alias("total_distance"))
window_spec = Window.orderBy(F.col("total_distance").desc())
ranked_users = total_distance.withColumn("rank", F.rank().over(window_spec))
print("Rank of the users based on distance travelled: ")
ranked_users.show()

# 9. Find the Most Active User by Total Active Minutes
most_active_user = data.groupBy("user_id").agg(F.sum("active_minutes").alias("total_active_minutes"))
most_active_user = most_active_user.orderBy(F.col("total_active_minutes").desc())
print("Most active user by total active minutes: ")
most_active_user.show(1)

# 10. Create a New Column for Calories Burned per Kilometer
data = data.withColumn("calories_per_km", data["calories"] / data["distance_km"])
print("New column for calories burned per kilometer")
data.show()

#2 Book Sales Data

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import max,sum,col
# Initialize spark session
spark=SparkSession.builder.appName("BookSales").getOrCreate()
# Load data
book_sales_df=spark.read.csv("/content/Book_sales_data.csv",header=True,inferSchema=True)

# Exercise
# 1. Find Total Sales Revenue per Genre
total_sales_revenue = book_sales_df.withColumn("total_sales", col("sale_price") * col("quantity")) \
    .groupBy("genre").agg(sum("total_sales").alias("total_revenue"))
print("total sales revenue per genre")
total_sales_revenue.show()

# 2. Filter Books Sold in the "Fiction" Genre
total_books_sold=book_sales_df.filter(book_sales_df.genre=="Fiction")
print("Books sold in fiction genre: ")
total_books_sold.show()

# 3. Find the Book with the Highest Sale Price
book_with_highest_price=book_sales_df.orderBy(col("sale_price").desc()).limit(1)
print("Book with highest price: ")
book_with_highest_price.show()

# 4.Calculate Total Quantity of Books Sold by Author
quantity_of_books_sold_by_author=book_sales_df.groupBy("author").agg(sum("quantity").alias("total_quantity"))
print("Quantity of books sold by author: ")
quantity_of_books_sold_by_author.show()

# 5. Identify Sales Transactions Worth More Than $50
sales_transaction=book_sales_df.withColumn("total_sales", col("sale_price") * col("quantity")) \
    .filter(col("total_sales") > 50)
print("Sales transaction worth more than $50: ")
sales_transaction.show()

# 6. Find the Average Sale Price per Genre
avg_sale_price_genre=book_sales_df.groupBy("genre").agg(sum("sale_price").alias("total_sales"), sum("quantity").alias("total_quantity")) \
    .withColumn("average_sale_price", col("total_sales") / col("total_quantity"))
print("Average sale price per genre:")
avg_sale_price_genre.show()

# 7. Count the Number of Unique Authors in the Dataset
unique_authors=book_sales_df.select("author").distinct().count()
print("Number of unique authors: ",unique_authors)

# 8. Find the Top 3 Best-Selling Books by Quantity
top_selling_books=book_sales_df.orderBy(col("quantity").desc()).limit(3)
print("top 3 selling books:")
top_selling_books.show()

# 9. Calculate Total Sales for Each Month
from pyspark.sql.functions import date_format
monthly_sales = book_sales_df.withColumn("month", date_format(col("date"), "yyyy-MM")) \
    .withColumn("total_sales", col("sale_price") * col("quantity")) \
    .groupBy("month").agg(sum("total_sales").alias("total_revenue"))
monthly_sales.show()

# 10. Create a New Column for Total Sales Amount
total_sales_amt=book_sales_df.withColumn("total_sales", col("sale_price") * col("quantity"))
total_sales_amt.show()

#3 Food delivery orders

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,sum,max

spark=SparkSession.builder.appName("FoodDelivery").getOrCreate()

food_delivert_df=spark.read.csv("/content/food_delivery_data.csv", header=True,inferSchema=True)

# Exercise
# 1. Calculate Total Revenue per Restaurant
total_revenue_per_restaurant = food_delivert_df.withColumn("total_revenue", col("price") * col("quantity")) \
    .groupBy("restaurant_name").agg(sum("total_revenue").alias("total_revenue"))
print("total revenue per restaurant: ")
total_revenue_per_restaurant.show()

# 2.  Find the Fastest Delivery
fastest_delivery=food_delivert_df.orderBy(col("delivery_time_mins").asc()).limit(1)
print("fastest delivery: ")
fastest_delivery.show()

# 3.Calculate Average Delivery Time per Restaurant
avg_delivery_time=food_delivert_df.groupBy("restaurant_name").agg(avg("delivery_time_mins").alias("avg_delivery_time"))
print("average delivery time per restaurant: ")
avg_delivery_time.show()

# 4. Filter Orders for a Specific Customer
order_for_specific_customers=food_delivert_df.filter(col("customer_id")==201)
print("order for specific customers: ")
order_for_specific_customers.show()

# 5. Find Orders Where Total Amount Spent is Greater Than $20
orders_on_amount_spent=food_delivert_df.withColumn("total_amount", col("price") * col("quantity")) \
    .filter(col("total_amount") > 20)
print("orders where total amount spent is greater than $20: ")
orders_on_amount_spent.show()

# 6. Calculate the Total Quantity of Each Food Item Sold
total_quantity_of_each_food_item=food_delivert_df.groupBy("food_item").agg(sum("quantity").alias("total_quantity"))
print("total quantity of each food item sold: ")
total_quantity_of_each_food_item.show()

# 7. Find the Top 3 Most Popular Restaurants by Number of Orders
top_3_restaurants=food_delivert_df.groupBy("restaurant_name").count().orderBy(col("count").desc()).limit(3)
print("top 3 most popular restaurants by number of orders: ")
top_3_restaurants.show()

# 8. Calculate Total Revenue per Day
total_revenue_per_day=food_delivert_df.withColumn("total_revenue", col("price") * col("quantity")) \
    .groupBy("order_d").agg(sum("total_revenue").alias("total_revenue"))
print("total revenue per day: ")
total_revenue_per_day.show()

# 9.  Find the Longest Delivery Time for Each Restaurant
longest_delivery_time=food_delivert_df.groupBy("restaurant_name").agg(max("delivery_time_mins"))
print("longest delivery time for each restaurant: ")
longest_delivery_time.show()

# 10.  Create a New Column for Total Order Value
total_order_value=food_delivert_df.withColumn("total_order_value", col("price") * col("quantity"))
print("new column for total order value: ")
total_order_value.show()

#4 Weather Data

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,sum,col,pow

spark=SparkSession.builder.appName("weather_data").getOrCreate()

weather_df=spark.read.csv("/content/weather_data.csv", header=True, inferSchema=True)

# Exercise
# 1. Find the Average Temperature for Each City
avg_temperature=weather_df.groupBy("city").agg(avg("temperature_c").alias("avg_temperature"))
print("Average temperature for each city: ")
avg_temperature.show()

# 2.Filter Days with Temperature Below Freezing
freezing_days=weather_df.filter(col("temperature_c")<0)
print("Days with temperature below freezing: ")
freezing_days.show()

# 3.Find the City with the Highest Wind Speed on a Specific Day
highest_wind_speed=weather_df.orderBy(col("wind_speed_kph").desc()).limit(1)
print("City with the highest wind speed on a specific day: ")
highest_wind_speed.show()

# 4. Calculate the Total Number of Days with Rainy Weather
rainy_days_count = weather_df.filter(weather_df["condition"] == "Rain").count()
print(f"Total number of rainy days: {rainy_days_count}")

# 5. Calculate the Average Humidity for Each Weather Condition
avg_humidity=weather_df.groupBy("condition").agg(avg("humidity").alias("avg_humidity"))
print("Average humidity for each weather condition: ")
avg_humidity.show()

# 6. Find the Hottest Day in Each City
hottest_day=weather_df.orderBy(col("temperature_c").desc()).limit(1)
print("Hottest day in each city: ")
hottest_day.show()

# 7. Identify Cities That Experienced Snow
snow_cities=weather_df.filter(weather_df["condition"]=="Snow").select("city").distinct()
print("Cities that experienced snow: ")
snow_cities.show()

# 8. Calculate the Average Wind Speed for Days When the Condition was Sunny
avg_wind_speed=weather_df.filter(weather_df["condition"]=="Sunny").agg(avg("wind_speed_kph").alias("avg_wind_speed"))
print("Average wind speed for days when the condition was sunny: ")
avg_wind_speed.show()

# 9. Find the Coldest Day Across All Cities
coldest_day=weather_df.orderBy(col("temperature_c").asc()).limit(1)
print("Coldest day across all cities: ")
coldest_day.show()

# 10. Create a New Column for Wind Chill
data_with_wind_chill = weather_df.withColumn("wind_chill",
    13.12 + 0.6215 * col("temperature_c") - 11.37 * pow(col("wind_speed_kph"), 0.16) +
    0.3965 * col("temperature_c") * pow(col("wind_speed_kph"), 0.16))
print("New dataset after adding column")
data_with_wind_chill.show()

#5 Airline Flight data

! pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,sum,max

spark=SparkSession.builder.appName('airline_flight_data').getOrCreate()

airline_df=spark.read.csv("/content/airline_flight_data.csv", header=True, inferSchema=True)

# Exercise
# 1.Find the Total Distance Traveled by Each Airline
total_distance_travelled = airline_df.groupBy("airline").agg(sum("distance").alias("total_distance"))
print("Total Distance Traveled by Each Airline: ")
total_distance_travelled.show()

# 2. Filter Flights with Delays Greater than 30 Minutes
delayed_flights = airline_df.filter(airline_df["delay_min"] > 30)
print("Flights with Delays Greater than 30 Minutes: ")
delayed_flights.show()

# 3. Find the Flight with the Longest Distance
longest_flight = airline_df.orderBy(airline_df["distance"].desc()).limit(1)
print("Flight with the Longest Distance: ")
longest_flight.show()

# 4. Calculate the Average Delay Time for Each Airline
average_delay = airline_df.groupBy("airline").agg(avg("delay_min").alias("average_delay"))
print("Average Delay Time for Each Airline: ")
average_delay.show()

# 5. Identify Flights That Were Not Delayed
not_delayed_flights = airline_df.filter(airline_df["delay_min"] == 0)
print("Flights That Were Not Delayed: ")
not_delayed_flights.show()

# 6. Find the Top 3 Most Frequent Routes
top_3_routes = airline_df.groupBy("origin", "destination").count().orderBy("count", ascending=False).limit(3)
print("Top 3 Most Frequent Routes: ")
top_3_routes.show()

# 7. Calculate the Total Number of Flights per Day
flights_per_day = airline_df.groupBy("start_date ").count()
print("Total Number of Flights per Day: ")
flights_per_day.show()

# 8. Find the Airline with the Most Flights
most_flights_airline = airline_df.groupBy("airline").count().orderBy("count", ascending=False).limit(1)
print("Airline with the Most Flights: ")
most_flights_airline.show()

# 9. Calculate the Average Flight Distance per Day
average_distance_per_day = airline_df.groupBy("start_date ").agg(avg("distance").alias("average_distance"))
print("Average Flight Distance per Day: ")
average_distance_per_day.show()

# 10. Create a New Column for On-Time Status
from pyspark.sql.functions import when
data_with_on_time = airline_df.withColumn("on_time", when(airline_df["delay_min"] == 0, True).otherwise(False))
print(" added colum on-time status: ")
data_with_on_time.show()
