# specify the file path and name
file_path = "c:/Users/amarj/Documents/example.txt"

# Open the file in write mode and write content
with open(file_path, "w") as file:
    file.write("Hello, this is content written to a file in your laptop!")

print("File created and content written successfully")

# Open the file in read mode and print each line
with open(file_path, "r") as file:
    for line in file:
        print(line.strip()) # Remove newline characters /n

# open a file in append mode and add new content
with open(file_path, "a") as file:
    file.write("\nThis is additional content appended to the file.")

# Read the entire file as a string and print it
with open(file_path,"r") as file:
    content = file.read()
    print(content)

# csv
import csv
#write data to a csv file
data = [["Name","Age"],["Alice,25"],["Bob",30]]
with open (file_path,"w", newline = "") as file:
    writer = csv.writer(file)
    writer.writerow(data)

# Reading data from a csv file
with open(file_path, "r") as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)

# JSON
import json
# writing data to a JSON file
data = { "name": "Alice", "age": 30, "city": "New york"}
with open(file_path, "w") as file:
    json.dump(data, file)

# Reading data from a JSON file
with open(file_path, "r") as file:
    loaded_data = json.load(file)
    print(loaded_data)
