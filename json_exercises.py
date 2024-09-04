# JSON Exercises
import csv
import json

''' Exercise 1: Reading a JSON File
1. Create a JSON file named `data.json` with the following content:
   ```json
   {
       "name": "John Doe",
       "age": 30,
       "city": "New York",
       "skills": ["Python", "Machine Learning", "Data Analysis"]
   }
   ```
2. Write a Python script to read and print the contents of the JSON file.'''

with open('D:/Desktop/data.json', 'r') as file:
    data = json.load(file)
    print(data)

''' Exercise 2: Writing to a JSON File
1. Create a Python dictionary representing a person's profile:
   ```python
   profile = {
       "name": "Jane Smith",
       "age": 28,
       "city": "Los Angeles",
       "hobbies": ["Photography", "Traveling", "Reading"]
   }
   ```
2. Write a Python script to save this data to a JSON file named `profile.json`.'''

profile = {
    "name": "Jane Smith",
    "age": 28,
    "city": "Los Angeles",
    "hobbies": ["Photography", "Traveling", "Reading"]
}

with open('D:/Desktop/profile.json', 'w') as file:
    json.dump(profile, file)

'''Exercise 3: Converting CSV to JSON
1. Using the `students.csv` file from the CSV exercises, write a Python script to read the file and convert the data to a list of dictionaries.
2. Save the list of dictionaries to a JSON file called `students.json`.'''

students = []

with open('D:/Desktop/students.csv', mode='r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        students.append(row)

with open('D:/Desktop/students.json', 'w') as file:
    json.dump(students, file)

''' Exercise 4: Converting JSON to CSV
1. Using the `data.json` file from Exercise 1, write a Python script to read the JSON data.
2. Convert the JSON data to a CSV format and write it to a file named `data.csv`.'''


with open('D:/Desktop/data.json', 'r') as file:
    data = json.load(file)

with open('D:/Desktop/data.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(data.keys())
    csv_writer.writerow(data.values())

'''Exercise 5: Nested JSON Parsing
1. Create a JSON file named `books.json` with the following content:
   ```json
   {
       "books": [
           {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "year": 1925},
           {"title": "War and Peace", "author": "Leo Tolstoy", "year": 1869},
           {"title": "The Catcher in the Rye", "author": "J.D. Salinger", "year": 1951}
       ]
   }
   ```
2. Write a Python script to read the JSON file and print the title of each book.'''

with open('D:/Desktop/books.json', 'r') as file:
    books_data = json.load(file)
    for book in books_data['books']:
        print(book['title'])
