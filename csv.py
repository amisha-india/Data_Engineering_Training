
import pandas as pd

# Load the csv file into a DataFrame
df = pd.read_csv('employee.csv')

# print the DataFrame
print(df)

# Display the first three rows
print(df.head(3))

# Show summary informaation about the Dataframe
print(df.info())

# Display summary statistics of numeric columns
print(df.describe())

# Filter rows where salary is greater than 80000
high_salary_df = df[df['Salary'] > 80000]
print(high_salary_df)

# Sort by Age in desending order
sorted_df = df.sort_values(by = 'Age', ascending=False)
print(sorted_df)

# json to dataframe

# load the json file into a dataframe
df = pd.read_json('employees.json')

# print the  DataFrame
print(df)

# Add a new column 'Bonus' which is 10% of the salary
df['Bonus'] = df['Salary'] * 0.10
print(df)

# Save the updated Dataframe to a new JSON file
df.to_json('employee_with_bonus.json',orient='records',lines=True)