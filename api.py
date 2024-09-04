# import requests
#
# response = requests.get("https://dummyjson.com/products/1")
# print(response.json()) #print the HTTP status code

# pandas
import pandas as pd

# Creating a dataframe from a dictinory with Indian names
data = {
    "Name": ["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25,30,35,40,45],
    "City": ["Mumbai", "Delhi", "Banglore","Chennai", "Pune"]

}

df = pd.DataFrame(data)
print(df)

# Accessing a single column
print(df["Name"])

# Accessing multiple columns
print(df[["Name", "Age"]])

# Accessing rows using index
print(df.iloc[0]) #first row

# filtering rows where age is greatar than 30
filtered_df = df[df["Age"]>30]
print(filtered_df)

# Adding a new column salary
df["Salary"] = [5000, 60000,70000,80000,90000]
print(df)

# Sorting by Age
sorted_df = df.sort_values(by="Age", ascending=False)
print(sorted_df)

# Rename
df_renamed = df.rename(columns={"Name": "Full Name", "Age": "Years"})
print(df_renamed)

# Drop
df_dropped = df.drop(columns=["City"])
print(df_dropped)

# Creatiing new col 'Seniority ' based on the age
df['Seniority'] = df['Age'].apply(lambda x: 'Senior' if x>=35 else 'Junior')
print(df)

# Group  by city and calculate the average salary in each city
df_grouped = df.groupby('City')["Salary"].mean()
print(df_grouped)

# Apply a customer function to the 'Salary' column to add a 10% bonus usage new
def add_bonus(salary):
    return salary *1.10
df['Salary_with_bonus'] = df['Salary'].apply(add_bonus)
print(df)

#create new dataframe
df_new = pd.DataFrame({
    "Name": ["Amit","Priya","Ravi"]
    "Bonus": [5000,6000,7000]
})
# Merge based on the 'Name' column
df_merged = pd.merge(df,df_new, on ="Name", how="left")
print(df_merged)

# Concatenate the two Dataframes
df_concat = pd.concat([df,df_new], ignore_index=True)
print(df_concat)

# person with more than 50000 salary
high_salary_df = df[df['salary'] > 5000]

# person's name start with A
a_name_df = df[df['name'].str.startswith('A')]

