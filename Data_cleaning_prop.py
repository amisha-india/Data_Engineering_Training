import pandas as pd
import numpy as np

# load the csv file
df = pd.read_csv('Sample_data.csv')

print("Before transformation")
print(df)

# Ensure there are no leading/ trailing spaces in col names
df.columns = df.columns.str.strip()

# strip spaces from the city col and replace empty strings with NaN
df['City'] = df['City'].str.strip().replace('', np.nan)

# fill missing values in the age col with the median age
df['Age'] = pd.to_numeric(df['Age'].str.strip(),error='coerce')
df['Age'] = df['Age'].fillna(df['Age'].median())

#fill missing values in the salary col with the median salary
df['Salary'] = df['Salary'].fillna(df['Salary'].median())

#display the dataframe after filling missing values
print(df)