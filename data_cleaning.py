import pandas as pd
import numpy as np
df=pd.read_csv("sample_data.csv")
print("Before Transformation")
print(df)

# ensure there are no leading/trailing spaces in column names
df.columns=df.columns.str.strip()

# Strip spaces from the 'City' column and replace empty strings with NaN
df['City']=df['City'].str.strip().replace("",np.nan)
# Fill the missing values of City with unknown
df['City']=df['City'].fillna('Unknown')
# Fill missing values in the "Age" column with the median age
df['Age']=pd.to_numeric(df["Age"].str.strip(), errors='coerce')
df['Age']=df['Age'].fillna(df['Age'].median())

# Fill missing values in the salary column with median salary
df['Salary']=df['Salary'].fillna(df['Salary'].median())

# Display Dataframe after missing values
print(df)
