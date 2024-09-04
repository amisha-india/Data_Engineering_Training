import pandas as pd

# Load the csv file
df = pd.read_csv('sample_data.csv')
#Display the dataframe
print(df)

# check for missing values in each column
print(df.isnull().sum())

# Display rows with missing data
print(df[df.isnull().any(axis=1)])

# Replace empty strings and strings with only spaces with NaN
df.replace(r'^\s*$',np.nan, regex=True,inplace=True)

# display eith rows with missing data
print(df[df.isnull().any(axis=1)])

# Drop rows with any missing values
df_cleaned = df.dropna()

# Display the cleaned dataframe
print(df_cleaned)
