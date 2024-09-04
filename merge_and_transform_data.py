import pandas as pd
# sample dataframe
df1 = pd.DataFrame({
    'employee_id': [1, 2, 3, 4],
    'employee_name': ['John Doe', 'Jane Smith', 'Sam Brown', 'Emily davis']
})

df2 = pd.DataFrame({
    'employee_id': [3, 4, 5, 6],
    'department': ['Finance', 'HR', 'IT', 'Marketing']
})

# Merge the DataFrame on 'Employee_id'
merge_df = pd.merge(df1, df2, on='employee_id', how='inner')
print(merge_df)

# group by

def = pd.DataFrame ({
    'employee_id': [1, 2, 3, 4],
    'employee_name': ['John Doe', 'Jane Smith', 'Sam Brown', 'Emily davis'],
    'Salary':[3000,4000,5000,6000]
})
#Group by department and calculate mean salary
grouped_df = df.groupby('department')['Salary'].mean().reset_index()
print(grouped_df)