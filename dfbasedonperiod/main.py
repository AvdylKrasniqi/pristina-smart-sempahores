import pandas as pd
import os

# Directory where your CSV files are stored
directory = './data'

# Initialize lists to hold dataframes for each time range
dfs_0730_0900 = []
dfs_0900_1100 = []
dfs_1100_1500 = []
dfs_1500_1800 = []

# Function to filter dataframe based on time range
def filter_by_time(df, start_hour, start_minute, end_hour, end_minute):
    return df[((df['DeviceDateTime'].dt.hour > start_hour) |
              ((df['DeviceDateTime'].dt.hour == start_hour) & (df['DeviceDateTime'].dt.minute >= start_minute))) &
              ((df['DeviceDateTime'].dt.hour < end_hour) |
              ((df['DeviceDateTime'].dt.hour == end_hour) & (df['DeviceDateTime'].dt.minute < end_minute)))]

# Loop through each file in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        df = pd.read_csv(file_path, parse_dates=['DeviceDateTime'])

        # Apply filters and append to respective lists
        dfs_0730_0900.append(filter_by_time(df, 7, 30, 9, 0))
        dfs_0900_1100.append(filter_by_time(df, 9, 0, 11, 0))
        dfs_1100_1500.append(filter_by_time(df, 11, 0, 15, 0))
        dfs_1500_1800.append(filter_by_time(df, 15, 0, 18, 0))

# Concatenate dataframes in each list
df_0730_0900 = pd.concat(dfs_0730_0900)
df_0900_1100 = pd.concat(dfs_0900_1100)
df_1100_1500 = pd.concat(dfs_1100_1500)
df_1500_1800 = pd.concat(dfs_1500_1800)

# Optional: Save each dataframe to a new CSV file
df_0730_0900.to_csv('0730_0900.csv', index=False)
df_0900_1100.to_csv('0900_1100.csv', index=False)
df_1100_1500.to_csv('1100_1500.csv', index=False)
df_1500_1800.to_csv('1500_1800.csv', index=False)
