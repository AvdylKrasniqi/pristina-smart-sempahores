import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

# Replace this with the path to your CSV file
csv_file_path = '1.csv'

# Read the CSV file
df = pd.read_csv(csv_file_path)
df = df.dropna(subset=['DeviceDateTime'])

df['Di2_Shifted'] = df['Di2'].shift(1).fillna(0).astype(int)
print(df)

# Generate IDs for each group of consecutive 'true' values
df['GroupID'] = (df['Di2'] & ~df['Di2_Shifted']).cumsum()
df.loc[df['Di2'] == 0, 'GroupID'] = None

# Drop the extra column used for calculation
df.drop('Di2_Shifted', axis=1, inplace=True)


# df = df.loc[(df['Di1'] == 1) & (df['Di2'] == 1)]

print(df)

df['Time'] = pd.to_datetime(df['DeviceDateTime'], format='%M:%S.%f')
group_times = df.groupby('GroupID').agg({'Time': ['min', 'max']})

group_times['Duration'] = (group_times['Time']['max'] - group_times['Time']['min']).dt.total_seconds()
threshold_seconds = 60
valid_groups = group_times[group_times['Duration'] >= threshold_seconds].index

df = df[df['GroupID'].isin(valid_groups)]



df.drop('Time', axis=1, inplace=True)

df.to_csv('1-grupet.csv', index=False)

# Function to make an HTTP request and return the display name
def fetch_display_name(lat, lon):
    try:
        response = requests.get(f"http://localhost:8080/reverse?lat={lat}&lon={lon}&format=json").json()
        if "address" not in response:
            return response['display_name']
        elif "road" in response['address']:
            return response['address']['road']
        elif "building" in response['address']:
            return response['address']['building']
        elif "suburb" in response['address']:
            return response['address']['suburb']
        elif "quarter" in response['address']:
            return response['address']['quarter']
        elif "hamlet" in response['address']:
            return response['address']['hamlet']
        elif "village" in response['address']:
            return response['address']['village']
        elif "town" in response['address']:
            return response['address']['town']
        else:
            return response['display_name']
    except Exception as e:
        print(f"Error fetching data for lat: {lat}, lon: {lon} - {e}")
        return None

# Function to handle each row
def process_row(row):
    display_name = fetch_display_name(row['Latitude'], row['Longitute'])
    return display_name

# Using ThreadPoolExecutor to process rows in parallel
with ThreadPoolExecutor() as executor:
    # Map process_row function to each row
    display_names = list(executor.map(process_row, [row for _, row in df.iterrows()]))

# Add the display names to the DataFrame
df['Display Name'] = display_names

# Now the DataFrame has an additional column with display names
print(df)

# Optionally, save the updated DataFrame to a new CSV file
df.to_csv('1-safe.csv', index=False)
