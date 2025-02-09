from datetime import datetime

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

# Replace this with the path to your CSV file
csv_file_path = '1500_1800.csv'

# Read the CSV file
df = pd.read_csv(csv_file_path)
df = df.dropna(subset=['DeviceDateTime'])

def convert_to_timedelta(time_str):
    return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')

df['DeviceDateTime'] = df['DeviceDateTime'].apply(convert_to_timedelta)
df = df.sort_values(by='DeviceDateTime')

df.reset_index()

df['total_duration'] = df['DeviceDateTime'].shift(-1) - df['DeviceDateTime']

df['total_duration'] = df['total_duration'].dt.total_seconds()
df.at[df.index[-1], 'total_duration'] = 0


df['Di2_Shifted'] = df['Di2'].shift(1).fillna(0).astype(int)
print(df)

# Generate IDs for each group of consecutive 'true' values
df['GroupID'] = (df['Di2'] & ~df['Di2_Shifted']).cumsum()
df.loc[df['Di2'] == 0, 'GroupID'] = None

# Drop the extra column used for calculation
df.drop('Di2_Shifted', axis=1, inplace=True)

df = df.loc[(df['Di1'] == 1) & (df['Di2'] == 1)]

print(df)

df['Time'] = df['DeviceDateTime']
# group_times = df.groupby('GroupID').agg({'Time': ['min', 'max']})
#
# group_times['Duration'] = (group_times['Time']['max'] - group_times['Time']['min']).dt.total_seconds()
# threshold_seconds = 60
# valid_groups = group_times[group_times['Duration'] >= threshold_seconds].index

# df = df[df['GroupID'].isin(valid_groups)]

df.drop('Time', axis=1, inplace=True)

#df.to_csv('16-grupet.csv', index=False)

# Function to make an HTTP request and return the display name
def fetch_display_name(lat, lon):
    try:
        print(f"http://localhost:8080/reverse?lat={lat}&lon={lon}&format=json&osm_type=W&layer=address&zoom=17&addressdetails=1")
        response = requests.get(f"http://localhost:8080/reverse?lat={lat}&lon={lon}&format=json").json()
        if "error" in response:
            print(response['error'])
            return ['NotAvailableRoad', '', '']
        if "address" not in response:
            return [response['display_name'], response['osm_id'], response['osm_type']]
        elif "road" in response['address']:
            return [response['address']['road'], response['osm_id'], response['osm_type']]
        elif "building" in response['address']:
            return [response['address']['building'], response['osm_id'], response['osm_type']]
        elif "suburb" in response['address']:
            return [response['address']['suburb'], response['osm_id'], response['osm_type']]
        elif "quarter" in response['address']:
            return [response['address']['quarter'], response['osm_id'], response['osm_type']]
        elif "hamlet" in response['address']:
            return [response['address']['hamlet'], response['osm_id'], response['osm_type']]
        elif "village" in response['address']:
            return [response['address']['village'], response['osm_id'], response['osm_type']]
        elif "town" in response['address']:
            return [response['address']['town'], response['osm_id'], response['osm_type']]
        else:
            return [response['display_name'], response['osm_id'], response['osm_type']]
    except Exception as e:
        print(f"Error fetching data for lat: {lat}, lon: {lon} - {e}")
        return ['NotAvailableRoad', '', '']


# Function to handle each row
def process_row(row):
    [display_name, osmid, osmtype] = fetch_display_name(row['Latitude'], row['Longitude'])
    print([display_name, osmid, osmtype])
    return [display_name, osmid, osmtype]

# Using ThreadPoolExecutor to process rows in parallel
with ThreadPoolExecutor() as executor:
    # Map process_row function to each row
    display_names = list(executor.map(process_row, [row for _, row in df.iterrows()]))
    print(process_row)


# Add the display names to the DataFrame
df['Display Name'] = display_names
df['osm_id'] = df['Display Name'].apply(lambda x: x[1])
df['osm_type'] = df['Display Name'].apply(lambda x: x[2])
df['Display Name'] = df['Display Name'].apply(lambda x: x[0])

df.to_csv('1500_1800-before-safe.csv', index=False)

df = df[df['Display Name'] != 'NotAvailableRoad']

# Now the DataFrame has an additional column with display names
print(df)

# Optionally, save the updated DataFrame to a new CSV file
df.to_csv('1500_1800-safe.csv', index=False)
