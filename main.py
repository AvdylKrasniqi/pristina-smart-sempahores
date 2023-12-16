import pandas as pd
import requests
from math import radians, cos, sin, asin, sqrt
from concurrent.futures import ThreadPoolExecutor

# Replace this with the path to your CSV file
csv_file_path = '1.csv'

# Read the CSV file
df = pd.read_csv(csv_file_path)


# Define the threshold for significant movement (in kilometers)
movement_threshold = 0.01

# Haversine formula to calculate the distance between two lat-long points
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return c * r

# Function to find ranges
def find_ranges(df, threshold):
    ranges = []
    start_index = 0
    last_lat, last_lon = df.iloc[0]['Latitude'], df.iloc[0]['Longitute']

    for i in range(1, len(df)):
        current_lat, current_lon = df.iloc[i]['Latitude'], df.iloc[i]['Longitute']
        if haversine(last_lon, last_lat, current_lon, current_lat) > threshold:
            ranges.append((start_index, i - 1))
            start_index = i
        last_lat, last_lon = current_lat, current_lon

    ranges.append((start_index, len(df) - 1))  # Add the last range
    return ranges

ranges = find_ranges(df, movement_threshold)


# Function to make an HTTP request and return the display name
def fetch_display_name(lat, lon):
    try:
        response = requests.get(f"http://localhost:8080/reverse?lat={lat}&lon={lon}&format=json")
        return response.json()['display_name']
    except Exception as e:
        print(f"Error fetching data for lat: {lat}, lon: {lon} - {e}")
        return None

range_results = []
# Function to process a range
def process_range(range_tuple):
    start_index, end_index = range_tuple
    print(f"Range: {start_index} - {end_index}")
    display_name = fetch_display_name(df.iloc[start_index]['Latitude'], df.iloc[start_index]['Longitute'])
    return (start_index, end_index, display_name)

# Using ThreadPoolExecutor to process ranges in parallel
with ThreadPoolExecutor() as executor:
    range_results = list(executor.map(process_range, ranges))

# Updating the DataFrame based on range results
for start_index, end_index, display_name in range_results:
    df.loc[start_index:end_index, 'Display Name'] = display_name

# Now the DataFrame has an additional column with display names
print(df)

df.to_csv('1-processed.csv', index=False)