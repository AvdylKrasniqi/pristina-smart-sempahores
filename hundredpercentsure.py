import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

# Replace this with the path to your CSV file
csv_file_path = '1.csv'

# Read the CSV file
df = pd.read_csv(csv_file_path)

df = df.loc[(df['Di1'] == 1) & (df['Di2'] == 1)]

print(df)

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
