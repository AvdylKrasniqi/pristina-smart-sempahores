import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
import overpass
from difflib import SequenceMatcher

# Replace this with the path to your CSV file
csv_file_path = '7-safe.csv'

# Read the CSV file
df = pd.read_csv(csv_file_path)

api = overpass.API(endpoint="http://localhost:8081/api/interpreter", timeout=600)


def get_from_nominatim(lat, lon, current_name):
    tries = 1
    nominatimResponse = None
    while tries < 1000:
        nominatimResponse = api.get('''way["highway"~"^(trunk|primary|secondary|tertiary|unclassified|residential)$"](around:{around}, {lat}, {long});out geom;'''.format(around=tries, lat=lat, long=lon), responseformat="json")
        print(nominatimResponse)
        if("elements" in nominatimResponse and len(nominatimResponse['elements']) > 0):
            for el in nominatimResponse['elements']:
                el['accuracy'] = SequenceMatcher(a=el['tags']['name'],b=current_name).ratio()
            return sorted(nominatimResponse['elements'], key=lambda x: x['accuracy'], reverse=True)
        if tries > 100:

            tries = tries + 10
        else:
            tries = tries + 1
    return None

def fetch_display_name(lat, lon, current_name):
    try:
        osm_ids = get_from_nominatim(lat, lon, current_name)
        res = requests.get("http://localhost:8080/lookup?osm_ids=W{}&format=json".format(osm_ids[0]['id']))
        if(res is not None and len(res.json()) > 0):
            res = res.json()[0]
            if "address" not in res:
                return [res['display_name'], res['osm_id'], res['osm_type']]
            elif "road" in res['address']:
                return [res['address']['road'], res['osm_id'], res['osm_type']]
            elif "building" in res['address']:
                return [res['address']['building'], res['osm_id'], res['osm_type']]
            elif "suburb" in res['address']:
                return [res['address']['suburb'], res['osm_id'], res['osm_type']]
            elif "quarter" in res['address']:
                return [res['address']['quarter'], res['osm_id'], res['osm_type']]
            elif "hamlet" in res['address']:
                return [res['address']['hamlet'], res['osm_id'], res['osm_type']]
            elif "village" in res['address']:
                return [res['address']['village'], res['osm_id'], res['osm_type']]
            elif "town" in res['address']:
                return [res['address']['town'], res['osm_id'], res['osm_type']]
            else:
                return [res['display_name'], res['osm_id'], res['osm_type']]
        else:
            return None
    except Exception as e:
        print(f"Error fetching data for lat: {lat}, lon: {lon} - {e}")
        return None


def process_row(row):
    if(row['osm_type'] == 'way'):
        return [row['Display Name'], row['osm_id'], row['osm_type']]
    else:
        result = fetch_display_name(row['Latitude'], row['Longitute'], row['Display Name'])
        if result is not None:
            [display_name, osmid, osmtype] = result
            return [display_name, osmid, osmtype]
        return [row['Display Name'], row['osm_id'], row['osm_type']]

with ThreadPoolExecutor() as executor:
    # Map process_row function to each row
    display_names = list(executor.map(process_row, [row for _, row in df.iterrows()]))
    print(process_row)


# Add the display names to the DataFrame
df['Display Name'] = display_names
df['osm_id'] = df['Display Name'].apply(lambda x: x[1])
df['osm_type'] = df['Display Name'].apply(lambda x: x[2])
df['Display Name'] = df['Display Name'].apply(lambda x: x[0])

# Now the DataFrame has an additional column with display names
print(df)

# Optionally, save the updated DataFrame to a new CSV file
df.to_csv('7-final.csv', index=False)
