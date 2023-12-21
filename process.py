import pandas as pd
csv_file_path = '1-safe.csv'

df = pd.read_csv(csv_file_path)

passenger_df = df[df['Di2'] == 1]

passenger_roads = {}

for index, row in passenger_df.iterrows():
    passenger_id = row['PassengerId']
    display_name = str(row['Display Name'])
    
    if passenger_id in passenger_roads:
        passenger_roads[passenger_id].add(display_name.replace(' ', '_'))
    else:
        passenger_roads[passenger_id] = {display_name.replace(' ', '_')}

# Write the results to the output.txt file
with open('output.txt', 'w', encoding='utf-8') as output_file:
    for passenger_id, roads_set in passenger_roads.items():
        roads_string = ' '.join(roads_set)
        output_file.write(f"{len(roads_set)} {roads_string}\n")
