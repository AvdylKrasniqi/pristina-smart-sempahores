from datetime import datetime

import pandas as pd

csv_file_path = '1-processed.csv'
df = pd.read_csv(csv_file_path)


def convert_to_timedelta(time_str):
    return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')

df['DeviceDateTime'] = df['DeviceDateTime'].apply(convert_to_timedelta)
# df['DeviceDateTime'] = pd.to_timedelta(df['DeviceDateTime'])

# Grouping by new_group_id and finding min/max times
time_grouped = df.groupby('new_group_id').agg(
    min_DeviceDateTime=('DeviceDateTime', 'min'),
    max_DeviceDateTime=('DeviceDateTime', 'max')
)
# Calculating the time difference (duration) for each group
time_grouped['duration'] = time_grouped['max_DeviceDateTime'] - time_grouped['min_DeviceDateTime']
time_grouped['total_duration'] = time_grouped['duration'].apply(lambda x: x.total_seconds())
# time_grouped = time_grouped.drop(columns=[('DeviceDateTime', 'min'), ('DeviceDateTime', 'max')])
# time_grouped = time_grouped.reset_index(inplace=True)
# print(time_grouped)
df_merged = pd.merge(time_grouped, df, on='new_group_id', how='left')
#
# print(df_merged)
#
#
total_duration = df_merged['total_duration'].sum()
unique_segments = df_merged['full_road_name'].nunique()
unique_intersections = pd.concat([df_merged['start'], df_merged['end']]).nunique()


print(f"{total_duration} {unique_intersections} {unique_segments} nr-shtigjeve 100")
for index, row in df_merged.iterrows():
    formatted_output = f"{row['start']} {row['end']} {row['full_road_name']} {row['total_duration']}"
    print(formatted_output)