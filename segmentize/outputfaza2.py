from datetime import datetime

import pandas as pd

csv_file_path = '7-processed.csv'
df = pd.read_csv(csv_file_path)


def convert_to_timedelta(time_str):
    return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')

df['DeviceDateTime'] = df['DeviceDateTime'].apply(convert_to_timedelta)

df['total_duration'] = df['DeviceDateTime'].shift(-1) - df['DeviceDateTime']
df['total_duration'] = df['total_duration'].dt.total_seconds()
df.at[df.index[-1], 'total_duration'] = 0
average_time_per_group = df.groupby('new_group_id')['total_duration'].mean()

average_time_per_group = average_time_per_group.reset_index()

df.drop('total_duration', axis=1, inplace=True)

df_merged = pd.merge(average_time_per_group, df, on='new_group_id', how='left')

df_merged = df_merged.groupby(['new_group_id', 'full_road_name']).last()

df_merged = df_merged.reset_index()
# print(df_merged['total_duration'])
#
# print(df_merged)
#
#

filtered_df = df_merged[~(df_merged['start'] == -1) & ~(df_merged['end'] == -1)]
total_duration = (int)(filtered_df['total_duration'].sum())
unique_segments = filtered_df['full_road_name'].count()
unique_intersections = unique_segments + 4
nr_shtigjeve = filtered_df['GroupID'].nunique()

unique_segments_names = filtered_df['full_road_name'].unique()


print(f"{total_duration} {unique_intersections} {unique_segments} {nr_shtigjeve} 100")
for index, row in filtered_df.iterrows():
    # if not (row['start'] == -1 or row['end'] == -1):
    formatted_output = f"{row['start']} {row['end']} {row['full_road_name']} {int(row['total_duration'])}"
    print(formatted_output)

grouped_by_group_id = filtered_df.groupby('GroupID')

# print(grouped_by_new_group_id)


for group_name, group_data in grouped_by_group_id:
    # if(group_data['full_road_name'].count() > 1):
    print(f"{group_data['full_road_name'].count()}", end=" ")
    for segment in group_data.iterrows():
        print(segment[1]['full_road_name'], end=" ")
    print("")