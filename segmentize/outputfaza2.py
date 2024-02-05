from datetime import datetime

import pandas as pd

csv_file_path = '16-processed.csv'
df = pd.read_csv(csv_file_path)


# def convert_to_timedelta(time_str):
#     return datetime.strptime(str(time_str), '%Y-%m-%d %H:%M:%S.%f')

# df['DeviceDateTime'] = df['DeviceDateTime'].apply(convert_to_timedelta)
# df = df.sort_values(by='DeviceDateTime')

# df['total_duration'] = df['DeviceDateTime'].shift(-1) - df['DeviceDateTime']
# print(df['DeviceDateTime'].shift(-1))
# print(df['DeviceDateTime'])

# df['total_duration'] = df['total_duration'].dt.total_seconds()
# df.at[df.index[-1], 'total_duration'] = 0


def g(df):
    df['road_shifted'] = df['full_road_name'].shift(1).fillna('0')
    group_id = 0
    for i in range(len(df)):
        if df.loc[i, 'full_road_name'] == df.loc[i, 'road_shifted']:
            df.loc[i, 'road_group_id'] = group_id
        else:
            group_id += 1
            df.loc[i, 'road_group_id'] = group_id
    return df


df = g(df)

average_time_per_group = df.groupby('road_group_id')['total_duration'].sum()

# print(average_time_per_group)

average_time_per_group = average_time_per_group.reset_index()

df.drop('total_duration', axis=1, inplace=True)

df_merged = pd.merge(average_time_per_group, df, on='road_group_id', how='left')

df_merged = df_merged.groupby(['road_group_id', 'full_road_name']).last()

df_merged = df_merged.reset_index()

filtered_df = df_merged[~(df_merged['start'] == -1) & ~(df_merged['end'] == -1)]

# print(filtered_df)
total_duration = (int)(filtered_df.drop_duplicates(subset=['full_road_name'])['total_duration'].sum())
unique_segments = filtered_df['full_road_name'].nunique()
unique_intersections = pd.concat([filtered_df['start'], filtered_df['end']]).nunique()
nr_shtigjeve = filtered_df.drop_duplicates(subset=['full_road_name'])['GroupID'].nunique()

print(f"{total_duration} {unique_intersections} {unique_segments} {nr_shtigjeve} 100")
for index, row in filtered_df.drop_duplicates(subset=['full_road_name']).iterrows():
    # if not (row['start'] == -1 or row['end'] == -1):
    formatted_output = f"{row['start']} {row['end']} {row['full_road_name']} {int(row['total_duration'])}"
    print(formatted_output)

grouped_by_group_id = filtered_df.groupby('GroupID')



for group_name, group_data in grouped_by_group_id:
    # if(group_data['full_road_name'].count() > 1):
    print(f"{group_data['full_road_name'].count()}", end=" ")
    for segment in group_data.iterrows():
        print(segment[1]['full_road_name'], end=" ")
    print("")