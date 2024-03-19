import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2 import extras
from concurrent.futures import ThreadPoolExecutor, as_completed


df = pd.read_csv('14.csv', parse_dates=['DeviceDateTime'])




print('adding temp id')
df['temp_id'] = np.arange(1, len(df) + 1)
df.reset_index()

print('calculating total duration')

df['total_duration'] = df['DeviceDateTime'].shift(-1) - df['DeviceDateTime']

df['total_duration'] = df['total_duration'].dt.total_seconds()
df.at[df.index[-1], 'total_duration'] = 0

print('adding group id')

df['Di2_Shifted'] = df['Di2'].shift(1).fillna(0).astype(int)

df['GroupID'] = (df['Di2'] & ~df['Di2_Shifted']).cumsum()
df.loc[df['Di2'] == 0, 'GroupID'] = None

# Drop the extra column used for calculation
df.drop('Di2_Shifted', axis=1, inplace=True)

df = df.loc[(df['Di1'] == 1) & (df['Di2'] == 1)]

print('Dropping iterations less than 60 seconds')

df['Time'] = df['DeviceDateTime']
group_times = df.groupby('GroupID').agg({'Time': ['min', 'max']})

group_times['Duration'] = (group_times['Time']['max'] - group_times['Time']['min']).dt.total_seconds()
threshold_seconds = 60
valid_groups = group_times[group_times['Duration'] >= threshold_seconds].index

df = df[df['GroupID'].isin(valid_groups)]

df.drop('Time', axis=1, inplace=True)
print('Calculating on db')

dbresult = pd.DataFrame()

def getClosestSegment():
    global dbresult
    data = []

    # Assuming df is defined outside this function and is accessible
    for i in range(len(df)):
        try:
            data.append((str(df.loc[i, 'temp_id']), str(df.loc[i, 'Latitude']), str(df.loc[i, 'Longitude'])))
        except Exception as e:
            print('error on')
            print(e)
            pass

    # Function to process each chunk of data
    def process_chunk(chunk_data):

        conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="postgres",
                        port="45678")

        mystr = ', '.join(['(' + item[0] + ',' + item[1] + ',' + item[2] + ')' for item in chunk_data])
        query = '''with helper as (
        SELECT * from (
                          values ''' + mystr + '''
                      ) as t(id, lat, lon)
    ),
         processed as (
             select id, st_setsrid(st_point(helper.lon, helper.lat), 4326) as geom from helper
         ),
         segments_updated as (
             select *, replace(st_x(st_startpoint(geom))::numeric(10,6) || '' || st_y(st_startpoint(geom))::numeric(10,6), '.', '')   as starting,
                    replace(st_x(st_endpoint(geom))::numeric(10,6) || '' || st_y(st_endpoint(geom))::numeric(10,6), '.', '')   as ending,
                    replace(road_name, ' ', '_') as full_road_name
             from segments
         ),
         closest_segment as (
             select processed.id, processed.geom, segments_updated.geom as segment_geom, starting, ending, segments_updated.full_road_name || '_' || starting || '_' || ending as full_road_name, st_distance(segments_updated.geom, processed.geom, false) as distance
             from processed inner join segments_updated on st_distance(segments_updated.geom, processed.geom, false) < 50
             order by id, st_distance(segments_updated.geom, processed.geom, false) asc
         )
    select distinct on (id) id as temp_id, full_road_name, starting as start, ending as end, distance
    from closest_segment;'''

        print(query)
        # Initialize cursor and execute the query for the current chunk
        with conn.cursor() as cursor:
            try:
                cursor.execute(query)
                result = pd.DataFrame(cursor.fetchall(), columns=['temp_id', 'full_road_name', 'start', 'end', 'distance'])
                cursor.close()
                conn.close()
                return result
            except Exception as e:
                print(f"Error: {e}")
                cursor.close()
                conn.close()
                return pd.DataFrame()

    # Split data into chunks for parallel processing
    def split_data(data, chunk_size):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    chunk_size = 100  # Define your chunk size based on your data and testing
    chunks = list(split_data(data, chunk_size))

    # Use ThreadPoolExecutor to process data chunks in parallel
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]

        for future in as_completed(futures):
            dbresult = pd.concat([dbresult, future.result()], ignore_index=True)



# def getClosestSegment():
#     global dbresult
#     cursor = conn.cursor()
#
#     data = []
#
#     for i in range(len(df)):
#         try:
#             # print(df.loc[i])
#             data.append((str(df.loc[i, 'temp_id']), str(df.loc[i, 'Latitude']), str(df.loc[i, 'Longitude'])))
#         except Exception as e:
#             print('error on')
#             print(e)
#             pass
#     helpingdata = ['(' + item[0] + ',' + item[1] + ',' + item[2] + ')' for item in data]
#     mystr = ', '.join(helpingdata)
#     print(mystr)
#     query = '''with helper as (
#     SELECT * from (
#                       values ''' + mystr + '''
#                   ) as t(id, lat, lon)
# ),
#      processed as (
#          select id, st_setsrid(st_point(helper.lon, helper.lat), 4326) as geom from helper
#      ),
#      segments_updated as (
#          select *, replace(st_x(st_startpoint(geom))::numeric(10,6) || '' || st_y(st_startpoint(geom))::numeric(10,6), '.', '')   as starting,
#                 replace(st_x(st_endpoint(geom))::numeric(10,6) || '' || st_y(st_endpoint(geom))::numeric(10,6), '.', '')   as ending,
#                 replace(road_name, ' ', '_') as full_road_name
#          from segments
#      ),
#      closest_segment as (
#          select processed.id, processed.geom, segments_updated.geom as segment_geom, segments_updated.full_road_name || '_' || starting || '_' || ending as full_road_name, st_distance(segments_updated.geom, processed.geom, false) as distance
#          from processed inner join segments_updated on st_distance(segments_updated.geom, processed.geom, false) < 50
#          order by id, st_distance(segments_updated.geom, processed.geom, false) asc
#      )
# select distinct on (id) id as temp_id, full_road_name, distance
# from closest_segment;'''
#
#     print(query)
#     try:
#         cursor.execute(query)
#         dbresult = pd.DataFrame(cursor.fetchall(), columns=['temp_id', 'full_road_name', 'distance'])
#     except Exception as e:
#         print(f"Error: {e}")
#         pass
#     finally:
#         cursor.close()
#     pass

getClosestSegment()
print(dbresult)

df['temp_id'] = df['temp_id'].astype(int)
dbresult['temp_id'] = dbresult['temp_id'].astype(int)


result = df.merge(dbresult, how='inner', on='temp_id')
print(result)

result.to_csv('14-output.csv', index=False)