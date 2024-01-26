import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs


csv_file_path = '../preprocess/7-final.csv'
df = pd.read_csv(csv_file_path)


one_road_to_one_segment = pd.DataFrame([], columns=['osm_id', 'Latitude', 'Longitude', 'start', 'end', 'full_road_name'])
one_road_to_multiple_segment = pd.DataFrame([], columns=['osm_id', 'Latitude', 'Longitude', 'start', 'end', 'full_road_name'])


conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="postgres",
                        port="45678")

def getClosestSegmentByCoordsAndOsmId():

    global one_road_to_multiple_segment
    cursor = conn.cursor()

    data = []

    for i in range(len(df)):
        try:
            print(df.loc[i])
            data.append((str(df.loc[i, 'Latitude']), str(df.loc[i, 'Longitude']), str(df.loc[i, 'osm_id'])))
        except Exception as e:
            print(e)
    mystr = ', '.join(['(' + item[0] + ',' + item[1] + ',' + item[2] + ')' for item in data])
    query = '''with helper as (
    select * from (
       values
              ''' + mystr + '''
    ) as t(lat, lon, osm_id)
)
select distinct on (helper_lat, helper_lon, helper_osm_id)  helper_osm_id as osm_id, helper_lat as Latitude, helper_lon as Longitude, coalesce(start_intersection_id - 61943, -1) as start, coalesce(end_intersection_id, -1) as end, replace(road_name, ' ', '_') || '_' || coalesce(start_intersection_id - 61943, -1) || '_' || coalesce(end_intersection_id - 61943, -1) as full_road_name from (
select helper.lat as helper_lat ,helper.lon as helper_lon, helper.osm_id as helper_osm_id, segments.*
from segments inner join roads_segements on segments.id = roads_segements.segement_id
inner join roads on roads_segements.road_id = roads.id
inner join helper on cast(roads.osm_id as varchar) = cast(helper.osm_id as varchar)
order by st_distance(st_setsrid(st_point(helper.lon, helper.lat), 4326), roads.geom) asc) a;'''
    try:
        cursor.execute(query)
        one_road_to_multiple_segment = pd.DataFrame(cursor.fetchall(), columns=['osm_id', 'Latitude', 'Longitude', 'start', 'end', 'full_road_name'])
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
    pass


def getSingleSegmentRoads():
    global one_road_to_one_segment
    cursor = conn.cursor()
    query = '''select roads.osm_id, coalesce(segments.start_intersection_id - 61943, -1) as start, coalesce(segments.end_intersection_id - 61943, -1) as end, replace(segments.road_name, ' ', '_') || '_' || coalesce(segments.start_intersection_id - 61943, -1) || '_' || coalesce(segments.end_intersection_id - 61943, -1) as full_road_name from segments inner join public.roads_segements rs on segments.id = rs.segement_id
inner join (
select id from (select roads.id, count(roads.id)
from roads inner join public.roads_segements rs on roads.id = rs.road_id
group by roads.id) where count = 1) a on rs.road_id = a.id
inner join roads on a.id = roads.id;'''
    try:
        cursor.execute(query)
        one_road_to_one_segment = pd.DataFrame(cursor.fetchall(), columns=['osm_id', 'start', 'end', 'full_road_name'])
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()

getSingleSegmentRoads()

# print(df)
# print(one_road_to_one_segment)
df['osm_id'] = df['osm_id'].astype(str)
one_road_to_one_segment['osm_id'] = one_road_to_one_segment['osm_id'].astype(str)

merged_df = pd.merge(df, one_road_to_one_segment, on='osm_id', how='left')
# print(merged_df)

getClosestSegmentByCoordsAndOsmId()


merged_df['osm_id'] = merged_df['osm_id'].astype(str)
merged_df['Latitude'] = merged_df['Latitude'].astype(str)
merged_df['Longitude'] = merged_df['Longitude'].astype(str)

one_road_to_multiple_segment['osm_id'] = one_road_to_multiple_segment['osm_id'].astype(str)
one_road_to_multiple_segment['Latitude'] = one_road_to_multiple_segment['Latitude'].astype(str)
one_road_to_multiple_segment['Longitude'] = one_road_to_multiple_segment['Longitude'].astype(str)

second_merge = pd.merge(merged_df[merged_df['start'].isna()].drop(['start', 'end', 'full_road_name'], axis=1), one_road_to_multiple_segment, on=['osm_id', 'Latitude', 'Longitude'], how='left')
conn.close()

merged_df = merged_df[~merged_df['start'].isna()]

concat = pd.concat([merged_df, second_merge])
concat = concat[~concat['start'].isna()]
concat = concat[~concat['end'].isna()]
concat['start'] = concat['start'].astype(int)
concat['end'] = concat['end'].astype(int)

concat['new_group_id'] = (concat['full_road_name'] != concat['full_road_name'].shift()).cumsum()

concat.to_csv('7-processed.csv', index=False)
