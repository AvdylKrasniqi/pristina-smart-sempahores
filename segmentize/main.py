import json

import overpass
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2 import extras


# pristinaBbox = '42.6551575,21.1541089,42.6598018,21.1605985'
pristinaBbox = '42.6290946,21.1066198,42.6969650,21.2136267'
pristinaBbox2 = '42.6290946,21.1066198,42.6969650,21.2136268'
intersections = []

overpassApi = overpass.API(endpoint="https://overpass-api.de/api/interpreter", timeout=(6000, 6000))

conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="postgres",
                        port="45678")


def getAllRoads(bbox):
    try:
        response = overpassApi.get('''way["highway"~"^(trunk|primary|secondary|tertiary|unclassified|residential)$"](''' + bbox + ''')->.streets; .streets out geom;''', responseformat="json");
        print(response)
        return response
    except Exception as e:
        print(e)
        return None

def getAllIntersections(bbox):
    try:
        response = overpassApi.get('''way["highway"~"^(trunk|primary|secondary|tertiary|unclassified|residential)$"](''' + bbox + ''')->.streets; node(way_link.streets:2-)->.connections; 
            foreach .connections->.connection( 
                way(bn.connection); 
                if (u(t["name"]) == "< multiple values found >") { 
                    (.connection; .intersections;)->.intersections; 
                } 
            ); 
            .intersections out geom;''', responseformat="json")
        # print(response)
        global intersections
        intersections = response
        return response
    except Exception as e:
        print(e)
        return None

def getRoadByName(bbox, roadName):
    pass

class MockCursor:
    def executemany(self, query, data):
        for params in data:
            built_query = query % params
            print("Built query:", built_query)

def insertRoads(bbox):
    global intersections
    roads = getAllRoads(bbox)
    getAllIntersections(bbox)
    if roads is not None and intersections is not None:
        cursor = conn.cursor()
        query = f"INSERT INTO roads (osm_id, geom, name) VALUES (%s, ST_MakeLine(%s), %s) ON CONFLICT DO NOTHING"
        # data = [
        #     (str(item['id']),
        #      AsIs('ARRAY[' + ', '.join(['ST_SetSRID(ST_MakePoint(' + str(point['lon']) + ', ' + str(point['lat']) + '),4326)' for point in roads['elements'][0]['geometry']]) + ']'),
        #      str(item['tags']['name'])) for item in roads['elements'] if 'name' in item['tags']
        # ]
        data = []
        for road in roads['elements']:
            points = []
            if 'name' in road['tags']:
                for point in road['geometry']:
                    points.append('ST_SetSRID(ST_MakePoint(' + str(point['lon']) + ', ' + str(point['lat']) + '),4326)')
                data.append((str(road['id']), AsIs('ARRAY[' + ', '.join(points) + ']'), road['tags']['name']  ))

        intersectionsToAdd = []
        intersectionsIds = [item['id'] for item in intersections['elements'] if 'id' in item and 'lat' in item and 'lon' in item]
        intersectionsQuery = f"INSERT INTO intersections (osm_id, geom) VALUES (%s, ST_SetSRID(ST_Point(%s, %s), 4326)) ON CONFLICT DO NOTHING"

        for road in roads['elements']:
            for geometry, node in zip(road['geometry'], road['nodes']):
                if node in intersectionsIds:
                    intersectionsToAdd.append((node, geometry['lon'], geometry['lat']))
        try:
            cursor.executemany(query, data)
            if len(intersectionsToAdd) > 0:
                cursor.executemany(intersectionsQuery, intersectionsToAdd)
            conn.commit()
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            # conn.close()


def insertIntersections(bbox):
    global intersections
    if intersections is not None:
        cursor = conn.cursor()
        query = f"INSERT INTO intersections (osm_id, geom) VALUES (%s, ST_SetSRID(ST_Point(%s, %s), 4326)) ON CONFLICT DO NOTHING"
        data = [(item['id'], item['lon'], item['lat']) for item in intersections['elements'] if 'lat' in item and 'lon' in item and 'id' in item]
        try:
            cursor.executemany(query, data)
            conn.commit()
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            # conn.close()




def fillDB(bbox):
    pass


insertRoads(pristinaBbox)
insertIntersections(pristinaBbox2)
conn.close()