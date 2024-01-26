import psycopg2
import overpass

conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="postgres",
                        port="45678")


overpassApi = overpass.API(endpoint="https://overpass-api.de/api/interpreter", timeout=(6000, 6000))


def getOSMIdForLatLon(lat, lon):
    try:
        response = overpassApi.get('''node(around:0,'''+ str(lat) + ''',''' + str(lon) + ''');''', responseformat="json")
        return response
    except Exception as e:
        print(e)
        return None


def registerOnDb(segment_id, osm_id, point_is_on_start, lat, lon):
    cursor = conn.cursor()
    cursor2 = conn.cursor()
    query = 'insert into intersections (geom, osm_id) values (ST_SetSRID(ST_MakePoint(' + str(lat) + ', ' + str(lon) + '),4326), ' + osm_id + ') ON CONFLICT DO NOTHING RETURNING id;'
    conn.commit()
    try:
        cursor.execute(query)
        intersectionId = cursor.fetchall()[0][0]
        if point_is_on_start:
            cursor2.execute('update segments set start_intersection_id = ' + str(intersectionId) + ' where id = ' + str(segment_id) + ';')
        else:
            cursor2.execute('update segments set end_intersection_id = ' + str(intersectionId) + ' where id = ' + str(segment_id) + ';')
        conn.commit()
    except Exception as e:
        print(segment_id)
        print(osm_id)
        print(e)
        pass
    finally:
        cursor.close()
        cursor2.close()

def getEmptyEndPoints():
    cursor = conn.cursor()
    query = 'select id, st_x(st_endpoint(geom)) as lon, st_y(st_endpoint(geom)) as lat from segments where end_intersection_id is null;'
    try:
        cursor.execute(query)
        res = cursor.fetchall()
        print(res)
        for el in res:
            overpassRes = getOSMIdForLatLon(el[2], el[1])
            if(overpassRes is not None and len(overpassRes['elements'])) > 0:
                if 'lat' in overpassRes['elements'][0] and'lon' in overpassRes['elements'][0] and 'id' in overpassRes['elements'][0]:
                    registerOnDb(el[0], str(overpassRes['elements'][0]['id']), False, str(overpassRes['elements'][0]['lat']), str(overpassRes['elements'][0]['lon']))
                pass
    except Exception as e:
        print(e)
    finally:
        cursor.close()



def getEmptyStartPoints():
    cursor = conn.cursor()
    query = 'select id, st_x(st_startpoint(geom)) as lon, st_y(st_startpoint(geom)) as lat from segments where start_intersection_id is null;'
    try:
        cursor.execute(query)
        res = cursor.fetchall()
        print(res)
        for el in res:
            overpassRes = getOSMIdForLatLon(el[2], el[1])
            if(overpassRes is not None and len(overpassRes['elements'])) > 0:
                if 'lat' in overpassRes['elements'][0] and'lon' in overpassRes['elements'][0] and 'id' in overpassRes['elements'][0]:
                    registerOnDb(el[0], str(overpassRes['elements'][0]['id']), True, str(overpassRes['elements'][0]['lat']), str(overpassRes['elements'][0]['lon']))
                pass
    except Exception as e:
        print(e)
    finally:
        cursor.close()




getEmptyStartPoints()