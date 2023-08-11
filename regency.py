import os
import json
from shapely.geometry import shape
import psycopg2
from psycopg2.extras import DictCursor

cwd = os.getcwd()
files = os.listdir(cwd)
type = "geojson"
files = [f for f in files if type in f]

conn = psycopg2.connect(host="localhost", user="postgres", password="123456", dbname="database_name", port=5432)

#Create table if not exist
cur = conn.cursor(cursor_factory=DictCursor)
create_table = "CREATE TABLE IF NOT EXISTS regency(regency_id int8, name text, geom geometry, latitude numeric, longitude numeric)"

cur.execute(create_table)

for file in files:
    with open(f"{cwd}/{file}", "r") as f:
        data = json.load(f)

    name = data["features"][0]["properties"]["name"]
    latitude = data["features"][0]["properties"]["latitude"]
    longitude = data["features"][0]["properties"]["longitude"]
    geom = shape(data["features"][0]["geometry"])
    id = file.replace(".geojson", "")
    insert_statement = f"INSERT INTO regency(regency_id, name, geom, latitude, longitude) VALUES ({id}, '{name}', ST_GeomFromText('{geom}', 4326), {latitude}, {longitude})"
    # insert_statement = "INSERT INTO regency(regency_id, name, geom, latitude, longitude) VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, %s)"
    # cur.execute(insert_statement, (id, name, geeom, latitude, longitude))
    cur.execute(insert_statement)

conn.commit()

print("Finished task")
cur.close()
conn.close()