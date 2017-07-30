'''
Destroy and create GRIDSMART traffic data tables in postgres db
'''
import sys
import psycopg2
from psycopg2 import sql
import secrets
import pdb

while True:
    print("\n\n!!! You are about to delete all data in the GRIDSMART database !!!")
    res = raw_input("Type \"yes\" to continue or anything else to quit: ")
    if res.lower() == "yes":
        break
    else:
        sys.exit()

dbname = secrets.DWH['dbname']
user = secrets.DWH['user']
password = secrets.DWH['password']
host = secrets.DWH['host']
port = 5432

tables = ['site', 'zone', 'traffic_count']

conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
cursor = conn.cursor()


for table in tables:
    query = sql.SQL( "TRUNCATE TABLE {}").format( sql.Identifier(table) )
    print(query)
    cursor.execute(query)
    
conn.commit()

for table in tables:
    query = sql.SQL( "DROP TABLE {}").format( sql.Identifier(table) )
    print(query)
    cursor.execute(query)

conn.commit()

sql = '''
    CREATE TABLE "site" (
        "id" TEXT PRIMARY KEY,
        "name" TEXT
    )
'''
cursor.execute(sql)

sql = '''
    CREATE TABLE "zone" (
        "id" TEXT PRIMARY KEY,
        "max_recall" BOOLEAN,
        "alerts" TEXT,
        "delay_seconds" INTEGER,
        "site_id" TEXT,
        "turn_type" TEXT,
        "speed_scale" INTEGER,
        "visibility_detection_enabled" BOOLEAN,
        "delay_seconds_precise" DOUBLE PRECISION,
        "protected_phases" TEXT,
        "latching" BOOLEAN,
        "permissive_phases" TEXT,
        "extension_seconds" INTEGER,
        "approach_type" TEXT,
        "expected_freeflow_speed" INTEGER,
        "include_in_data" BOOLEAN,
        "name" TEXT,
        "number_of_lanes" INTEGER
    )
'''
cursor.execute(sql)

sql = '''
    CREATE TABLE "traffic_count" (
        "id" TEXT PRIMARY KEY,
        "timestamp" TIMESTAMP WITHOUT TIME ZONE,
        "approach" TEXT,
        "turn" TEXT,
        "length_ft" INTEGER,
        "speed_mph" INTEGER,
        "phase" INTEGER,
        "light" TEXT,
        "seconds_of_light_state" DOUBLE PRECISION,
        "seconds_since_green" DOUBLE PRECISION,
        "recent_free_flow_speed_mph" DOUBLE PRECISION,
        "calibration_free_flow_speed_mph" INTEGER,
        "include_in_approach_data" INTEGER,
        "zone_id" TEXT
    )
'''

cursor.execute(sql)
conn.commit()

sql = '''
    SELECT * from information_schema.tables
'''
cursor.execute(sql)
res = cursor.fetchall()

for t in res:
    if t[2] in tables:
        print(t)

conn.close()