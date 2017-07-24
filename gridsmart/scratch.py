import json
import sys
import secrets
import psycopg2
from psycopg2.extensions import AsIs

dbname = secrets.DWH['dbname']
user = secrets.DWH['user']
password = secrets.DWH['password']
host = secrets.DWH['host']
port = 5432

conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
cursor = conn.cursor()





sql = '''
    SELECT column_name, data_type, character_maximum_length
    from INFORMATION_SCHEMA.COLUMNS where table_name = 'zone';
'''

zones = json.loads(open('zones.json', 'r').read())

for zone in zones:
    columns = zone.keys()
    values = [zone[key] for key in zone]
    insert_statement = 'INSERT into zone (%s) values %s'
    query = cursor.mogrify(insert_statement, (AsIs(','.join(columns)), tuple(values)))
    cursor.execute(query)


sql = "ALTER TABLE zone RENAME COLUMN delayseconds TO delay_seconds"

cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN alerts DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN max_recall DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN delay_seconds DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN speed_scale DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN visibility_detection_enabled DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN delay_seconds_precise DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN protected_phases DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN latching DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN permissive_phases DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN extension_seconds DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN approach_type DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN expected_freeflow_speed DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN include_in_data DROP NOT NULL"
cursor.execute(sql)
sql = "ALTER TABLE zone ALTER COLUMN name DROP NOT NULL"
cursor.execute(sql)

sql = "DROP TABLE zone"

def que(sql):
    #  for quick querying
    # asusmes conn, cursor exist
    cursor.execute(sql)
    results = cursor.fetchall()
    return(results)

#   get all zones with site name


sql = '''
    SELECT zone.site_id, site.id, site.name
    FROM zone
    FULL OUTER JOIN site ON zone.site_id=site.id;
'''



######## PONY
from pony.orm import *
from datetime import datetime

db = Database()
db.bind(provider='postgres', user=user, password=password, host=host, database=dbname, port=5432)
sql_debug(True)
db.generate_mapping(create_tables=False)











