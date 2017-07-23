import csv
import pdb
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import secrets

'''
GRIDSMART counts file fields:
    timestamp
    approach
    turn
    length_ft
    speed_mph
    phase
    light
    seconds_of_light_state
    seconds_since_green
    recent_free_flow_speed_mph
    calibration_free_flow_speed_mph
    include_in_approach_data
    zone_id
'''

def buildInsertTemplate(columns):
    col_str = ', '.join(str(c) for c in columns)

    template = '''
        INSERT INTO count ( {} )
        VALUES %s
    '''.format(col_str)

    return template

def replaceDate(row):
    row[0] = datetime.today()
    return tuple(row)

def processCountFile(conn, fname, page_size):
    cursor = conn.cursor()
    inserts = []
    count = 0

    with open(fname, 'r') as infile:
        reader = csv.reader(infile)
        next(reader, None)  # skip header

        for row in reader:        
            count += 1
            index += 1

            #  timestamps will need to be parsed and formatted to be database-friendly
            row = replaceDate(row)
            
            #  inserts is list of records that are to be inserted in this chunk
            inserts.append(row)

            if index == page_size:
                index = 0
                print('write a chunk')

                #  http://initd.org/psycopg/docs/extras.html
                execute_values(cursor, QUERY_TEMPLATE, inserts, page_size=PAGE_SIZE)

                #  empty insert array
                inserts = []
                print(count)

        #  write the last bit of rows
        if (inserts):
            execute_values(cursor, QUERY_TEMPLATE, inserts, page_size=PAGE_SIZE)    

        cursor.close()

        return '{} rows written'.format(count)



# todo
# update knack_data_pub to drop camera IPs to FME source files as JSON
# write code that creates tables
# write code that checks current date for each reader and creates a 'todo' list
# write code that gets count files
# write code that parses raw count, appends an 'upload date' and site code to raw counts
# update code that inserts raw counts and add date parsing
# write code the inserts aggregate counts

#  get IP addresses
#  get latest data date and site info from camera
#  get latest data date from db
#  create work list
#  site_id|   date |process_status|rows created
#  abc1233|1-2-2017|complete

fname = '2016-10-03.csv'
index = 0
page_size = 3000
cols = ['timestamp', 'approach', 'turn', 'length_ft', 'speed_mph', 'phase', 'light', 'seconds_of_light_state', 'seconds_since_green', 'recent_free_flow_speed_mph', 'calibration_free_flow_speed_mph', 'include_in_approach_data', 'zone_id']

dbname = secrets.DWH['dbname']
user = secrets.DWH['user']
password = secrets.DWH['password']
host = secrets.DWH['host']
port = 5432

conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
template = buildInsertTemplate(cols)
results = processCountFile(conn, fname, page_size)

conn.close()












