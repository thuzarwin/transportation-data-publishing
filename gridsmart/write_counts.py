#  do you need to conn.commit() after execute values?
import csv
import json
import os
import sys
import socket
from datetime import datetime
import logging
import traceback
import pytz
import pdb
import psycopg2
from psycopg2.extras import execute_values
import arrow
import secrets


def buildInsertTemplate(columns, table):
    col_str = ', '.join(str(c) for c in columns)

    template = '''
        INSERT INTO {} ( {} )
        VALUES %s
    '''.format(table, col_str)

    return template

def replaceDate(row, datetime_format):
    time_string = row[0]
    dt = arrow.get(time_string, datetime_format)
    dt = dt.replace(tzinfo='US/Central')
    row[0] = dt.format()  #  format to ISO 8601 (dropping milliseconds)
    return tuple(row)

def getRowCount(conn, table, primary_key):
    cursor = conn.cursor()
    cursor.mogrify("SELECT COUNT(%s) FROM %s AS COUNT;", (primary_key, table))
    return cursor.fetchall()

def processCountFile(conn, fname, page_size, query_template):
    cursor = conn.cursor()
    inserts = []
    count = 0
    index = 0

    with open(fname, 'r') as fin:
        reader = csv.reader(fin)
        next(reader, None)  # skip header

        for row in reader:        
            count += 1
            index += 1
            #  timestamps will need to be parsed and formatted to be database-friendly
            row = replaceDate(row, count_datetime_format)
            #  inserts is list of records that are to be inserted in this chunk
            inserts.append(row)

            if index == page_size:
                index = 0
                print('write a chunk')
                #  http://initd.org/psycopg/docs/extras.html
                execute_values(cursor, query_template, inserts, page_size=page_size)
                #  empty insert array
                inserts = []
                print(count)
                


        #  write the last bit of rows
        if (inserts):
            execute_values(cursor, query_template, inserts, page_size=page_size)    

        cursor.close()

        return '{} rows written'.format(count)





fname = 'import_master.csv'

page_size = 3000
cols = ['timestamp', 'approach', 'turn', 'length_ft', 'speed_mph', 'phase', 'light', 'seconds_of_light_state', 'seconds_since_green', 'recent_free_flow_speed_mph', 'calibration_free_flow_speed_mph', 'include_in_approach_data', 'zone_id']
count_datetime_format = 'YYYYMMDDTHHmmss.S'

dbname = secrets.DWH['dbname']
user = secrets.DWH['user']
password = secrets.DWH['password']
host = secrets.DWH['host']
port = 5432

conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
template = buildInsertTemplate(cols, 'traffic_count')
results = processCountFile(conn, fname, page_size, template)

#  results = getRowCount(conn, 'traffic_count')

conn.commit()
conn.close()
