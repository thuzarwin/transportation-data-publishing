#  do you need to conn.commit() after execute values?
import csv
import json
import os
import sys
import socket
import logging
import traceback
import pytz
import pdb
from gtipy import * # make sure the gtipy directory is in your python path
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import secrets

count_datetime_format = 'YYYYMMDDTHHmmss.S'
fieldmap_zones = {
    'Id' : 'id',
    'MaxRecall' : 'max_recall',
    'Alerts' : 'alerts',
    'DelaySeconds' : 'delay_seconds',
    'TurnType' : 'turn_type',
    'SpeedScale' : 'speed_scale',
    'VisibilityDetectionEnabled' : 'visibility_detection_enabled',
    'DelaySecondsPrecise' : 'delay_seconds_precise',
    'ProtectedPhases' : 'protected_phases',
    'Latching' : 'latching',
    'PermissivePhases' : 'permissive_phases',
    'ExtensionSeconds' : 'extension_seconds',
    'ApproachType' : 'approach_type',
    'ExpectedFreeFlowSpeed' : 'expected_freeflow_speed',
    'IncludeInData' : 'include_in_data',
    'Name' : 'name',
    'site_id' : 'site_id',
    'traffic_counts' : 'traffic_counts',
}


def getJSON(path):
    try:
        with open(path, 'r') as fin:
            return  json.loads(fin.read())
    except:
        print()

def getSiteData(detector):
    port = 8902

    if not 'DETECTOR_IP' in device.keys():
        raise ValueError('Device missing IP address')

    ip = device['DETECTOR_IP']

    try:
        socket.inet_aton(ip)    
    except socket.error:
        raise ValueError('Device has invalid IP address')
    try:
        # create SiteAPI object using the utility function
        site = site_at_ip_port(ip, port)

    except requests.exceptions.ConnectionError as e:
        raise(e)

    return site

def appendSiteID(zones, site_id):
    for key in zones.keys():
        zones[key]['site_id'] = site_id
    return zones

def removeNulls(obj):
    '''
        Turn list fields into csv lines
        To be inserted into text fields in database
    '''
    for key in obj.keys():
        if not obj[key]:
            obj.pop(key)

    return obj

def findRow(cursor, key, val):
    try:
        print('use mogrify')


def buildInsertTemplate(columns):
    col_str = ', '.join(str(c) for c in columns)

    template = '''
        INSERT INTO count ( {} )
        VALUES %s
    '''.format(col_str)

    return template

def replaceDate(row, datetime_format):
    time_string = row[0]
    dt = arrow.get(time_string, datetime_format)
    dt = d.replace('US/Central')
    row[0] = dt
    return tuple(row)

def processCountFile(conn, fname, page_size):
    cursor = conn.cursor()
    inserts = []
    count = 0

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
# check for and create sites/zones as needed
# get count files for 
# parses raw count, appends an 'upload date' and site code to raw counts
# inserts raw counts and add date parsing
# inserts aggregate counts


detector_path = os.path.join(secrets.FME_DIRECTORY, 'detectors.json')
detectors = getJSON(detector_path)
gridsmart = [row for row in detectors if row['DETECTOR_TYPE'] == 'GRIDSMART']

zones_all = {}

for device in gridsmart:
    #  get site data
    try:
        site = getSiteData(device)
    except ValueError as e:
        print(e)
        continue
    except requests.exceptions.ConnectionError as e:
        print(e)
        continue

    #  get and process zone data
    site_id = site.id
    zones = site.get_vehicle_zones()
    zones = appendSiteID(zones, site_id)
    zones_all.update(zones)
    
    print('get zone data')

zones_f = []

for zone in zones_all.keys():
    new = {}
    for key in zones_all[zone].keys():
        if key in fieldmap_zones:
            new[fieldmap_zones[key]] = zones_all[zone][key]

    zones_f.append(new)

new = []
for row in zones_f:
    row = removeNulls(row)
    new.append(row)

pdb.set_trace()

bob = open('zones.json', 'w')
json.dump(new, bob)
bob.close()

# fname = '2016-10-03.csv'
# index = 0
# page_size = 3000
# cols = ['timestamp', 'approach', 'turn', 'length_ft', 'speed_mph', 'phase', 'light', 'seconds_of_light_state', 'seconds_since_green', 'recent_free_flow_speed_mph', 'calibration_free_flow_speed_mph', 'include_in_approach_data', 'zone_id']

# dbname = secrets.DWH['dbname']
# user = secrets.DWH['user']
# password = secrets.DWH['password']
# host = secrets.DWH['host']
# port = 5432

# conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
# template = buildInsertTemplate(cols)
# results = processCountFile(conn, fname, page_size)

# conn.close()

