'''
Verify that GRIDSMART site and zone data is current in database.
Update as needed.
'''
import csv
import json
import os
import sys
import socket
import logging
import traceback
import pytz
import pdb
import requests
import psycopg2
import secrets
from gridsmart_api import Gridsmart
from pg_helpers import *


def getJSON(path):
    try:
        with open(path, 'r') as fin:
            return  json.loads(fin.read())
    except:
        print()    


def validateIP(gridsmart, ip_field = 'DETECTOR_IP'):
    '''
    Validate IP exists and is valid format
    '''
    if not ip_field in gridsmart.keys():
        raise ValueError('Device missing IP address')

    ip = gridsmart['DETECTOR_IP']

    try:
        socket.inet_aton(ip)    
    except socket.error:
        raise ValueError('Device has invalid IP address')
 
    return ip


#  connect to data warehouse
dbname = secrets.DWH['dbname']
user = secrets.DWH['user']
password = secrets.DWH['password']
host = secrets.DWH['host']
port = 5432
conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)

#  fieldmapping gridsmart API >> data warehouse
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

# Get detectors JSON from network share and filter for GRIDSMART.
#  JSON is updatedhourly by data publishing job 
detectors_path = os.path.join(secrets.FME_DIRECTORY, 'detectors.json')
detector_data = getJSON(detectors_path)
gridsmarts = [row for row in detector_data if row['DETECTOR_TYPE'] == 'GRIDSMART']

for row in gridsmarts:
    #  get site and zone data for all GRIDSMART devices
    try:
        #  extract IP and validate
        ip = validateIP(row)
    except ValueError as e:
        print(e)
        continue
    
    try:
        #  create site instance and get data
        gs = Gridsmart(ip, get_zones=True, fieldmap_zones=fieldmap_zones, timeout=5)

    except requests.exceptions.ConnectionError as e:
        print(e)
        continue

    #  query data warehouse for site id + insert if not found
    if not queryColVal(conn, 'site', 'id', gs.id):
        res = insertByDict(conn, 'site', {'id' : gs.id, 'name' : gs.name } )
    else:
        print('site exists')

    #  query data warehouse for each zone id + insert if not found
    for zone in gs.zones.keys():
        if not queryColVal(conn, 'zone', 'id', gs.zones[zone]['id']):
            res = insertByDict(conn, 'zone', gs.zones[zone])
        else:
            print('zone exists')

conn.close()