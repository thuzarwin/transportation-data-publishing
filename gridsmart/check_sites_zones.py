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
from gridsmart_helpers import *
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


def appendSiteID(zones, site_id):
    for key in zones.keys():
        zones[key]['site_id'] = site_id
    return zones


def mapFields(zone, fieldmap):
    for key in zone.keys():
        if key in fieldmap:
            zone[fieldmap[key]] = zone.pop(key)
        else:
            #  drop fields not in fieldmap
            zone.pop(key)
    return zone


def removeNulls(obj):
    for key in obj.keys():
        if not obj[key]:
            obj.pop(key)

    return obj


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

sites = {}
zones = {}

sites_new = []
zones_new = []

for gridsmart in gridsmarts:
    #  get site and zone data for all GRIDSMART devices
    try:
        #  extract IP and validate
        ip = validateIP(gridsmart)
    except ValueError as e:
        print(e)
        continue

    #  generate site endpoint
    site_endpoint = site_api_endpoint(ip, 8902)
    
    try:
        #  get site attributes
        site_id = getSiteID(site_endpoint)
        site_name = getSiteName(site_endpoint)

    except requests.exceptions.ConnectionError as e:
        print(e)
        continue

    #  update site dict
    sites[site_id] = {'id' : site_id, 'name' : site_name}

    #  get complete site config
    site_config = getSiteConfig(site_endpoint)

    #  get and process zone data
    site_zones = getZoneInfo(site_config)
    
    #  and a site_id to zone information
    site_zones = appendSiteID(site_zones, site_id)
    
    #  replace api fieldnames with db fieldnames
    for zone_id in site_zones.keys():
        site_zones[zone_id] = mapFields(site_zones[zone_id], fieldmap_zones)
        site_zones[zone_id] = removeNulls(site_zones[zone_id])

    #  update master zones dict 

    zones.update(site_zones)

#  verify each zone exists in database
#  insert new zone and site as needed

dbname = secrets.DWH['dbname']
user = secrets.DWH['user']
password = secrets.DWH['password']
host = secrets.DWH['host']
port = 5432

conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)

for site_id in sites.keys():
    if not queryColVal(conn, 'site', 'id', site_id):
        sites_new.append( sites[site_id] )

for zone_id in zones.keys():
    #  encode unicode zone id
    if not queryColVal(conn, 'zone', 'id', zone_id):
        zones_new.append( zones[zone_id] )

for site in sites_new:
    res = insertByDict(conn, 'site', site)
    
for zone in zones_new:
    res = insertByDict(conn, 'zone', zone)

conn.close()