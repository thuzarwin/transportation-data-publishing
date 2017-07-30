'''
Utilities for fetching and parsing data from GRIDSMART API
Mostly borrowed from gtipy
https://bitbucket.org/GRIDSMART/public-gtip
'''
import logging
import requests

logger = logging.getLogger(__name__)

def dashify_site_or_zone_id(id_):
    '''
    From gtipy
    GRIDSMART JSON serializer removes the dashes, this puts them back
    https://bitbucket.org/GRIDSMART/public-gtipy
    '''
    # undash first, then add dashes
    idu = id_.replace('-','')
    return '%s-%s-%s-%s-%s' % (idu[:8], idu[8:12], idu[12:16], idu[16:20], idu[20:])


def site_api_endpoint(ip, port):
    '''
    From gtipy
    Utility function to construct site API endpoint from IP address and optional port
    https://bitbucket.org/GRIDSMART/public-gtipy
    '''
    return 'http://' + ip + ':' + str(port) + '/api/'


def getSiteID(endpoint):
    '''
    From gtipy
    https://bitbucket.org/GRIDSMART/public-gtipy
    '''
    ep = endpoint + 'system/siteid.json'
    res = requests.get(ep, timeout=3)
    if res.status_code != 200:
        raise Exception('failed to get site id')
    id_ = res.json().encode('ascii')
    id_ = dashify_site_or_zone_id(id_)
    return id_


def getSiteName(endpoint):
    ep = endpoint + 'sitename.json'
    res = requests.get(ep, timeout=3)
    if res.status_code != 200:
        raise Exception('failed to get site name')
    name = res.json().encode('ascii')
    return name
    

def getSiteConfig(endpoint):
    ep = endpoint + 'site.json'
    res = requests.get(ep, timeout=2)
    if res.status_code != 200:
        raise Exception('failed to get site config')
    site_config = res.json()
    return site_config


def getZoneInfo(site_config):
    '''
    From gtipy
    Get all vehicle zones from site
    https://bitbucket.org/GRIDSMART/public-gtipy
    '''
    vehicle_zones = {}
    for cam in site_config['CameraDevices']:
        cam_model = cam.values()[0] # key is fisheye, [0] value is dict
        masks = cam_model['CameraMasks']
        zones = masks['ZoneMasks']
        vzones = filter( (lambda z: z.keys()[0]=='Vehicle'), zones)
        vzones = map( (lambda z: z.values()[0]), vzones) # key is vehicle, [0] value is dict
        for vz in vzones:
            # must dashify b/c GRIDSMART JSON serializer removes dashes from GUIDs
            vz_id = dashify_site_or_zone_id(vz['Id'])
            vz['Id'] = vz_id
            vehicle_zones[vz_id] = vz

    return vehicle_zones
