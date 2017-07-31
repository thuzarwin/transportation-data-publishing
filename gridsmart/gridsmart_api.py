'''
Utilities for fetching and parsing data from GRIDSMART API
Mostly borrowed from gtipy
https://bitbucket.org/GRIDSMART/public-gtip
'''
import requests

class Gridsmart(object):

    def __init__(self, ip, port=8902, timeout=10):
        self.ip = ip
        self.port = port
        self.endpoint = 'http://' + ip + ':' + str(port) + '/api/'
        self.timeout = float(timeout)
        self.id = None
        #  self.siteID(enpoint, timeout)

        self.name = None
        #  self.siteName(endpoint, timeout)

    def siteID(self, endpoint, timeout):
        ep = endpoint + 'system/siteid.json'
        res = requests.get(ep, timeout=timeout)
        
        if res.status_code != 200:
            raise Exception('failed to get site id')
            site_id = res.json().encode('ascii')
        
        self.id = dashify_site_or_zone_id(site_id)

        return self.id


    def siteName(self, endpoint, timeout):
        ep = endpoint + 'sitename.json'
        res = requests.get(ep, timeout=timeout)
        if res.status_code != 200:
            raise Exception('failed to get site name')
        
        self.name = res.json().encode('ascii')
        
        return self.name

            


#  helpers
def dashify_site_or_zone_id(id_):
    '''
    From gtipy
    GRIDSMART JSON serializer removes the dashes, this puts them back
    https://bitbucket.org/GRIDSMART/public-gtipy
    '''
    # undash first, then add dashes
    idu = id_.replace('-','')
    return '%s-%s-%s-%s-%s' % (idu[:8], idu[8:12], idu[12:16], idu[16:20], idu[20:])

    

# def getSiteConfig(endpoint):
#     ep = endpoint + 'site.json'
#     res = requests.get(ep, timeout=2)
#     if res.status_code != 200:
#         raise Exception('failed to get site config')
#     site_config = res.json()
#     return site_config


# def getZoneInfo(site_config):
#     '''
#     From gtipy
#     Get all vehicle zones from site
#     https://bitbucket.org/GRIDSMART/public-gtipy
#     '''
#     vehicle_zones = {}
#     for cam in site_config['CameraDevices']:
#         cam_model = cam.values()[0] # key is fisheye, [0] value is dict
#         masks = cam_model['CameraMasks']
#         zones = masks['ZoneMasks']
#         vzones = filter( (lambda z: z.keys()[0]=='Vehicle'), zones)
#         vzones = map( (lambda z: z.values()[0]), vzones) # key is vehicle, [0] value is dict
#         for vz in vzones:
#             # must dashify b/c GRIDSMART JSON serializer removes dashes from GUIDs
#             vz_id = dashify_site_or_zone_id(vz['Id'])
#             vz['Id'] = vz_id
#             vehicle_zones[vz_id] = vz

#     return vehicle_zones
