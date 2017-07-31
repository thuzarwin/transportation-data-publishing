import requests
import pdb

#  todo
#  logging

class Gridsmart:
    '''
    Class to interact with a GRIDSMART site via the API as
    documented at http://userguide.gridsmartcloud.com/api.html
    
    Mostly borrowed from gtipy
    https://bitbucket.org/GRIDSMART/public-gti

    Parameters
    ----------
    ip : string (required)
        IP address of gridsmart camerasite
    port : int or string (default: 8902)
        Port to reach site endpoint
    timeout : numeric (default: 10)
        number of seconds before http request timeout
    get_zones : bool (default: False)
        extract zone information from site config and append as dict to self.zones
    fieldmap_zones : dict (default: None)
        when provided, keys in zone data will be replaced according to field map
        see method for additional details
    '''
    def __init__(self, ip, port=8902, timeout=10, get_zones=False, fieldmap_zones=None):
        self.ip = ip
        self.port = port
        self.endpoint = 'http://' + ip + ':' + str(port) + '/api/'
        self.timeout = float(timeout)

        self.id = None
        self.getId(self.endpoint, self.timeout)

        self.name = None
        self.getName(self.endpoint, self.timeout)

        self.config = None
        self.getConfig(self.endpoint, self.timeout)

        self.zones = None

        if get_zones:
            self.getZones(self.config)

            self.fieldmap_zones = fieldmap_zones
            
            if fieldmap_zones:
                for zone in self.zones:
                    self.zones[zone] = self.mapFields(self.zones[zone], self.fieldmap_zones)

    def getId(self, endpoint, timeout):
        '''
        Copied from gtipy
        https://bitbucket.org/GRIDSMART/public-gtipy

        Get the site GRIDSMART site ID

        Parameters
        ----------
        endpoint : string
            the API endpoint (URL) at which to retrieve the site id
        timeout : numeric (default: 10)
            number of seconds before http request timeout

        Returns
        -------
        value : string
            The properly-dashified site ID
        '''
        ep = endpoint + 'system/siteid.json'
        res = requests.get(ep, timeout=timeout)
        
        if res.status_code != 200:
            raise Exception('failed to get site id')
        
        site_id = res.json().encode('ascii')
        
        self.id = dashify_site_or_zone_id(site_id)
        return self.id

    def getName(self, endpoint, timeout):
        '''
        Copied from gtipy
        https://bitbucket.org/GRIDSMART/public-gtipy

        Get the site GRIDSMART site name

        Parameters
        ----------
        endpoint : string
            the API endpoint (URL) at which to retrieve the site id
        timeout : numeric (default: 10)
            number of seconds before http request timeout

        Returns
        -------
        value : string
            The site name
        '''
        ep = endpoint + 'sitename.json'
        res = requests.get(ep, timeout=timeout)
        if res.status_code != 200:
            raise Exception('failed to get site name')
        
        self.name = res.json().encode('ascii')
        return self.name

    def getConfig(self, endpoint, timeout):
        '''
        Copied from gtipy
        https://bitbucket.org/GRIDSMART/public-gtipy
        
        Get the site GRIDSMART site configuration

        Parameters
        ----------
        endpoint : string
            the API endpoint (URL) at which to retrieve the site id
        timeout : numeric (default: 10)
            number of seconds before http request timeout

        Returns
        -------
        config : dict
            Dictionary created from site config JSON
        '''
        ep = endpoint + 'site.json'
        res = requests.get(ep, timeout=timeout)
        if res.status_code != 200:
            raise Exception('failed to get site config')
        self.config = res.json()
        return self.config      

    def getZones(self, config):
        '''
        Copied from gtipy
        https://bitbucket.org/GRIDSMART/public-gtipy

        Get all the vehicle zones for the site

        Parameters
        ----------
        config : dict (required)
            a GRIDSMART site configuration dictionary (see getConfig method)

        Returns
        -------
        zones : dict
            Dictionary of <zone_id, zone_data>
        '''
        vehicle_zones = {}
        
        site_id = dashify_site_or_zone_id(config['Id'])
        config['Id'] = site_id

        for cam in config['CameraDevices']:
            cam_model = cam.values()[0] # key is fisheye, [0] value is dict
            masks = cam_model['CameraMasks']
            zones = masks['ZoneMasks']
            vzones = filter( (lambda z: z.keys()[0]=='Vehicle'), zones)
            vzones = map( (lambda z: z.values()[0]), vzones) # key is vehicle, [0] value is dict
            for vz in vzones:
                # must dashify b/c GRIDSMART JSON serializer removes dashes from GUIDs
                vz_id = dashify_site_or_zone_id(vz['Id'])
                vz['Id'] = vz_id
                vz['site_id'] = site_id  #  append site_id to zone object...to make life easier
                vehicle_zones[vz_id] = vz
        self.zones = vehicle_zones
        return self.zones

    def mapFields(self, obj, fieldmap):
        '''
        Lookup and replace keys in a dictioanry from an input fieldmap.

        **Keys not found in the fieldmap are dropped**
        **Keys with empty values are dropped**

        Parameters
        ----------
        obj : dict (required)
            a dict whose (top-level) keys are to be replaced
        fieldmap : dict (required)
            a dict of format { input_key : output_key } which will be used to replace the input dict's fields

        Returns
        -------
        obj : dict
            Dictionary with keys replaced according to fieldmap
        '''
        for key in obj.keys():
            
            #  check for and remove empty key/vals
            if not obj[key]:
                obj.pop(key)
                continue

            # lookup and replace key
            if key in fieldmap:
                obj[fieldmap[key]] = obj.pop(key)
            else:
                #  drop fields not in fieldmap
                obj.pop(key)

        return obj

#  helper methods
def dashify_site_or_zone_id(id_):
    '''
    Copied from gtipy
    https://bitbucket.org/GRIDSMART/public-gtipy
    
    GRIDSMART JSON serializer removes the dashes, this puts them back
    '''
    # undash first, then add dashes
    idu = id_.replace('-','')
    return '%s-%s-%s-%s-%s' % (idu[:8], idu[8:12], idu[12:16], idu[16:20], idu[20:])