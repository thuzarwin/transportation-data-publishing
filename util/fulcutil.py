''' 
Helper methods to interact with a Fulcrum app.

See:
    http://developer.fulcrumapp.com/
    https://github.com/fulcrumapp/fulcrum-python
'''
import pdb
from fulcrum import Fulcrum
import json
import requests


def get_all_metadata(api_key):
    fulc = Fulcrum(api_key)
    forms = fulc.forms.search()
    return forms['forms']


def get_form_id(app_name, metadata):
    for form in metadata:
        if app_name.lower() == form['name'].lower():
            return form['id']
    
    return None


def load_metadata(path='../config/metadata/fulcrum.json'):
    with open(path, 'r') as fin:
        return json.loads(fin.read())


def get_users(api_key):
    fulcrum = Fulcrum(key=api_key)
    users = fulcrum.memberships.search() 
    return users['memberships']


def get_form_field_data(fulc, form_id):
    '''
    Return form field data from a Fulcrum instance
    '''
    form = fulc.forms.find(form_id)
    return form['form'][0]['elements']

# def get_user_by_email(api_key, form_id, email):
#     email = email.lower()
#     fulcrum = Fulcrum(key=api_key_fulcrum)
#     users = fulcrum.memberships.search(url_params={'form_id': form_id})

#     for user in users['memberships']:
#         if user['email'].lower() == email:
#             return user

#     return None


def get_query_by_value(field, value, table):
    # return f'SELECT * from "{table}" WHERE {field} LIKE "{value}"'
    return f'SELECT * from "{table}" WHERE {field} LIKE \'{value}\''


def query(api_key, query, format_='json', timeout=15):
    url = 'https://api.fulcrumapp.com/api/v2/query'
    params = {
        'token' : api_key,
        'q' : query,
        'format' : format_
    }
    res = requests.get(url, params=params, timeout=timeout)
    
    return res.json()


def get_template():
    ''' fulcrum record dict with required keys '''
    return  { 'record': {
            'form_id': None,
            'form_values': {},
            # 'id',    <<< update only
        }
    }


def format_record(record, template, form_id):
    template['record']['form_id'] = form_id
    return template
















