'''
fulcrum / knack integration engine


todo:

Design should be such that two or one way sync is possible. 
e.g. do not need to create all PMs in fulcrum. maybe we just...do! or set all existing knack pms with 'syncd' status. in any case, jobs would be run as: fulc.py -knack_app=x, -fulc_app=y, -task=z

- overhall knack pm workflow so that there is a submitted status and records are created from fulcrum with status of submitted, not approved
- add app names and table/object names to field map
- add fields all PM to knack PM object
- add fulcrum id field to knack PM object and work orders
- add field to fulcrums to indicate if sent to knack and to lock editing/set status
- left off mapping fields for insert
- you want to design the field map in a way that in can be passed with a 'from/to' param
- and handle either 'direction'
- assigned to id lookup/mapping
- logging

# results_knack = {'update' : 0, 'insert' : 0, 'errors' : 0
'''

import argparse
import pdb

from fulcrum import Fulcrum
import knackpy

import _setpath
from config.fulcrum.config import CFG_KNACK_FULCRUM
from config.fulcrum.config import KNACK_FULC_FIELDMAP

from config.secrets import *
from util import fulcutil


def cli_args():
    parser = argparse.ArgumentParser(
        prog='fulcrum/knack data sync',
        description='Synchronize data between Knack application and Fulcrum application'
    )

    parser.add_argument(
        'knack',
        type=str,
        choices=['data_tracker_prod', 'data_tracker_test_fulcrum'],
        help="Name of the Knack application that will be accessed."
    )

    parser.add_argument(
        'fulcrum',
        type=str,
        choices=['work orders'],
        help='Name of the fulcrum app that will be accessed.'
    )

    args = parser.parse_args()
    
    return(args)


def get_records_knack(app_name, config, endpoint_type='private'):
    api_key = KNACK_CREDENTIALS[app_knack]['api_key']
    app_id = KNACK_CREDENTIALS[app_knack]['app_id']

    if endpoint_type == 'public':
        return knackpy.Knack(
          scene=config['scene'],
          view=config['view'],
          app_id=app_id,
        )

    else:
        return knackpy.Knack(
          scene=config['scene'],
          view=config['view'],
          ref_obj=config['ref_obj'],
          api_key=api_key,
          app_id=app_id,
        )


def map_fields(record, field_map, *, source, dest):
    record_mapped = {}
    
    for field in record:
        for entry in field_map:
            try:
                if entry[f'name_{source}'] == field:
                    record_mapped[entry[f'name_{dest}']] = record[field]
                    continue

            except KeyError:
                continue

    pdb.set_trace()
    return record_mapped


def get_fulcrum_id(knack_record, api_key, table):
    '''
    Lookup record in Fulcrum and append record ID to input record dict
    '''
    query = fulcutil.get_query_by_value('knack_id', knack_record['id'], table)
    res = fulcutil.query(api_key, query)
    
    if len(res['rows']) == 1:
        knack_record['fulcrum_id'] = res['rows'][0]['_record_id']
        return knack_record

    elif len(res['rows']) == 0:
        knack_record['fulcrum_id'] = None
        return knack_record

    else:
        raise Exception('Multiple records found with same ID!')



def update_fulcrum(*, record, task, api_key, form_id):
    record = map_fields(
        record,
        field_map=KNACK_FULC_FIELDMAP,
        source='knack',
        dest='fulcrum'
    )

    payload = fulcutil.get_template()
    fulcrum = Fulcrum(key=api_key)
    
    if task == 'create':
        res = fulcrum.records.create(payload)
        
    
    elif task == 'update':
        res = fulcrum.records.update(form_id, payload)
    
    else:
        raise Exception(f'Unknown task: {task}')
    
    return res



def main(app_knack, app_fulcrum):
    #  get configuration for Knack and Fulcrum
    api_key_fulcrum = FULCRUM['api_key']
    cfg_knack_public = CFG_KNACK_FULCRUM[app_knack]['work_orders_public']
    cfg_knack_private = CFG_KNACK_FULCRUM[app_knack]['work_orders_private']
    meta_fulc = fulcutil.load_metadata()
    form_id_fulcrum = fulcutil.get_form_id(app_fulcrum, meta_fulc)
    
    #  check public knack endpoint for records
    kn_public = get_records_knack(app_knack, cfg_knack_public, endpoint_type='public')
    
    if not kn_public.data:
        #  no knack records to update in fulcrum
        return None

    #  get fulcrum user records
    users_fulcrum = fulcutil.get_users(api_key_fulcrum)

    #  get complete records data from private endpoint
    kn = get_records_knack(app_knack, cfg_knack_private, endpoint_type='private')

    for record in kn.data:
        
        record = get_fulcrum_id(
            record,
            api_key_fulcrum,
            'Work Orders'
        )
        
        if record['fulcrum_id']:
            continue

        else:
            task = 'create'
            res = update_fulcrum(
                record=record,
                task=task,
                api_key=api_key_fulcrum,
                form_id=form_id_fulcrum
            )

        print(task)
        pdb.set_trace()
        

        if res.status_code == 200:
            res = update_knack(kn_record)
            results[task] += 1

        else:
            print("SHIT!")
            results[task] += 1  


if __name__ == '__main__':

    args = cli_args()
    app_knack = args.knack
    app_fulcrum = args.fulcrum
    
    main(app_knack, app_fulcrum)
   
















