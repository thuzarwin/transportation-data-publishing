cfg = {
    'metadata' : '../metdata/fulcrum.json'
}


CFG_KNACK_FULCRUM = {
    'data_tracker_test_fulcrum' : {
        'work_orders_public' : {  
            'scene' : 'scene_514',
            'view' : 'view_1785',
        },
        'work_orders_private' : {  
            'obj' : 'object_31',
            'ref_obj' : ['object_31'],
            'scene' : 'scene_665',
            'view' : 'view_1786',
        }
    }
} 


KNACK_FULC_FIELDMAP = [
    {
        'name_knack' : 'id',
        'name_fulcrum' : 'knack_id',
        'detect_changes' : False,
        'form_name_fulcrum' : 'Work Orders',
        'table_fulcrum' : 'form',
        'type_fulcrum' : str,
        'type_knack' : str,
    },
    {
        'name_knack' : 'fuclrum_id',
        'name_fulcrum' : '_record_id',
        'detect_changes' : False,
        'table_fulcrum' : 'form',
        'type_fulcrum' : str,
        'type_knack' : str,
    },
    {
        'name_knack' : 'TECHNICIAN_LEAD',
        'name_fulcrum' : '_assigned_to_id',
        'detect_changes' : True,
        'table_fulcrum' : '',
        'type_fulcrum' : str,
        'type_knack' : str,
    },
    {
        'name_knack' : 'work_order_status',
        'name_fulcrum' : '_status',
        'detect_changes' : True,
        'table_fulcrum' : '',
        'type_fulcrum' : str,
        'type_knack' : str,
    },
    {
        'name_knack' : 'CREATED_DATE',
        'name_fulcrum' : '_created_at',
        'detect_changes' : True,
        'table_fulcrum' : '',
        'type_fulcrum' : 'date_iso',
        'type_knack' : 'date_unix',
    },
    {
        'name_knack' : 'MODIFIED_DATE',
        'name_fulcrum' : '_updated_at',
        'detect_changes' : True,
        'table_fulcrum' : '',
        'type_fulcrum' : 'date_iso',
        'type_knack' : 'date_unix',
    },
    {
        'name_knack' : 'WORK_TYPE',
        'name_fulcrum' : 'work_type',
        'detect_changes' : True,
        'table_fulcrum' : 'this might change', #todo
    },
    {
        'name_knack' : 'ASSET_TYPE',
        'name_fulcrum' : 'asset_type',
        'detect_changes' : True,
        'table_fulcrum' : '',
        'type_fulcrum' : str,
        'type_knack' : str,
    },
    {
        'name_knack' : 'signal',
        'name_fulcrum' : 'signal',
        'detect_changes' : True,
        'table_fulcrum' : '',
    },
    {
        'name_knack' : 'PROBLEM_FOUND',
        'name_fulcrum' : 'problem_found',
        'detect_changes' : True,
        'table_fulcrum' : '',
    },
    {
        'name_knack' : 'ACTION_TAKEN',
        'name_fulcrum' : 'action_taken',
        'detect_changes' : True,
        'table_fulcrum' : '',
    },
    {
        'name_knack' : 'WORK_ORDER_ID',
        'name_fulcrum' : 'work_order_id',
        'detect_changes' : True,
        'table_fulcrum' : '',
    },
]










