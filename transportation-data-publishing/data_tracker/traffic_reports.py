'''
Parse COA traffic report feed and upload to Knack database.
(A different script publishes the information to the COA pubic data portal).

This feed is generated by some CTM black magic that
extracts incident data from the public safety CAD databse and publishes
it in the form of an RSS feed and cold fusion-powered HTML table.
See: http://www.ci.austin.tx.us/qact/qact_rss.cfm
'''
import os
import pdb
import traceback

import arrow
import feedparser
import knackpy

import _setpath
from config.secrets import *
from config.traffic_report.meta import *
from util import agolutil
from util import datautil
from util import emailutil
from util import logutil

#  init logging 
script = os.path.basename(__file__).replace('.py', '')
logfile = f'{LOG_DIRECTORY}/{script}.log'
logger = logutil.timed_rotating_log(logfile)

now = arrow.now()
logger.info('START AT {}'.format(str(now)))

#  config
feed_url = 'http://www.ci.austin.tx.us/qact/qact_rss.cfm'
primary_key = 'TRAFFIC_REPORT_ID'
status_key = 'TRAFFIC_REPORT_STATUS'
date_field = 'PUBLISHED_DATE'
status_date_field = 'TRAFFIC_REPORT_STATUS_DATE_TIME'
knack_obj = 'object_121'
knack_scene = 'scene_514'
knack_view = 'view_1624'
id_field_raw = 'field_1826'
knack_status_field = 'field_1843'

knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
agol_creds = AGOL_CREDENTIALS


def get_filter(field_id, field_val):

    return [
        {
           'field' : field_id,
           'operator' : 'is',
           'value' : field_val
        }
    ]


def get_records(
    view,
    scene,
    creds,
    filters=None,
    rows_per_page=1000,
    page_limit=100):

    return knackpy.Knack(
        view=knack_view,
        scene=knack_scene,
        app_id=creds['app_id'],
        filters=filters,
        rows_per_page=rows_per_page,
        page_limit=page_limit
    )


def parseFeed(feed):
    '''
    extract feed data by applying some unfortunate hardcoded parsing
    logic to feed entries
    '''
    records = []
    for entry in feed.entries:
        record = handleRecord(entry)
        records.append(record)
    return records


def getTimestamp(datestr):
    #  returns a naive millsecond timestamp in local time
    #  which is good because Knack needs a local timestamp
    return arrow.get(datestr).timestamp * 1000


def localTimestamp():
    #  create a "local" timestamp, ie unix timestamp shift for local time
    #  because Knack assumes time values in local (Central) time
    return arrow.now().replace(tzinfo='UTC').timestamp * 1000


def has_match(dicts, val, key):
    #  find the first dict in a list of dicts that matches a key/value
    for record in dicts:
        if record[key] == val:
            return True
    return False

                
def parseTitle(title):
    #  parse a the feed "title" element
    #  assume feed will never have Euro sign (it is non-ascii)
    try:
        title = title.replace('    ', '€')
        #  remove remaining whitespace clumps like so: 
        title = " ".join(title.split())
        #  split into list on
        title = title.split('€')
        #  remove empty strings reducing to twoelements
        #  first is address, second is issue type, with leading dash (-)
        title = list(filter(None, title))

        issue = title[1].replace('-', '').strip()
        address = title[0].replace('/', '&')
    except IndexError as e:
        logger.info(title)
        raise e
        
    return address, issue


def parseSummary(summary):
    #  feed summary is pipe-delimitted and gnarly
    summary = summary.split('|')
    summary = [thing.strip() for thing in summary]
    #  return lat/lon elements from summary array in format [x, y]
    return summary[1:3]


def handleRecord(entry):
    #  turn rss feed entry into traffic report dict
    record = {}
    timestamp = getTimestamp(entry.published_parsed)
    status_date = localTimestamp()
    #  compose record id from entry identifier (which is not wholly unique)
    #  and publicatin timestamp
    record_id = '{}_{}'.format( str(entry.id), str(timestamp) )
    record[primary_key] = record_id
    record[date_field] = timestamp
    record[status_date_field] = status_date
    title = entry.title
    #  parse title
    title = parseTitle(title)
    record['ADDRESS'] = title[0]
    record['ISSUE_REPORTED'] = title[1]
    #  parse lat/lon
    geocode = parseSummary(entry.summary)
    record['LATITUDE'] =  geocode[0]
    record['LONGITUDE'] =  geocode[1]
    record['LOCATION'] = { 'latitude' : geocode[0], 'longitude' : geocode[1] }
    return record


def main(date_time):
    try:
        #  get "ACTIVE" traffic report filter
        filter_active = get_filter(knack_status_field, 'ACTIVE')

        #  get existing traffic report records from Knack
        kn = get_records(
            knack_view,
            knack_scene,
            knack_creds,
            filters=filter_active
        )

        if not kn.data:
            #  if there are no "old" active records
            #  we need some data to get schema
            #  so fetch one record from source table
            kn = get_records(
                knack_view,
                knack_scene,
                knack_creds,
                page_limit=1,
                rows_per_page=1
            )

            #  we don't want the record, just the schema, so clear kn.data
            kn.data = []

        #  manually insert field metadata into Knackpy object
        #  because field metadata is not avaialble in public views
        #  and we use a public view to reduce the # of private API calls
        #  of which we have a daily limit
        kn.fields = TRAFFIC_REPORT_META
        kn.make_field_map()

        #  get and parse feed
        feed = feedparser.parse(feed_url)
        new_records = parseFeed(feed)
       
        #  convert feed fieldnames to Knack database names
        #  prepping them for upsert 
        new_records = datautil.replace_keys(new_records, kn.field_map)
        primary_key_raw = kn.field_map[primary_key]
        status_key_raw = kn.field_map[status_key]
        date_field_raw = kn.field_map[date_field]
        status_date_field_raw = kn.field_map[status_date_field]

        records_create = []
        records_update = []

        for new_rec in new_records:
            #  lookup current records in knack database to verify if they already exist
            #  we do this because sometimes the feed request returns no results
            #  only to return already-existing incidents on the next request
            record_id = new_rec.get(id_field_raw)
            filter_existing = get_filter(id_field_raw, record_id)
            
            match_records = get_records(
                knack_view,
                knack_scene,
                knack_creds,
                filters=filter_existing
            )
        
            if match_records.data:
                if match_records.data[0][knack_status_field] == 'ACTIVE':
                    logger.info( 'NO CHANGE: {}'.format(new_rec[primary_key_raw]) )
                    continue

                else:
                    #  record exists but has been archived
                    new_rec['id'] = match_records.data[0]['id']
                    new_rec[status_key_raw] = 'ACTIVE'
                    records_update.append(new_rec)

        #  look for old records in new data
        for old_rec in kn.data:
            if not has_match(
                new_records,
                old_rec[primary_key_raw],
                primary_key_raw
            ):
                old_rec[status_key_raw] = 'ARCHIVED'
                old_rec[status_date_field_raw] = localTimestamp()
                records_update.append(old_rec)

        records_update = datautil.reduce_to_keys(records_update, ['id', status_key_raw, status_date_field_raw])

        count = 0

        logger.info('CREATE: {}'.format([rec['field_1826'] for rec in records_create]))
        
        for record in records_update:
            count += 1
            print( 'Updating record {} of {}'.format( count, len(records_update) ) )
            res = knackpy.record(
                record,
                obj_key=knack_obj,
                app_id= knack_creds['app_id'],
                api_key=knack_creds['api_key'],
                method='update',
            )

        count = 0
        
        for record in records_create:
            count += 1
            print( 'Inserting record {} of {}'.format( count, len(records_create) ) )

            try:
                res = knackpy.record(
                    record,
                    obj_key=knack_obj,
                    app_id= knack_creds['app_id'],
                    api_key=knack_creds['api_key'],
                    method='create',
                )
            except Exception as e:
                if 'unique' in e:
                    logger.info('Duplicate insert: {}'.format(record))

        return 'Done.'

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        error_text = traceback.format_exc()
        emailutil.send_email(ALERTS_DISTRIBUTION, 'Traffic Report Process Failure', str(error_text), EMAIL['user'], EMAIL['password'])
        raise e


results = main(now)
logger.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)
