import logging
import traceback
import pdb
import psycopg2
from psycopg2.extras import execute_values
import arrow
from pg_helpers import *
from gridsmart import *
import secrets


#  get max count recorded from gridsmart with something like this:
queryMaxWhere(conn, 'traffic_count', 'count_date_field_name', 'site_id', site_id):
