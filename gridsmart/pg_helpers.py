'''
Utilities for querying postgresql database

see http://initd.org/psycopg/docs/usage.html
and http://initd.org/psycopg/docs/sql.html
'''
import pdb
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import AsIs

def queryColVal(conn, table, col, val):
    '''
    fetch records from table where record has specified col value
    return array of records
    '''
    cursor = conn.cursor()
    query = sql.SQL("select * from {} where {} = %s").format(sql.Identifier(table), sql.Identifier(col))
    cursor.execute(query, (val,))
    return cursor.fetchall()

def insertByDict(conn, table, obj):
    cursor = conn.cursor()
    cols = obj.keys()
    vals = [obj[key] for key in cols]
    #  http://initd.org/psycopg/docs/sql.html
    query = sql.SQL("insert into {} ({}) values ({})").format(
        sql.Identifier(table),
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(', ').join(map(sql.Placeholder, cols)))

    print(query.as_string(conn))
    cursor.execute(query, obj)
    conn.commit()
    return True

    