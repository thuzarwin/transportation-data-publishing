'''
Utilities for querying postgresql database

see http://initd.org/psycopg/docs/usage.html
and http://initd.org/psycopg/docs/sql.html
'''
import psycopg2
from psycopg2 import sql

def queryColVal(conn, table, col, val):
    '''
    fetch records from table where record has specified col value
    return array of records
    '''
    cursor = conn.cursor()
    query = sql.SQL("SELECT * FROM {} WHERE {} = %s").format(sql.Identifier(table), sql.Identifier(col))
    cursor.execute(query, (val,))
    return cursor.fetchall()

def insertByDict(conn, table, obj):
    cursor = conn.cursor()
    cols = obj.keys()
    vals = [obj[key] for key in cols]
    #  http://initd.org/psycopg/docs/sql.html
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(', ').join(map(sql.Placeholder, cols)))

    print(query.as_string(conn))
    cursor.execute(query, obj)
    conn.commit()
    return True

def queryMaxWhere(conn, table, max_col, where_col, where_val):
    cursor = conn.cursor()
    query = sql.SQL("SELECT MAX({}) FROM {} WHERE {} = %s").format(sql.Identifier(max_col), sql.Identifier(table), sql.Identifier(col))
    cursor.execute(query, (where_val,))
    return cursor.fetchall()