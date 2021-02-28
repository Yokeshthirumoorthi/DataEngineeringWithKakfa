# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import json
from datetime import datetime
import data_gatherer

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
Trip_TableName = 'Trip'
# Datafile = "./downloads/2021-01-13.json"  # name of the data file to be loaded
DataUrl = "http://34.83.136.192:8000/getStopEvents/"
CreateDB = False  # indicates whether the DB table should be (re)-created


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--dataurl", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  DataUrl = args.dataurl
  global CreateDB
  CreateDB = args.createtable

def row2vals_trip(row):
    tripId = row[0]
    routeId = row[2]
    vehicleId = row[1]
    serviceKey = row[4]
    direction =  row[3]
    
    ret = f"""
        {tripId},
        {routeId},
        {vehicleId},
        '{serviceKey}',
        '{direction}'
    """
    return ret

def getSQLcmnd(tableName, valstr):
    cmd = f"INSERT INTO {tableName} VALUES ({valstr});"
    return cmd


def getUpdateSQLcmnd(tableName, row):
    tripId = row[0]
    routeId = row[2]
    vehicleId = row[1]
    serviceKey = row[4]
    direction =  row[3]
    
    cmd = f"UPDATE {tableName} SET route_id = {routeId}, service_key = '{serviceKey}', direction = '{direction}' WHERE trip_id = {tripId};"
    return cmd    

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection

def dropIfExists(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {Trip_TableName};
            drop type if exists service_type;
            drop type if exists tripdir_type;
        """)

        print(f"Created Enum Types")

def createEnumTypes(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
            create type tripdir_type as enum ('Out', 'Back');
        """)

        print(f"Created Enum Types")

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTripTable(conn):
	with conn.cursor() as cursor:
		cursor.execute(f"""
        	CREATE TABLE {Trip_TableName} (
            trip_id integer,
            route_id integer,
            vehicle_id integer,
            service_key service_type,
            direction tripdir_type
         	);
            ALTER TABLE {Trip_TableName} ADD PRIMARY KEY (trip_id);
    	""")

		print(f"Created {Trip_TableName}")

def load(conn, cmd):
    with conn.cursor() as cursor:
        # print (cmd)
        # cursor.execute(cmd)
        try:
            cursor.execute(cmd)
        except psycopg2.IntegrityError:
            conn.rollback()
        else:
            conn.commit()

def loadToDB(conn, row):
    if isValidData(row):
        cmd_trip = getSQLcmnd(Trip_TableName, row2vals_trip(row))
        cmd_breadcrumb = getSQLcmnd(BreadCrumb_TableName, row2vals_breadcrumb(row))
        load(conn, cmd_trip)
        load(conn, cmd_breadcrumb)


def main():
    initialize()
    conn = dbconnect()
    rlis = data_gatherer.getStops(DataUrl)
    
    if CreateDB:
    	dropIfExists(conn)
    	createEnumTypes(conn)
    	createTripTable(conn)

    start = time.perf_counter()

    loaded = 0
    print(f"Loading {len(rlis)} rows")
    for row in rlis:
        # cmd_trip = getSQLcmnd(Trip_TableName, row2vals_trip(row))
        cmd_trip = getUpdateSQLcmnd(Trip_TableName, row)
        load(conn, cmd_trip)
        loaded = loaded + 1

    print(f"Loaded {loaded} rows")
    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')   

if __name__ == "__main__":
    main()



