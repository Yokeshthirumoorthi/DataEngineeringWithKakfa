# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import json
from datetime import datetime

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
BreadCrumb_TableName = 'BreadCrumb'
Trip_TableName = 'Trip'
Datafile = "./downloads/2021-01-13.json"  # name of the data file to be loaded
CreateDB = True  # indicates whether the DB table should be (re)-created

def isValidTripId(tripId):
    return tripId != ''

def isValidLatitude(latitude):
    return latitude != ''

def isValidLongitude(tripId):
    return tripId != ''

def isValidTripData(row):
    valid = True

    if not isValidTripId(row['EVENT_NO_TRIP']):
        valid = False

    return valid

def isValidBreadCrumbData(row):
    valid = True

    if not isValidTripId(row['EVENT_NO_TRIP']):
        valid = False

    if not isValidLatitude(row['GPS_LATITUDE']):
        valid = False

    if not isValidLongitude(row['GPS_LONGITUDE']):
        valid = False

    return valid


def row2vals_trip(row):
    tripId = row['EVENT_NO_TRIP']
    routeId = 0 # Temp value
    vehicleId = row['VEHICLE_ID']
    serviceKey = 'Weekday' # Temp value
    direction =  'Out' # row['DIRECTION'] or 0.0
    
    ret = f"""
        {tripId},
        {routeId},
        {vehicleId},
        '{serviceKey}',
        '{direction}'
    """
    return ret

def row2vals_breadcrumb(row):
    tstamp = datetime.strptime(row['OPD_DATE'], '%d-%b-%y')
    latitude = row['GPS_LATITUDE']
    longitude = row['GPS_LONGITUDE']
    direction = 0 #row['DIRECTION']
    speed = 0 #row['VELOCITY']
    tripId = row['EVENT_NO_TRIP']
    
    ret = f"""
        '{tstamp}',
        {latitude},
        {longitude},
        {direction},
        {speed},
        {tripId}
    """
    return ret

# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	with open(fname, mode="r") as fil:
		dr = json.load(fil)

		rowlist = []
		for row in dr:
			rowlist.append(row)

	return rowlist

def getSQLcmnd(tableName, valstr):
    cmd = f"INSERT INTO {tableName} VALUES ({valstr});"
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
            DROP TABLE IF EXISTS {BreadCrumb_TableName};
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

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createBreadCrumbTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
        	CREATE TABLE {BreadCrumb_TableName} (
            tstamp timestamp,
            latitude float,
            longitude float,
            direction integer,
            speed float,
            trip_id integer
         	);
            ALTER TABLE {BreadCrumb_TableName} ADD FOREIGN KEY (trip_id) REFERENCES {Trip_TableName};
    	""")

		print(f"Created {BreadCrumb_TableName}")

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

def main():
    conn = dbconnect()
    rlis = readdata(Datafile)
    
    if CreateDB:
    	dropIfExists(conn)
    	createEnumTypes(conn)
    	createTripTable(conn)
    	createBreadCrumbTable(conn)

    start = time.perf_counter()

    for row in rlis[:500]:
        if isValidTripData(row) & isValidBreadCrumbData(row):
            cmd_trip = getSQLcmnd(Trip_TableName, row2vals_trip(row))
            cmd_breadcrumb = getSQLcmnd(BreadCrumb_TableName, row2vals_breadcrumb(row))
            load(conn, cmd_trip)
            load(conn, cmd_breadcrumb)

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')   

if __name__ == "__main__":
    main()



