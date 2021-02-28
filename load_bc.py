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
CreateDB = False  # indicates whether the DB table should be (re)-created


def initialize():

  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

# Existence Check: TripId and Vehicle should not be null
def isTripIdAndVehicleIdAvailable(row):
    tripId = row['EVENT_NO_TRIP']
    vehicleId = row['VEHICLE_ID']
    return (tripId != '') & (vehicleId != '')

# Limit Check: TripId and VehicleId should be positive integers. And Vehicle Id should be 4 digits
def isTripIdAndVehicleIdValid(row):
    tripId = row['EVENT_NO_TRIP']
    vehicleId = row['VEHICLE_ID']
    return (int(tripId) > 0) & (int(vehicleId) > 0) & (len(vehicleId) == 4)

# Limit Check: Velocity should be less than 5000
def isVelocityValid(row):
    velocity = row['VELOCITY'] or 0
    return (int(velocity) >= 0) & (int(velocity) <5000)

# Existence Check: Date and Actual Time should not be null
def isDateAndTimeAvailable(row):
    date = row['OPD_DATE']
    actualTime = row['ACT_TIME']
    return (date != '') & (actualTime != '')

# Existence Check: Lat Long should not be null
def isLatitudeAndLongitudeAvailable(row):
    latitude = row['GPS_LATITUDE']
    longitude = row['GPS_LONGITUDE']
    return (latitude != '') & (longitude != '')

# Limit Check: Latitude and longitude should not be 0.0. ANd Lat < 0 and Long > 0
def isLatitudeAndLongitudeValid(row):
    latitude = row['GPS_LATITUDE'] or 0.0
    longitude = row['GPS_LONGITUDE'] or 0.0
    return (float(latitude) > 0.0) & (float(longitude) < 0.0)

def isValidData(row):
    valid = True

    if not isTripIdAndVehicleIdAvailable(row):
        valid = False

    if not isTripIdAndVehicleIdValid(row):
        valid = False

    if not isVelocityValid(row):
        valid = False

    if not isDateAndTimeAvailable(row):
        valid = False

    if not isLatitudeAndLongitudeAvailable(row):
        valid = False

    if not isLatitudeAndLongitudeValid(row):
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
    ttime = time.strftime('%H:%M:%S', time.gmtime(int(row['ACT_TIME'])))
    tstamp = datetime.strptime(row['OPD_DATE'] + ' ' + ttime, '%d-%b-%y %H:%M:%S')
    latitude = row['GPS_LATITUDE']
    longitude = row['GPS_LONGITUDE']
    direction = row['DIRECTION'] or 0
    speed = row['VELOCITY'] or 0
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

def loadToDB(conn, row):
    if isValidData(row):
        cmd_trip = getSQLcmnd(Trip_TableName, row2vals_trip(row))
        cmd_breadcrumb = getSQLcmnd(BreadCrumb_TableName, row2vals_breadcrumb(row))
        load(conn, cmd_trip)
        load(conn, cmd_breadcrumb)


def main():
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)
    
    if CreateDB:
    	dropIfExists(conn)
    	createEnumTypes(conn)
    	createTripTable(conn)
    	createBreadCrumbTable(conn)

    start = time.perf_counter()

    loaded = 0
    print(f"Loading {len(rlis)} rows")
    for row in rlis:
        if isValidData(row):
            cmd_trip = getSQLcmnd(Trip_TableName, row2vals_trip(row))
            cmd_breadcrumb = getSQLcmnd(BreadCrumb_TableName, row2vals_breadcrumb(row))
            load(conn, cmd_trip)
            load(conn, cmd_breadcrumb)
            loaded = loaded + 1

    print(f"Loaded {loaded} rows")
    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')   

if __name__ == "__main__":
    main()



