# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import json

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
BreadCrumb_TableName = 'BreadCrumb'
Trip_TableName = 'Trip'
Datafile = "./downloads/2021-01-13.json"  # name of the data file to be loaded
CreateDB = True  # indicates whether the DB table should be (re)-created

def row2vals(row):
    eventNoTrip = row['EVENT_NO_TRIP']
    eventNoStop = row['EVENT_NO_STOP']
    opdDate = row['OPD_DATE']
    vehicleId = row['VEHICLE_ID']
    meters = row['METERS']
    actTime = row['ACT_TIME']
    velocity =  row['VELOCITY'] or 0.0
    direction =  row['DIRECTION'] or 0.0
    radioQuality =  row['RADIO_QUALITY'] or 0.0
    gpsLongitude = row['GPS_LONGITUDE']
    gpsLatitude = row['GPS_LATITUDE']
    gpsSatellites = row['GPS_SATELLITES']
    gpsHdop = row['GPS_HDOP']
    scheduleDeviation = row['SCHEDULE_DEVIATION'] or 0.0
    
    ret = f"""
        {eventNoTrip},
        {eventNoStop},
        '{opdDate}',
        {vehicleId},
        {meters},
        {actTime},
        {velocity},
        {direction},
        {radioQuality},
        {gpsLongitude},
        {gpsLatitude},
        {gpsSatellites},
        {gpsHdop},
        {scheduleDeviation}
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

def getSQLcmnds(rowlist):
	cmdlist = []
	for row in rowlist:
		valstr = row2vals(row)
		cmd = f"INSERT INTO {Trip_TableName} VALUES ({valstr});"
		cmdlist.append(cmd)
	return cmdlist

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

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTripTable(conn):
	with conn.cursor() as cursor:
		cursor.execute(f"""
        	DROP TABLE IF EXISTS {Trip_TableName};
        	CREATE TABLE {Trip_TableName} (
            EventNoTrip       TEXT,
            EventNoStop       TEXT,
            OpdDate        TEXT,
            VehicleId      TEXT,
            Meters      TEXT,
            ActTime        TEXT,
            Velocity        TEXT,
            Direction       TEXT,
            RadioQuality       TEXT,
            GpsLongitude       TEXT,
            GpsLatitude        TEXT,
            GpsSatellites      TEXT,
            GpsHdop                TEXT,
            ScheduleDeviation      TEXT
         	);
    	""")

		print(f"Created {Trip_TableName}")

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createBreadCrumbTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
        	DROP TABLE IF EXISTS {BreadCrumb_TableName};
        	CREATE TABLE {BreadCrumb_TableName} (
            EventNoTrip       TEXT,
            EventNoStop       TEXT,
            OpdDate        TEXT,
            VehicleId      TEXT,
            Meters      TEXT,
            ActTime        TEXT,
            Velocity        TEXT,
            Direction       TEXT,
            RadioQuality       TEXT,
            GpsLongitude       TEXT,
            GpsLatitude        TEXT,
            GpsSatellites      TEXT,
            GpsHdop                TEXT,
            ScheduleDeviation      TEXT
         	);
    	""")

		print(f"Created {BreadCrumb_TableName}")

def load(conn, icmdlist):
	with conn.cursor() as cursor:
		print(f"Loading {len(icmdlist)} rows")
		start = time.perf_counter()
    
		for cmd in icmdlist:
			# print (cmd)
			cursor.execute(cmd)


		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
    conn = dbconnect()
    rlis = readdata(Datafile)
    
    cmdlist = getSQLcmnds(rlis[:5])

    if CreateDB:
    	createTripTable(conn)
    	createBreadCrumbTable(conn)

    load(conn, cmdlist)


if __name__ == "__main__":
    main()



