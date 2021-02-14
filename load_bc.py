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

def main():
    conn = dbconnect()
    # rlis = readdata(Datafile)
    
    # cmdlist = getSQLcmnds(rlis)

    if CreateDB:
    	createTripTable(conn)
    	createBreadCrumbTable(conn)


    # load(conn, cmdlist)


if __name__ == "__main__":
    main()



