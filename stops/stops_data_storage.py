import database

# Generate a SQL Query string for a single trip data
def getUpdateSQLcmnd(row):
    tableName = database.Trip_TableName

    TRIP_ID_INDEX = 0
    VEHICLE_NUMBER_INDEX = 1
    ROUTE_NUMBER_INDEX = 2
    DIRECTION_INDEX = 3
    SERVICE_KEY_INDEX = 4

    trip_id = row[TRIP_ID_INDEX]
    route_id = row[ROUTE_NUMBER_INDEX]
    vehicle_id = row[VEHICLE_NUMBER_INDEX]
    service_key = row[SERVICE_KEY_INDEX]
    direction =  row[DIRECTION_INDEX]
    
    cmd = f"UPDATE {tableName} SET route_id = {route_id}, service_key = '{service_key}', direction = '{direction}' WHERE trip_id = {trip_id};"
    return cmd

#  This is the API for external caller.
#  Generate a SQL cmd string and pass it to db function
def update_trip_table(conn, row):
    cmd = getUpdateSQLcmnd(row)
    database.execute(conn, cmd)
