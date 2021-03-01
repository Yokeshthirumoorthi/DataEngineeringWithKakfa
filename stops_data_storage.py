import database

# Generate a SQL Query string for a single trip data
def getUpdateSQLcmnd(trip_json):
    tableName = database.Trip_TableName

    trip_id = trip_json['trip_id']
    route_id = trip_json['route_number']
    vehicle_id = trip_json['vehicle_number']
    service_key = trip_json['service_key']
    direction = trip_json['direction']
    
    cmd = f"UPDATE {tableName} SET route_id = {route_id}, service_key = '{service_key}', direction = '{direction}' WHERE trip_id = {trip_id};"
    return cmd

#  This is the API for external caller.
#  Generate a SQL cmd string and pass it to db function
def update_trip_table(conn, row):
    cmd = getUpdateSQLcmnd(row)
    database.execute(conn, cmd)
