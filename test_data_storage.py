import database
import stops_data_gatherer
import stops_data_storage
import time

#  Test method to pull data from url and save on db
if __name__ == "__main__":
    conn = database.dbconnect()
    data_url="http://34.83.136.192:8000/getStopEvents/"
    trips_data = stops_data_gatherer.get_stops_data(data_url)

    start = time.perf_counter()

    loaded = 0
    print(f"Loading {len(trips_data)} rows")
    for row in trips_data:
        stops_data_storage.update_trip_table(conn, row)
        loaded = loaded + 1

    print(f"Loaded {loaded} rows")
    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')   
