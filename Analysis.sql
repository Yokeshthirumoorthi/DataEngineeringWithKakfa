-- How many vehicles are there in the C-Tran system?

SELECT COUNT(DISTINCT vehicle_id) 
AS Count 
FROM trip;

-- How many bread crumb reading events occurred on October 2, 2020?

SELECT COUNT( * ) as "Number of Events"
FROM breadcrumb
WHERE tstamp::date = date '2020-09-02';


-- How many bread crumb reading events occurred on October 3, 2020?

SELECT COUNT( * ) as "Number of Events"
FROM breadcrumb
WHERE tstamp::date = date '2020-09-03';

-- On average, how many bread crumb readings are collected on each day of the week?

SELECT date(tstamp), count(tstamp) as total_count
FROM breadcrumb
GROUP BY date(tstamp)

-- List the C-Tran trips that crossed the I-5 bridge on October 2, 2020. To find this, search for all trips that have bread crumb readings that occurred within a lat/lon bounding box such as [(45.620460, -122.677744), (45.615477, -122.673624)]. 

SELECT * FROM breadcrumb
WHERE tstamp::date = date '2020-09-02'
AND (latitude < 45.610794 AND longitude < -122.576979)
OR (latitude > 45.606989 AND longitude > -122.569501);


-- List all bread crumb readings for a specific portion of Highway 14 (bounding box: [(45.610794, -122.576979), (45.606989, -122.569501)]) during Mondays between 4pm and 6pm. Order the readings by tstamp. Then list readings for Sundays between 6am and 8am. How do these two time periods compare for this particular location?

SELECT * FROM breadcrumb
WHERE (extract(isdow from tstamp) = 1 AND tstamp::time > '16:00:00' AND tstamp::time < '18:00:00')
AND (latitude < 45.610794 AND longitude < -122.576979)
OR (latitude > 45.606989 AND longitude > -122.569501);

-- What is the maximum velocity reached by any bus in the system?

SELECT max(speed) as max_velocity
FROM breadcrumb

-- List all possible directions and give a count of the number of vehicles that faced precisely that direction during at least one trip. Sort the list by most frequent direction to least frequent.

SELECT direction, count(trip_id) AS Vehicles
FROM breadcrumb
WHERE direction > 0
GROUP by direction
ORDER BY Vehicles desc;

-- Which is the longest (in terms of time) trip of all trips in the data?

SELECT B.trip_id, (max(A.tstamp) - min(A.tstamp)) as traveltime
FROM breadcrumb
GROUP BY trip_id
ORDER BY traveltime desc

-- Compare total number of trips on weekdays to total number of trips on weekend

SELECT COUNT(*) FROM breadcrumb
WHERE (extract(isdow from tstamp) = 6 AND extract(isdow from tstamp) = 7);

SELECT COUNT(*) FROM breadcrumb
WHERE (extract(isdow from tstamp) != 6 AND extract(isdow from tstamp) != 7);

-- Which Vehicle has made most number of trips

SELECT vehicle_id, Count(*) as count
FROM trip
GROUP BY vehicle_id
ORDER BY count desc

-- Which vehicle has made the shortest trip by time.

SELECT C.trip_id, C.vehicle_id, (max(C.tstamp) - min(C.tstamp)) as traveltime
FROM (SELECT A.trip_id, B.Vehicle_id, A.tstamp
    FROM breadcrumb AS A JOIN trip AS B
    ON A.trip_id = B.trip_id) AS C
GROUP BY C.trip_id, C.vehicle_id
ORDER BY traveltime
