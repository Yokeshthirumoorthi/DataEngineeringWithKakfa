#!/usr/bin/env python
#
# Copyright 2021 Yokesh Thirumoorthi

import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup, Tag
import json

# h3 element looks like "Stop Events for trip 170612619 for today". 
# Extract the 5th word as trip_id
def get_trip_id(h3_element):
    splitted_title = h3_element.get_text().split()
    return splitted_title[4]

# Extract the table under each h3 element
# And extract the first row in the table.
# NOTE: row at index 0 is header, hence extract row at index 1
def get_first_row_in_stops_table(h3_element):
    table = h3_element.find_next("table")
    rows = table.find_all('tr')
    first_row = rows[1]
    data = first_row.find_all('td')
    return data

# Convert int(0 / 1) to direction enum 
# 0 means 'Out' and 1 means 'Back'
def int_to_dir_enum(dir_int):
    return 'Back' if dir_int == '1' else 'Out'

# Convert W - Weekday, S - Saturday, U - Sunday
def char_to_service_enum(service_char):
    if service_char == 'W':
        return 'Weekday'
    elif service_char == 'S':
        return 'Saturday'
    elif service_char == 'U':
        return 'Sunday'
    else:
        return 'Sunday' # Do the default

def create_data_row_json(trip_id, row):
    trip_json = {}

    column_values = [column.text for column in row]
    VEHICLE_NUMBER_INDEX = 0
    ROUTE_NUMBER_INDEX = 3
    DIRECTION_INDEX = 4
    SERVICE_KEY_INDEX = 5

    trip_json['trip_id'] = trip_id
    trip_json['vehicle_number'] = column_values[VEHICLE_NUMBER_INDEX]
    trip_json['route_number'] = column_values[ROUTE_NUMBER_INDEX]
    trip_json['direction'] = int_to_dir_enum(column_values[DIRECTION_INDEX])
    trip_json['service_key'] = char_to_service_enum(column_values[SERVICE_KEY_INDEX])

    return trip_json

# Get the trip headings
def get_trips_headings(soup):
    all_trips_headings = soup.find_all("h3")
    return all_trips_headings

def print_trips(trips):
    columnNames = ["trip_id", "vehicle_number", "route_number", "direction", "service_key"]
    df = pd.DataFrame(trips, columns=columnNames)
    print(df)

def get_stops_data(url):
    html = urlopen(url)
    soup = BeautifulSoup(html, 'html.parser')
    trips_headings = get_trips_headings(soup)
    trips = []

    for trip_heading in trips_headings:
        trip_id = get_trip_id(trip_heading)
        stop_row = get_first_row_in_stops_table(trip_heading)
        trip_data = create_data_row_json(trip_id, stop_row)
        trips.append(trip_data)

    return trips

if __name__ == "__main__":
    trips = get_stops_data("http://34.83.136.192:8000/getStopEvents/")
    print(trips)