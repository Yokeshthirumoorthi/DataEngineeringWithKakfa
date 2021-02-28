#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []
with open('q1.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:]:
        row = line.split("\t")
        
        # Uncomment these lines
        lat = row[0]
        long = row[1]
        speed = row[2]

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
     	
        try:
            latitude, longitude = map(float, (lat, long))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("q1.geojson", "w") as f:
    f.write('%s' % collection)