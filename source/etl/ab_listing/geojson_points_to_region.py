import json
import csv
from shapely.geometry import shape, GeometryCollection, Point

with open('datasets/vancouver_district_and_metro_vancouver_boundaries.geojson') as f:
    geojson = json.load(f)

point = Point(40.712776, -74.005974)
neighbourhoods_shape = {}
neighbourhoods_counter = {}
for feature in geojson['features']:
    neighbourhoods_shape[feature['properties']['name']] = feature['geometry']
    neighbourhoods_counter[feature['properties']['name']] = 0
    #polygon = shape(feature['geometry'])

    #if polygon.contains(point):
        #print ('Found containing polygon:', feature)
counter = 0
with open('datasets/inside_airbnb/listings/cleaned/coordinates.csv') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader, None)
    for row in csv_reader:
        print('Processing row %d' % counter)
        counter = counter + 1
        point = Point(row[1], row[0])
        for key in neighbourhoods_shape:
            polygon = shape(neighbourhoods_shape[key])
            if polygon.contains(point):
                neighbourhoods_counter[key] = neighbourhoods_counter[key] + 1

with open("datasets/inside_airbnb/listings/cleaned/airbnb_neighbourhoods_counter.json", "w") as outfile: 
    json.dump(neighbourhoods_counter, outfile)