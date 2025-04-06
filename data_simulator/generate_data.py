import csv 
import random
from datetime import datetime, timedelta
import os

NUM_CARS = 1000
NUM_ROADS = 10

START_TIME = datetime(2025, 4, 6, 6, 0, 0)
TIME_INTERVAL = timedelta(seconds=5)  # generate data every 5 seconds
OUTPUT_FOLDER = 'data/raw/'
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

road_centers = [
    (43.6532, -79.3832),  #took from the internet
    (43.7000, -79.4000),
    (43.6600, -79.3800),
    (43.6400, -79.3900),
    (43.6800, -79.3700),
    (43.6700, -79.3500),
    (43.6900, -79.3600),
    (43.7100, -79.3400),
    (43.7200, -79.3300),
    (43.7300, -79.3200),
]

with open(os.path.join(OUTPUT_FOLDER, 'traffic_data.csv'), mode='w', newline='')as file:
    writer = csv.writer(file)
    writer.writerow(['timestamp', 'car_id', 'speed', 'lat', 'lon', 'road_id'])

    current_time = START_TIME
    for i in range(NUM_CARS):
        car_id = random.randint(1000, 9999)
        road_id = random.randint(0, NUM_ROADS - 1)
        speed = random.gauss(50, 20) #normal distribution
        speed = max(0, min(speed, 120))

        center_lat, center_lon = road_centers[road_id]
        lat = center_lat + random.uniform(-0.005, 0.005)
        lon = center_lon + random.uniform(-0.005, 0.005)

        writer.writerow([
            current_time.strftime('%Y-%m-%d %H:%M:%S'),
            car_id,
            round(speed, 2),
            round(lat, 6),
            round(lon, 6),
            road_id
        ])

        current_time += TIME_INTERVAL

