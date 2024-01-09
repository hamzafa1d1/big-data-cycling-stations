import json
import time
import urllib.request
from kafka import KafkaProducer
from datetime import datetime

def fetch_stations_data(api_key):
    url = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={api_key}"
    response = urllib.request.urlopen(url)
    return json.loads(response.read().decode())

def format_station_data(station):
    if 'position' in station and station['last_update']:
        utc_from_timestamp = datetime.utcfromtimestamp(int(station['last_update']) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        latitude = station["position"]["lng"] if "position" in station and "lat" in station["position"] else None
        longitude = station["position"]["lat"] if "position" in station and "lng" in station["position"] else None

        formatted_station = {
            "number": station["number"] if "number" in station else None,
            "contractName": station["contract_name"] if "contract_name" in station else "",
            "name": station["name"] if "name" in station else "",
            "address": station["address"] if "address" in station else "",
            "last_update": utc_from_timestamp,
            "position": {
                "latitude": latitude,
                "longitude": longitude
            },
            "totalStands": {
                "availabilities": {
                    "bikes": station["available_bikes"] if "available_bikes" in station else 0,
                    "stands": station["bike_stands"] if "bike_stands" in station else 0
                },
                "capacity": station["bike_stands"] if "bike_stands" in station else 0
            }
        }
        return formatted_station
    return None

def send_to_kafka(producer, topic, data):
    producer.send(topic, json.dumps(data).encode())


API_KEY = "3beaee5ddce1c36dc62349cebd33a3a718464360"
producer = KafkaProducer(bootstrap_servers="localhost:9092")
while True:
    stations_data = fetch_stations_data(API_KEY)

    for station in stations_data:
        formatted_station = format_station_data(station)
        if formatted_station:
            send_to_kafka(producer, "velib-stations", formatted_station)

    produced_records_count = len(stations_data)
    current_timestamp = time.time()
    print(f"[{datetime.fromtimestamp(current_timestamp)}] Produced {produced_records_count} station records.")
    time.sleep(1)