import json

import requests

ride = {
    "start_station_id": 31117.0,
    "end_station_id": 31602.0,
    "start_lat": 38.923330,
    "start_lng": -77.035200,
    "end_lat": 38.930800,
    "end_lng": -77.031500,
    "rideable_type": "classic_bike",
}


if __name__ == "__main__":

    host = "http://127.0.0.1:9696"
    url = f"{host}/predict"
    response = requests.post(url, json=ride)
    result = response.json()

    print(result)
