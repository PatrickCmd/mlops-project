from typing import Union

import pandas as pd
from haversine import Unit, haversine


def distance(row):
    """
    Returns distance between two geo-location coordinates
    """
    start = (row["start_lat"], row["start_lng"])
    end = (row["end_lat"], row["end_lng"])
    distance = haversine(start, end, unit=Unit.MILES)

    return distance


def prepare_features(input_data: Union[list[dict], pd.DataFrame]):
    """Prepare features for model predictions."""

    X = pd.DataFrame(input_data)

    X["trip_distance"] = X.apply(distance, axis=1)
    X.drop(columns=["start_lat", "start_lng", "end_lat", "end_lng"], inplace=True)

    # start_end_id
    X[["start_station_id", "end_station_id"]] = X[
        ["start_station_id", "end_station_id"]
    ].astype(str)
    X["start_end_id"] = X["start_station_id"] + "_" + X["end_station_id"]
    X.drop(columns=["start_station_id", "end_station_id"], inplace=True)

    X = X.to_dict(orient="records")

    return X


if __name__ == "__main__":
    ride = {
        "start_station_id": 31117.0,
        "end_station_id": 31602.0,
        "start_lat": 38.923330,
        "start_lng": -77.035200,
        "end_lat": 38.930800,
        "end_lng": -77.031500,
        "rideable_type": "classic_bike",
    }
    input_data = [ride]
    X = prepare_features(input_data)
    print(X)
