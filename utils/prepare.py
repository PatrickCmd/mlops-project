import os
import zipfile
from io import BytesIO

import pandas as pd
import requests
from tqdm import tqdm
from prefect import flow, task, get_run_logger
from haversine import Unit, haversine
from prefect.task_runners import SequentialTaskRunner


@task(name="Download data files from CitiBikes", retries=3, retry_delay_seconds=2)
def download_files(files):
    logger = get_run_logger()
    logger.info("Downloading datafiles")
    for file, path in files:
        # https://s3.amazonaws.com/capitalbikeshare-data/202206-capitalbikeshare-tripdata.zip
        if os.path.exists(f"{path}/{file}"):
            logger.info(f"File path: {path}/{file} already exists")
        else:
            logger.info("Downloading training and validation datasets")
            url = f"https://s3.amazonaws.com/capitalbikeshare-data/{file}"
            resp = requests.get(url, stream=True)
            save_path = f"{path}/{file}"
            with open(save_path, "wb") as handle:
                for data in tqdm(
                    resp.iter_content(),
                    desc=f"file",
                    postfix=f"save to {save_path}",
                    total=int(resp.headers["Content-length"]),
                ):
                    handle.write(data)


@task(name="Read csv data")
def read_data(filepath):
    """
    Returns cleaned dataframe
    """
    logger = get_run_logger()
    # Read csv zip file into dataframe
    logger.info("Reading CSV zip data file into pandas dataframe")
    with zipfile.ZipFile(filepath, "r") as f:
        for name in f.namelist():
            if name.endswith(".csv"):
                with f.open(name) as zd:
                    df = pd.read_csv(zd)
                break

    return df


@task(name="Save dataframe to parquet")
def save_to_parquet(df, filepath):
    """
    Saves dataframe to parquet format
    """
    logger = get_run_logger()
    # Generate destination parquet path
    logger.info("Saving dataframe from csv to parquet")
    dest_path = filepath.split(".")
    dest_path = f".{dest_path[1]}.parquet"

    # Save file destination path
    df.to_parquet(dest_path, engine="pyarrow", index=False)

    return dest_path


def distance(row):
    """
    Returns distance between two geo-location coordinates
    """
    logger = get_run_logger()
    logger.info("Get distance between two geo-location coordinates")
    start = (row["start_lat"], row["start_lng"])
    end = (row["end_lat"], row["end_lng"])
    distance = haversine(start, end, unit=Unit.MILES)

    return distance


@task(name="Prepare model features")
def prepare_features(dest_path):
    """
    Wrangle and return cleaned dataframe
    """
    # Read parquet to dataframe
    df = pd.read_parquet(dest_path)

    # Change datetime columns to datatime datatype
    df.started_at = pd.to_datetime(df.started_at)
    df.ended_at = pd.to_datetime(df.ended_at)

    df["duration"] = df.ended_at - df.started_at
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)

    mask_duration = (df.duration >= 1) & (df.duration <= 60)
    df = df[mask_duration]

    # Drop columns
    drop_columns = [
        "ride_id",
        "started_at",
        "ended_at",
        "start_station_name",
        "end_station_name",
        "member_casual",
    ]
    df.drop(columns=drop_columns, inplace=True)

    # Calculate trip distance from start and end coordinates
    df["trip_distance"] = df.apply(distance, axis=1)
    df.drop(columns=["start_lat", "start_lng", "end_lat", "end_lng"], inplace=True)

    # start_end_id
    df[["start_station_id", "end_station_id"]] = df[
        ["start_station_id", "end_station_id"]
    ].astype(str)
    df["start_end_id"] = df["start_station_id"] + "_" + df["end_station_id"]
    df.drop(columns=["start_station_id", "end_station_id"], inplace=True)

    target = "duration"
    features = list(df.drop(columns=target).columns)
    X = df[features]
    y = df[target]

    return X, y


@flow(task_runner=SequentialTaskRunner())
def process_data(train_file, valid_file):
    # Down load data files
    files = [(train_file, "./data"), (valid_file, "./data")]
    download_files(files)
    train_filepath = ("/").join((files[0][1], files[0][0]))
    valid_filepath = ("/").join((files[1][1], files[1][0]))

    # CSV dataframes
    train_df = read_data(train_filepath)
    valid_df = read_data(valid_filepath)

    # Save CSV dataframes to parquet datasets
    train_parquet_path = save_to_parquet(train_df, train_filepath)
    valid_parquet_path = save_to_parquet(valid_df, valid_filepath)

    # Process train and validation features
    X_train, y_train = prepare_features(train_parquet_path)
    X_valid, y_valid = prepare_features(valid_parquet_path)

    return X_train, y_train, X_valid, y_valid


if __name__ == "__main__":
    files = [
        ("202206-capitalbikeshare-tripdata.zip", "./data"),
        ("202205-capitalbikeshare-tripdata.zip", "./data"),
    ]

    download_files(files)
