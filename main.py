import os
import time
import argparse
from datetime import datetime

import mlflow
import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
from prefect import flow, task, get_run_logger
from sklearn.impute import SimpleImputer
from mlflow.entities import ViewType
from mlflow.tracking import MlflowClient
from prefect.context import get_run_context
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.pipeline import make_pipeline
from category_encoders import OneHotEncoder
from prefect.task_runners import SequentialTaskRunner
from sklearn.linear_model import Ridge, LinearRegression
from sklearn.model_selection import GridSearchCV, cross_val_score, train_test_split
from sklearn.feature_extraction import DictVectorizer

from utils.prepare import process_data


@task(name="Run models")
def run_models(X_train, y_train, X_valid, y_valid):
    for model_class in (Ridge, GradientBoostingRegressor, RandomForestRegressor):
        with mlflow.start_run():

            # Build and Train model
            model = make_pipeline(
                DictVectorizer(),
                SimpleImputer(),
                model_class(random_state=42),
            )
            model.fit(X_train.to_dict(orient="records"), y_train)

            # MLflow logging
            start_time = time.time()
            y_pred_train = model.predict(X_train.to_dict(orient="records"))
            y_pred_valid = model.predict(X_valid.to_dict(orient="records"))
            inference_time = time.time() - start_time

            mae_train = mean_absolute_error(y_train, y_pred_train)
            mae_valid = mean_absolute_error(y_valid, y_pred_valid)
            rmse_train = mean_squared_error(y_train, y_pred_train, squared=False)
            rmse_valid = mean_squared_error(y_valid, y_pred_valid, squared=False)

            mlflow.set_tag("author/developer", "PatrickCmd")
            mlflow.set_tag("Model", f"{model_class}")

            mlflow.log_metric("mae_train", mae_train)
            mlflow.log_metric("mae_valid", mae_valid)
            mlflow.log_metric("rmse_train", rmse_train)
            mlflow.log_metric("rmse_valid", rmse_valid)
            mlflow.log_metric(
                "inference_time",
                inference_time / (len(y_pred_train) + len(y_pred_valid)),
            )


@flow(name="mlflow-training", task_runner=SequentialTaskRunner())
def main(train_file, valid_file):
    # Set and run experiment
    ctx = get_run_context()
    MLFLOW_TRACKING_URI = "http://127.0.0.1:5000"
    EXPERIMENT_NAME = (
        f"citibikes-experiment-{ctx.flow_run.expected_start_time.strftime('%Y-%m-%d')}"
    )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    mlflow.sklearn.autolog()

    logger = get_run_logger()
    logger.info("Process data features for model training and validation")
    X_train, y_train, X_valid, y_valid = process_data(train_file, valid_file)
    logger.info(
        f"Train and Validation df shapes: {X_train.shape}, {y_train.shape}, {X_valid.shape}, {y_valid.shape}"
    )

    # Run models
    logger.info("Training models")
    run_models(X_train, y_train, X_valid, y_valid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--train_file", help="file for training data.")
    parser.add_argument("--valid_file", help="file for validation data.")
    args = parser.parse_args()

    parameters = {
        "train_file": args.train_file,
        "valid_file": args.valid_file,
    }
    main(**parameters)
