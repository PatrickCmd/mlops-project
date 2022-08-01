# from prefect.flow_runners import SubprocessFlowRunner
from datetime import datetime, timedelta

from prefect.deployments import Deployment, FlowScript
from prefect.orion.schemas.schedules import CronSchedule, IntervalSchedule

date_str = datetime.today().strftime("%Y-%m-%d")


Deployment(
    name="deploy-mlflow-training",
    schedule=IntervalSchedule(interval=timedelta(minutes=10080)),
    flow=FlowScript(path="./main.py", name="mlflow-training"),
    parameters={
        "train_file": "202204-capitalbikeshare-tripdata.zip",
        "valid_file": "202205-capitalbikeshare-tripdata.zip",
    },
    tags=["ml-training"],
)

Deployment(
    name="deploy-mlflow-staging",
    schedule=CronSchedule(
        cron="0 9 1 * *",
    ),
    flow=FlowScript(path="./stage.py", name="mlflow-staging"),
    parameters={
        "tracking_uri": "http://127.0.0.1:5000",
        "experiment_name": f"citibikes-experiment-{date_str}",
    },
    tags=["ml-staging"],
)
