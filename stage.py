import os
import argparse
from datetime import datetime

import mlflow
from prefect import flow, task, get_run_logger
from mlflow.entities import ViewType
from mlflow.tracking import MlflowClient
from prefect.task_runners import SequentialTaskRunner


@task(name="Register and stage best model")
def stage_model(tracking_uri, experiment_name):
    """Register and stage best model."""
    logger = get_run_logger()

    # Get best model from current experiment
    logger.info("Getting best model from current experiment")
    client = MlflowClient(tracking_uri=tracking_uri)
    candidates = client.search_runs(
        experiment_ids=client.get_experiment_by_name(experiment_name).experiment_id,
        # filter_string='metrics.rmse_valid < 6.5 and metrics.inference_time < 20e-6',
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=5,
        order_by=["metrics.rmse_valid ASC"],
    )

    # Register and stage best model
    logger.info("Registering and staging best model")
    best_model = candidates[0]
    experiment_id = best_model.info.experiment_id
    run_id = best_model.info.run_id
    try:
        registered_model = mlflow.register_model(
            model_uri=f"runs:/{best_model.info.run_id}/model",
            name=f"CITIBIKESDurationModel-{run_id}",
        )
    except Exception:
        client.create_registered_model(f"CITIBIKESDurationModel-{run_id}")
        registered_model = client.create_model_version(
            name=f"CITIBIKESDurationModel-{run_id}",
            source=f"s3://mlflow-models-artifact-store-cmd/{experiment_id}/{run_id}/artifacts/model",
            run_id=run_id,
        )

    client.transition_model_version_stage(
        name=f"CITIBIKESDurationModel-{run_id}",
        version=registered_model.version,
        stage="Staging",
    )

    # Update description of staged model
    logger.info("Updating description of staged model")
    client.update_model_version(
        name=f"CITIBIKESDurationModel-{run_id}",
        version=registered_model.version,
        description=f"[{datetime.now()}] The model version {registered_model.version} from experiment '{experiment_name}' was transitioned to Staging.",
    )


@flow(name="mlflow-staging", task_runner=SequentialTaskRunner())
def main(tracking_uri, experiment_name):
    # Stage best model
    stage_model(tracking_uri=tracking_uri, experiment_name=experiment_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracking_uri", help="Mlflow tracking uri.")
    parser.add_argument("--experiment_name", help="mlflow tracking experiment name.")
    args = parser.parse_args()

    parameters = {
        "tracking_uri": args.tracking_uri,
        "experiment_name": args.experiment_name,
    }
    main(**parameters)
