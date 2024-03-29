import mlflow
from mlflow.models import infer_signature
import os

# import logging
default_uri = "https://mlflow.staging2.streeteasy.cloud"
# from typing import Optional, Literal, List, Callable, Union

metaflow_config = dict(
    tracking_uri=os.getenv("MLFLOW_TRACKING_URI", default_uri),
    experiment_name="expert_transaction_matching",
    model_name="expert_transaction_matching",
    tags=dict(target="matching"),
)


def mlflow_log_model(
    model,
    Xtrain,
    ytrain,
    metaflow_config,
    model_metadata=None,
    validation=None,
    upload_model=False,
):
    mlflow.set_tracking_uri(metaflow_config["tracking_uri"])  # type: ignore
    mlflow.set_experiment(metaflow_config["experiment_name"])

    mlflow.autolog()
    with mlflow.start_run(run_name=metaflow_config.get("run_name")):
        model.fit(Xtrain, ytrain)
        mlflow.set_tags(metaflow_config.get("tags", {}))

        #         mlflow.log_params(model.get_params())
        signature = infer_signature(Xtrain, ytrain)

        if upload_model:
            mlflow.sklearn.log_model(
                model,
                signature=signature,
                registered_model_name=metaflow_config["model_name"],
                artifact_path="model",
                metadata=model_metadata,
                input_example=Xtrain.head(5),
            )
        if validation:
            Xval, yval, scorers = validation
            val_metrics = {
                f"val_{scorer._score_func.__name__}": scorer(model, Xval, yval)
                for scorer in scorers
            }
            mlflow.log_metrics(val_metrics)
    return model
