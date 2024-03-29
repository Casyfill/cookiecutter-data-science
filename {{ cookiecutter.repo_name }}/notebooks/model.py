# from sklearn.model_selection import train_test_split
# from category_encoders import LeaveOneOutEncoder  # , TargetEncoder,

# from lightgbm import LGBMClassifier
# from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    average_precision_score,
    make_scorer,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# from sklearn.calibration import CalibratedClassifierCV


def measure(model, X, y, n_splits=5) -> dict:
    skf = StratifiedKFold(n_splits, shuffle=True, random_state=2022)

    average_precision_scorer = make_scorer(average_precision_score, average="macro")
    average_recall_scorer = make_scorer(recall_score, average="binary")
    roc_auc_scorer = make_scorer(roc_auc_score, average="macro", needs_proba=True)
    scoring = {
        "average_precision": average_precision_scorer,
        "average_recall": average_recall_scorer,
        "roc_auc": roc_auc_scorer,
    }

    metrics = cross_validate(
        model, X, y, cv=skf, return_train_score=True, scoring=scoring
    )

    metrics_avg = {k: v.mean() for k, v in metrics.items()}
    metrics_avg["features"] = list(X.columns)

    return metrics_avg


# def _target_encoder(name, columns, **kwargs):
#     return (
#         name,
#         LeaveOneOutEncoder(cols=columns, handle_unknown="ignore", **kwargs),
#         columns,
#     )


def _oh_encoder(name, columns, **kwargs):
    return (
        name,
        OneHotEncoder(handle_unknown="ignore", sparse_output=True, **kwargs),
        columns,
    )


def get_model(classifier, model_params={}):
    # transformers = [
    #         _target_encoder("unittype_te", ['lottype_cat']),
    #         _oh_encoder("ut", ["unittype_cat"]),
    # ]

    #     preprocessor = ColumnTransformer(
    #         transformers=transformers, remainder="passthrough"
    #     ).set_output(transform="pandas")

    model = classifier(**model_params)

    return Pipeline(
        steps=[
            #             ("preprocessor", preprocessor),
            ("model", model),
        ]
    )
