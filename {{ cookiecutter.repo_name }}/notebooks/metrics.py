from functools import partial
from sklearn.metrics import make_scorer
from sklearn.metrics import average_precision_score

from sklearn.metrics import roc_auc_score


def precision_at_f(ytrue, y_score, feta=0.95):
    mask = y_score >= feta
    return ytrue[mask].mean()


def recall_at_f(ytrue, y_score, feta=0.95):
    mask = y_score >= feta
    detected = ytrue[mask].sum()
    total = ytrue.sum()
    return detected / total


recall_at_85 = partial(recall_at_f, feta=0.85)
recall_at_85.__name__ = "recall_at_85"

precision_at_85 = partial(precision_at_f, feta=0.85)
precision_at_85.__name__ = "precision_at_85"


METRICS = [
    make_scorer(average_precision_score, average="weighted"),  # , average = 'weighted')
    make_scorer(roc_auc_score, average="weighted"),
    make_scorer(precision_at_f, needs_proba=True, greater_is_better=True),
    make_scorer(recall_at_f, needs_proba=True, greater_is_better=True),
    make_scorer(recall_at_85, needs_proba=True, greater_is_better=True),
    make_scorer(precision_at_85, needs_proba=True, greater_is_better=True),
]
