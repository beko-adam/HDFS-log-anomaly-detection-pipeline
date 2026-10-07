import pandas as pd
from django.conf import settings
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)

from log_analyzer.models import BlockPrediction, JobEvaluation


def evaluate_job(job_id):
    truth = pd.read_csv(
        settings.HDFS_LABEL_PATH,
        dtype={"BlockId": str},
    )[["BlockId", "Label"]]

    # Adjust these ORM fields to match your models.
    rows = BlockPrediction.objects.filter(
        feature__job_id=job_id,
    ).values_list(
        "feature__block_id",
        "label",
    )

    predicted = pd.DataFrame.from_records(
        rows.iterator(chunk_size=10_000),
        columns=["BlockId", "Prediction"],
    )

    if truth.empty or predicted.empty:
        raise ValueError("Labels or predictions are empty.")

    if truth["BlockId"].duplicated().any():
        raise ValueError("Duplicate block IDs in labels.")

    if predicted["BlockId"].duplicated().any():
        raise ValueError("Duplicate block IDs in predictions.")

    evaluation = truth.merge(
        predicted,
        on="BlockId",
        how="outer",
        validate="one_to_one",
        indicator=True,
    )

    coverage = evaluation["_merge"].value_counts().to_dict()

    if not evaluation["_merge"].eq("both").all():
        raise ValueError(
            f"Labels and predictions do not match: {coverage}"
        )

    mapping = {"Normal": 0, "Anomaly": 1}
    y_true = evaluation["Label"].map(mapping)
    y_pred = evaluation["Prediction"].map(mapping)

    if y_true.isna().any() or y_pred.isna().any():
        raise ValueError("Unknown or missing labels.")

    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(
            precision_score(y_true, y_pred, zero_division=0)
        ),
        "recall": float(
            recall_score(y_true, y_pred, zero_division=0)
        ),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "confusion_matrix": confusion_matrix(
            y_true, y_pred, labels=[0, 1]
        ).tolist(),
        "evaluated_blocks": len(evaluation),
    }

    JobEvaluation.objects.update_or_create(
        job_id=job_id,
        defaults=metrics,
    )

    return metrics