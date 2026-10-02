




from django.db import transaction
from django.utils import timezone
import csv
import re
import time
from collections import Counter, defaultdict

import httpx
import joblib
import numpy as np
from django.db import transaction
from django.utils import timezone
from .models import BlockFeature, BlockPrediction, LogJob
from django.conf import settings



BLOCK_PATTERN = re.compile(r"blk_-?\d+")
WILDCARD_PATTERN = re.compile(r"<\*>|\[\*\]")




def load_prediction_resources():
    artifact = joblib.load(settings.HDFS_MODEL_PATH)

    model = artifact["model"]
    event_ids = list(artifact["event_ids"])
    threshold = float(artifact["threshold"])
    classes = list(model.classes_)

    if not event_ids or len(event_ids) != len(set(event_ids)):
        raise ValueError("Model event IDs are empty or duplicated.")

    if not 0 <= threshold <= 1:
        raise ValueError("The threshold must be between 0 and 1.")

    if 1 not in classes:
        raise ValueError("Expected anomaly class 1.")

    return {
        "model": model,
        "event_ids": event_ids,
        "threshold": threshold,
        "anomaly_column": classes.index(1),
    }

def predict_saved_blocks(job_id, batch_size=100000, resources=None):
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    start = time.perf_counter()

    claimed = LogJob.objects.filter(
        pk=job_id,
        status=LogJob.Status.CONVERTED,
    ).update(status=LogJob.Status.PREDICTING)

    if not claimed:
        raise ValueError("Job must have status 'converted'.")

    job = LogJob.objects.get(pk=job_id)

    totals = Counter()
    read_seconds = 0.0
    scoring_seconds = 0.0
    insert_seconds = 0.0

    try:
        if resources is None:
            resources = load_prediction_resources()

        if resources["event_ids"] != job.event_ids:
            raise ValueError(
                "Model event order differs from saved feature order."
            )

        if job.threshold is None or not 0 <= job.threshold <= 1:
            raise ValueError("Job has an invalid threshold.")

        if job.total_blocks == 0:
            raise ValueError("Job has no saved blocks.")

        model = resources["model"]
        anomaly_column = resources["anomaly_column"]
        last_id = 0

        with transaction.atomic():
            while True:
                read_start = time.perf_counter()

                rows = list(
                    BlockFeature.objects
                    .filter(job=job, pk__gt=last_id)
                    .order_by("pk")
                    .values_list("pk", "feature_vector")[:batch_size]
                )

                read_seconds += time.perf_counter() - read_start

                if not rows:
                    break

                X = np.asarray(
                    [vector for _, vector in rows],
                    dtype=np.float32,
                )

                if X.shape != (len(rows), len(job.event_ids)):
                    raise ValueError("Invalid saved feature dimensions.")

                scoring_start = time.perf_counter()

                scores = model.predict_proba(X)[:, anomaly_column]

                scoring_seconds += (
                    time.perf_counter() - scoring_start
                )

                if (
                    len(scores) != len(rows)
                    or not np.all(np.isfinite(scores))
                    or np.any((scores < 0) | (scores > 1))
                ):
                    raise ValueError("Model returned invalid probabilities.")

                insert_start = time.perf_counter()
                predictions = []

                for (feature_id, _), score in zip(rows, scores):
                    label = (
                        "Anomaly"
                        if score >= job.threshold
                        else "Normal"
                    )

                    totals[label] += 1

                    predictions.append(
                        BlockPrediction(
                            feature_id=feature_id,
                            label=label,
                            anomaly_score=float(score),
                        )
                    )

                BlockPrediction.objects.bulk_create(
                    predictions,
                    batch_size=batch_size,
                )

                insert_seconds += (
                    time.perf_counter() - insert_start
                )

                last_id = rows[-1][0]

            if sum(totals.values()) != job.total_blocks:
                raise ValueError(
                    "Prediction count differs from saved block count."
                )

            job.status = LogJob.Status.COMPLETED
            job.normal_count = totals["Normal"]
            job.anomaly_count = totals["Anomaly"]
            job.finished_at = timezone.now()

            job.save(
                update_fields=[
                    "status",
                    "normal_count",
                    "anomaly_count",
                    "finished_at",
                ]
            )

        print(f"\nJob: {job.pk}")
        print(f"Predicted blocks: {sum(totals.values()):,}")
        print(f"Normal: {totals['Normal']:,}")
        print(f"Anomaly: {totals['Anomaly']:,}")
        print(f"Database reads: {read_seconds:.3f}s")
        print(f"Model scoring only: {scoring_seconds:.3f}s")
        print(f"Build and insert predictions: {insert_seconds:.3f}s")
        print(f"Total prediction: {time.perf_counter() - start:.3f}s")

        return job

    except Exception as exc:
        LogJob.objects.filter(pk=job.pk).update(
            status=LogJob.Status.FAILED,
            error=str(exc),
            finished_at=timezone.now(),
        )
        raise

