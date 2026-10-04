

import time
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

import joblib
import numpy as np

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .models import BlockFeature, BlockPrediction, LogJob
from .prediction_workers import initialise_worker, score_batch


def load_prediction_resources():
    artifact = joblib.load(settings.HDFS_MODEL_PATH)

    model = artifact["model"]
    event_ids = list(artifact["event_ids"])
    threshold = float(artifact["threshold"])
    classes = list(model.classes_)

    if not event_ids or len(event_ids) != len(set(event_ids)):
        raise ValueError("Model event IDs are empty or duplicated.")

    if not np.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("The threshold must be between 0 and 1.")

    if 1 not in classes:
        raise ValueError("Expected anomaly class 1.")

    return {
        "model": model,
        "event_ids": event_ids,
        "threshold": threshold,
        "anomaly_column": classes.index(1),
    }


def predict_saved_blocks(
    job_id,
    batch_size=10_000,
    resources=None,
    workers=2,
    insert_batch_size=2_000,
):
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    if workers <= 0:
        raise ValueError("workers must be greater than zero.")

    if insert_batch_size <= 0:
        raise ValueError("insert_batch_size must be greater than zero.")

    start = time.perf_counter()

    claimed = LogJob.objects.filter(
        pk=job_id,
        status=LogJob.Status.CONVERTED,
    ).update(
        status=LogJob.Status.PREDICTING,
    )

    if not claimed:
        raise ValueError("Job must have status 'converted'.")

    totals = Counter()

    read_seconds = 0.0
    feature_seconds = 0.0
    worker_scoring_seconds = 0.0
    waiting_seconds = 0.0
    insert_seconds = 0.0
    resource_seconds = 0.0

    # Bound the number of batches held by the executor.
    max_pending = workers
    pending = deque()

    try:
        job = LogJob.objects.get(pk=job_id)

        resource_start = time.perf_counter()

        if resources is None:
            resources = load_prediction_resources()

        resource_seconds = time.perf_counter() - resource_start

        if list(resources["event_ids"]) != list(job.event_ids):
            raise ValueError(
                "Model event order differs from saved feature order."
            )

        if (
            job.threshold is None
            or not np.isfinite(job.threshold)
            or not 0 <= job.threshold <= 1
        ):
            raise ValueError("Job has an invalid threshold.")

        if job.total_blocks <= 0:
            raise ValueError("Job has no saved blocks.")

        model = resources["model"]
        anomaly_column = resources["anomaly_column"]

        last_id = None
        exhausted = False

        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=get_context("spawn"),
            initializer=initialise_worker,
            initargs=(model, anomaly_column),
        ) as executor:
            try:
                # Preserve all-or-nothing prediction inserts.
                with transaction.atomic():
                    if BlockPrediction.objects.filter(
                        feature__job_id=job.pk,
                    ).exists():
                        raise ValueError(
                            "Job already has predictions. "
                            "Resolve existing results before retrying."
                        )

                    while pending or not exhausted:
                        # Schedule multiple batches for parallel scoring.
                        while (
                            not exhausted
                            and len(pending) < max_pending
                        ):
                            read_start = time.perf_counter()

                            queryset = BlockFeature.objects.filter(
                                job_id=job.pk,
                            )

                            if last_id is not None:
                                queryset = queryset.filter(
                                    pk__gt=last_id,
                                )

                            rows = list(
                                queryset
                                .order_by("pk")
                                .values_list(
                                    "pk",
                                    "feature_vector",
                                )[:batch_size]
                            )

                            read_seconds += (
                                time.perf_counter() - read_start
                            )

                            if not rows:
                                exhausted = True
                                break

                            feature_start = time.perf_counter()

                            feature_ids = [
                                feature_id
                                for feature_id, _ in rows
                            ]

                            features = np.asarray(
                                [vector for _, vector in rows],
                                dtype=np.float32,
                            )

                            expected_shape = (
                                len(rows),
                                len(job.event_ids),
                            )

                            if features.shape != expected_shape:
                                raise ValueError(
                                    "Invalid saved feature dimensions."
                                )

                            if not np.all(np.isfinite(features)):
                                raise ValueError(
                                    "Saved features contain "
                                    "non-finite values."
                                )

                            feature_seconds += (
                                time.perf_counter() - feature_start
                            )

                            future = executor.submit(
                                score_batch,
                                features,
                            )

                            pending.append(
                                (feature_ids, future)
                            )

                            last_id = rows[-1][0]

                            del rows, features, feature_ids, future

                        if not pending:
                            continue

                        feature_ids, future = pending.popleft()

                        wait_start = time.perf_counter()

                        scores, batch_scoring_seconds = (
                            future.result()
                        )

                        waiting_seconds += (
                            time.perf_counter() - wait_start
                        )

                        worker_scoring_seconds += (
                            batch_scoring_seconds
                        )

                        if len(scores) != len(feature_ids):
                            raise ValueError(
                                "Prediction count differs "
                                "from batch size."
                            )

                        insert_start = time.perf_counter()

                        predictions = []
                        batch_totals = Counter()

                        for feature_id, score in zip(
                            feature_ids,
                            scores,
                        ):
                            label = (
                                "Anomaly"
                                if score >= job.threshold
                                else "Normal"
                            )

                            batch_totals[label] += 1


                            ###########################

                            predictions.append(
                                BlockPrediction(
                                    feature_id=feature_id,
                                    label=label,
                                    anomaly_score=float(score),
                                )
                            )

                        BlockPrediction.objects.bulk_create(
                            predictions,
                            batch_size=insert_batch_size,
                        )

                        totals.update(batch_totals)
                        ############################
                        insert_seconds += (
                            time.perf_counter() - insert_start
                        )

                        del (
                            predictions,
                            scores,
                            feature_ids,
                            future,
                            batch_totals,
                        )

                    predicted_count = sum(totals.values())

                    if predicted_count != job.total_blocks:
                        raise ValueError(
                            "Prediction count differs "
                            "from saved block count."
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

            finally:
                # Cancel queued work on failure.
                # Running tasks finish before the pool exits.
                for _, future in pending:
                    future.cancel()

                pending.clear()

        total_seconds = time.perf_counter() - start
        predicted_count = sum(totals.values())

        print(f"\nJob: {job.pk}")
        print(f"Workers: {workers}")
        print(f"Batch size: {batch_size:,}")
        print(f"Predicted blocks: {predicted_count:,}")
        print(f"Normal: {totals['Normal']:,}")
        print(f"Anomaly: {totals['Anomaly']:,}")
        print(f"Resource loading: {resource_seconds:.3f}s")
        print(f"Database reads: {read_seconds:.3f}s")
        print(f"Feature construction: {feature_seconds:.3f}s")
        print(
            "Worker scoring time, summed: "
            f"{worker_scoring_seconds:.3f}s"
        )
        print(f"Main-process waiting: {waiting_seconds:.3f}s")
        print(
            "Build and insert predictions: "
            f"{insert_seconds:.3f}s"
        )
        print(f"Total prediction: {total_seconds:.3f}s")
        print(
            "End-to-end prediction throughput: "
            f"{predicted_count / total_seconds:,.0f} blocks/s"
        )

        return job

    except Exception as exc:
        LogJob.objects.filter(pk=job_id).update(
            status=LogJob.Status.FAILED,
            error=str(exc),
            finished_at=timezone.now(),
        )
        raise
