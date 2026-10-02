import csv
import re
import time
from collections import Counter, defaultdict
import httpx
import joblib
import numpy as np
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from .models import BlockFeature, BlockPrediction, LogJob
import multiprocessing
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    wait,
)
from itertools import islice
from .parser_workers import initialize_parser, parse_chunk







BLOCK_PATTERN = re.compile(r"blk_-?\d+")
WILDCARD_PATTERN = re.compile(r"<\*>|\[\*\]")


# 1. Collect text logs from a URL.
def collect_data(source_url):
    with httpx.Client(
        timeout=httpx.Timeout(60.0, connect=10.0),
        follow_redirects=False,
    ) as client:
        with client.stream("GET", source_url) as response:
            response.raise_for_status()

            if response.status_code != 200:
                raise ValueError("Expected HTTP 200.")

            response.encoding = "utf-8"

            yield from response.iter_lines()


def load_conversion_resources():
    templates = []

    artifact = joblib.load(settings.HDFS_MODEL_PATH)

    event_ids = list(artifact["event_ids"])
    threshold = float(artifact["threshold"])

    with settings.HDFS_TEMPLATE_PATH.open(
        encoding="utf-8-sig",
        newline="",
    ) as file:
        for row in csv.DictReader(file):
            parts = WILDCARD_PATTERN.split(row["EventTemplate"])
            expression = ".*?".join(re.escape(part) for part in parts)

            templates.append(
                (row["EventId"], re.compile(expression))
            )

    event_ids = [event_id for event_id, _ in templates]

    if not event_ids or len(event_ids) != len(set(event_ids)):
        raise ValueError("Template event IDs are empty or duplicated.")

    return {
        "templates": templates,
        "event_ids": event_ids,
        "threshold": threshold
    }


def convert_logs(source_url, resources, workers=10, chunk_size=200_000):
    """Read and group logs. Does not write to the database."""
    if min(workers, chunk_size) <= 0:
        raise ValueError("workers and chunk_size must be positive.")

    if multiprocessing.current_process().daemon:
        raise RuntimeError(
            "Run conversion from the Django shell or Kafka consumer."
        )

    start = time.perf_counter()
    statistics = {
        "total_lines": 0,
        "blank_lines": 0,
        "matched_lines": 0,
        "unmatched_lines": 0,
        "matched_lines_without_block": 0,
    }
    block_counts = defaultdict(Counter)
    completed_chunks = 0
    merge_seconds = 0.0

    def merge_result(future):
        nonlocal completed_chunks, merge_seconds

        counts, chunk_statistics = future.result()

        for name, value in chunk_statistics.items():
            statistics[name] += value

        if chunk_statistics["unmatched_lines"]:
            raise ValueError("A chunk contains unmatched log lines.")

        if chunk_statistics["matched_lines_without_block"]:
            raise ValueError(
                "A chunk contains matched lines without block IDs."
            )

        merge_start = time.perf_counter()

        for block_id, incoming_counts in counts.items():
            block_counts[block_id].update(incoming_counts)

        merge_seconds += time.perf_counter() - merge_start
        completed_chunks += 1

        if completed_chunks == 1 or completed_chunks % 10 == 0:
            print(
                f"Merged chunks: {completed_chunks:,} | "
                f"Processed lines: {statistics['total_lines']:,} | "
                f"Blocks: {len(block_counts):,}",
                flush=True,
            )

    lines = collect_data(source_url)
    pending = set()
    context = multiprocessing.get_context("spawn")

    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=context,
            initializer=initialize_parser,
            initargs=(resources["templates"],),
        ) as executor:
            try:
                while True:
                    chunk = list(islice(lines, chunk_size))

                    if not chunk:
                        break

                    pending.add(executor.submit(parse_chunk, chunk))

                    if len(pending) >= workers * 2:
                        finished, pending = wait(
                            pending,
                            return_when=FIRST_COMPLETED,
                        )
                        for future in finished:
                            merge_result(future)

                while pending:
                    finished, pending = wait(
                        pending,
                        return_when=FIRST_COMPLETED,
                    )
                    for future in finished:
                        merge_result(future)

            except Exception:
                for future in pending:
                    future.cancel()
                raise
    finally:
        lines.close()

    if not block_counts:
        raise ValueError("No blocks were found.")

    elapsed = time.perf_counter() - start

    print(
        f"Conversion finished | blocks={len(block_counts):,} | "
        f"read/parse/merge={elapsed:.3f}s | "
        f"grouping included={merge_seconds:.3f}s",
        flush=True,
    )

    return block_counts, statistics


def save_features(job, block_counts, batch_size=100_000):
    """Build and save features atomically, then mark the job converted."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive.")

    start = time.perf_counter()
    build_seconds = 0.0
    insert_seconds = 0.0
    saved_rows = 0
    batches = 0

    with transaction.atomic():
        # Prevent two savers from inserting this job concurrently.
        locked_job = LogJob.objects.select_for_update().get(pk=job.pk)
        if locked_job.status != LogJob.Status.CONVERTING:
            raise ValueError("Job must have status 'converting'.")
        if locked_job.features.exists():
            raise ValueError(
                "Job already has features; inspect before retrying.")

        items = iter(block_counts.items())
        while True:
            build_start = time.perf_counter()
            batch = [
                BlockFeature(
                    job_id=locked_job.pk,
                    block_id=block_id,
                    event_counts=dict(counts),
                    feature_vector=[
                        counts.get(event_id, 0)
                        for event_id in locked_job.event_ids
                    ],
                )
                for block_id, counts in islice(items, batch_size)
            ]
            build_elapsed = time.perf_counter() - build_start
            build_seconds += build_elapsed

            if not batch:
                break

            insert_start = time.perf_counter()
            BlockFeature.objects.bulk_create(
                batch,
                batch_size=batch_size,
            )
            insert_elapsed = time.perf_counter() - insert_start
            insert_seconds += insert_elapsed

            saved_rows += len(batch)
            batches += 1

            print(
                f"Feature batch {batches} | rows={len(batch):,} | "
                f"total={saved_rows:,} | "
                f"build={build_elapsed:.3f}s | "
                f"bulk_create={insert_elapsed:.3f}s",
                flush=True,
            )

        locked_job.total_blocks = saved_rows
        locked_job.status = LogJob.Status.CONVERTED
        locked_job.save(update_fields=["total_blocks", "status"])

        commit_start = time.perf_counter()

    commit_seconds = time.perf_counter() - commit_start

    # Keep the caller's object consistent for the Kafka handoff.
    job.total_blocks = saved_rows
    job.status = LogJob.Status.CONVERTED

    print(
        f"Saving finished | rows={saved_rows:,} | "
        f"build={build_seconds:.3f}s | "
        f"bulk_create={insert_seconds:.3f}s | "
        f"commit={commit_seconds:.3f}s | "
        f"total={time.perf_counter() - start:.3f}s",
        flush=True,
    )
    return job


def convert_and_save_logs(
    source_url,
    batch_size=100_000,
    job_id=None,
    resources=None,
    workers=10,
    chunk_size=200_000,
):
    """Existing Kafka entry point: claim, convert, save."""
    if min(batch_size, workers, chunk_size) <= 0:
        raise ValueError(
            "batch_size, workers and chunk_size must be positive."
        )

    start = time.perf_counter()
    statistics = {}

    if job_id is None:
        job = LogJob.objects.create(
            source_url=source_url,
            status=LogJob.Status.CONVERTING,
        )
    else:
        claimed = LogJob.objects.filter(
            pk=job_id,
            source_url=source_url,
            status=LogJob.Status.PENDING,
        ).update(status=LogJob.Status.CONVERTING)

        if not claimed:
            raise ValueError("Job is missing or already started.")

        job = LogJob.objects.get(pk=job_id)

    try:
        pass

    ########################################
        if resources is None:
            resources = load_conversion_resources()

        job.event_ids = resources["event_ids"]
        job.threshold = resources["threshold"]
        job.save(update_fields=["event_ids", "threshold"])
        #########################################
        print(f"Conversion job: {job.pk}", flush=True)

        block_counts, statistics = convert_logs(
            source_url,
            resources,
            workers=workers,
            chunk_size=chunk_size,
        )

        job.statistics = statistics
        job.save(update_fields=["statistics"])

        save_features(job, block_counts, batch_size=batch_size)

        print(
            f"Convert and save total: "
            f"{time.perf_counter() - start:.3f}s",
            flush=True,
        )
        return job

    except Exception as exc:
        LogJob.objects.filter(pk=job.pk).update(
            status=LogJob.Status.FAILED,
            error=str(exc),
            statistics=statistics,
            finished_at=timezone.now(),
        )
        raise

