import csv
import multiprocessing
import re
import time

from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from itertools import islice

import httpx
import joblib
import numpy as np
from django.conf import settings
from django.db import close_old_connections, connections, transaction
from django.utils import timezone
from .models import BlockFeature, LogJob
from .parser_workers import initialize_parser, parse_chunk
import threading
import psutil


BLOCK_PATTERN = re.compile(r"blk_-?\d+")
WILDCARD_PATTERN = re.compile(r"<\*>|\[\*\]")



import threading
import psutil


class MemoryMonitor:
    def __init__(self, interval=0.5):
        self.interval = interval
        self.process = psutil.Process()
        self.stop_event = threading.Event()
        self.thread = None
        self.baseline_mb = 0.0
        self.main_peak_mb = 0.0
        self.children_peak_mb = 0.0
        self.total_peak_mb = 0.0

    def sample(self):
        try:
            main_mb = self.process.memory_info().rss / (1024 ** 2)
            children = self.process.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return

        children_mb = 0.0

        for child in children:
            try:
                children_mb += child.memory_info().rss / (1024 ** 2)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        self.main_peak_mb = max(self.main_peak_mb, main_mb)
        self.children_peak_mb = max(self.children_peak_mb, children_mb)
        self.total_peak_mb = max(self.total_peak_mb, main_mb + children_mb)

    def run(self):
        while not self.stop_event.wait(self.interval):
            self.sample()

    def __enter__(self):
        self.baseline_mb = self.process.memory_info().rss / (1024 ** 2)
        self.sample()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_event.set()
        self.thread.join()
        self.sample()
        print(f"Memory | main baseline={self.baseline_mb:,.1f} MB | main sampled peak={self.main_peak_mb:,.1f} MB | workers sampled peak={self.children_peak_mb:,.1f} MB | combined sampled peak={self.total_peak_mb:,.1f} MB", flush=True)




def collect_data(source_url):
    """Collect text logs from a URL."""
    with httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0), follow_redirects=False) as client:
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

    with settings.HDFS_TEMPLATE_PATH.open(encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            parts = WILDCARD_PATTERN.split(row["EventTemplate"])
            expression = ".*?".join(re.escape(part) for part in parts)
            templates.append((row["EventId"], re.compile(expression)))

    event_ids = [event_id for event_id, _ in templates]

    if not event_ids or len(event_ids) != len(set(event_ids)):
        raise ValueError("Template event IDs are empty or duplicated.")

    return {"templates": templates, "event_ids": event_ids, "threshold": threshold}


def convert_logs(source_url, resources, workers=5, chunk_size=200_000):
    """Read, parse and merge logs without writing to the database."""
    if min(workers, chunk_size) <= 0:
        raise ValueError("workers and chunk_size must be positive.")

    if multiprocessing.current_process().daemon:
        raise RuntimeError("Run conversion from the Django shell or Kafka consumer.")

    start = time.perf_counter()
    statistics = {"total_lines": 0, "blank_lines": 0, "matched_lines": 0, "unmatched_lines": 0, "matched_lines_without_block": 0}
    block_counts = defaultdict(Counter)
    submitted_chunks = 0
    completed_chunks = 0
    read_seconds = 0.0
    merge_seconds = 0.0
    wait_seconds = 0.0
    new_blocks = 0
    existing_block_merges = 0

    def merge_result(future):
        nonlocal completed_chunks, merge_seconds, new_blocks, existing_block_merges

        counts, chunk_statistics = future.result()

        for name, value in chunk_statistics.items():
            statistics[name] += value

        if chunk_statistics["unmatched_lines"]:
            raise ValueError("A chunk contains unmatched log lines.")

        if chunk_statistics["matched_lines_without_block"]:
            raise ValueError("A chunk contains matched lines without block IDs.")

        merge_start = time.perf_counter()

        for block_id, incoming_counts in counts.items():
            existing_counts = block_counts.get(block_id)

            if existing_counts is None:
                # Reuse the returned Counter instead of copying it.
                block_counts[block_id] = incoming_counts
                new_blocks += 1
            else:
                existing_counts.update(incoming_counts)
                existing_block_merges += 1

        merge_seconds += time.perf_counter() - merge_start
        completed_chunks += 1

        if completed_chunks == 1 or completed_chunks % 10 == 0:
            print(f"Merged chunks: {completed_chunks:,} | Processed lines: {statistics['total_lines']:,} | Blocks: {len(block_counts):,}", flush=True)

    lines = collect_data(source_url)
    pending = set()
    context = multiprocessing.get_context("spawn")

    try:
        with ProcessPoolExecutor(max_workers=workers, mp_context=context, initializer=initialize_parser, initargs=(resources["templates"],)) as executor:
            try:
                while True:
                    read_start = time.perf_counter()
                    chunk = list(islice(lines, chunk_size))
                    read_seconds += time.perf_counter() - read_start

                    if not chunk:
                        break

                    pending.add(executor.submit(parse_chunk, chunk))
                    submitted_chunks += 1

                    if len(pending) >= workers * 2:
                        wait_start = time.perf_counter()
                        finished, pending = wait(pending, return_when=FIRST_COMPLETED)
                        wait_seconds += time.perf_counter() - wait_start

                        for future in finished:
                            merge_result(future)

                while pending:
                    wait_start = time.perf_counter()
                    finished, pending = wait(pending, return_when=FIRST_COMPLETED)
                    wait_seconds += time.perf_counter() - wait_start

                    for future in finished:
                        merge_result(future)

            except Exception:
                for future in pending:
                    future.cancel()
                raise

    finally:
        close = getattr(lines, "close", None)

        if close is not None:
            close()

    if not block_counts:
        raise ValueError("No blocks were found.")

    elapsed = time.perf_counter() - start
    throughput = statistics["total_lines"] / max(elapsed, 0.000001)

    print(f"Conversion finished | blocks={len(block_counts):,} | lines={statistics['total_lines']:,} | chunks={completed_chunks:,}/{submitted_chunks:,} | workers={workers} | chunk_size={chunk_size:,}", flush=True)
    print(f"Timings | read/parse/merge={elapsed:.3f}s | input_collection={read_seconds:.3f}s | grouping included={merge_seconds:.3f}s | main_process_wait={wait_seconds:.3f}s | throughput={throughput:,.0f} lines/s", flush=True)
    print(f"Merge operations | new_blocks_reused={new_blocks:,} | existing_block_updates={existing_block_merges:,}", flush=True)

    return block_counts, statistics


def _save_feature_batch(job_id, event_ids, items, batch_number, database, insert_batch_size):
    """Build and commit one batch using this thread's connection."""
    close_old_connections()

    try:
        build_start = time.perf_counter()
        batch = [BlockFeature(job_id=job_id, block_id=block_id, event_counts=dict(counts), feature_vector=[counts.get(event_id, 0) for event_id in event_ids]) for block_id, counts in items]
        build_seconds = time.perf_counter() - build_start
        insert_start = time.perf_counter()

        with transaction.atomic(using=database):
            BlockFeature.objects.using(database).bulk_create(batch, batch_size=insert_batch_size)

        # Includes this worker's commit.
        insert_seconds = time.perf_counter() - insert_start

        print(f"Feature batch {batch_number} | rows={len(batch):,} | build={build_seconds:.3f}s | insert+commit={insert_seconds:.3f}s", flush=True)

        return {"rows": len(batch), "build_seconds": build_seconds, "insert_seconds": insert_seconds}

    finally:
        connections[database].close()


def save_features(job, block_counts, batch_size=100_000, workers=2, insert_batch_size=10_000):
    """
    Save distinct batches concurrently, then mark the job converted.

    Requires PostgreSQL and invocation outside transaction.atomic().
    All concurrent callers must use this function's advisory-lock protocol.
    Worker batches commit independently.
    Caught worker failures trigger cleanup after submitted workers finish.
    A process crash can leave partial rows requiring cleanup.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be positive.")

    if insert_batch_size <= 0:
        raise ValueError("insert_batch_size must be positive.")

    if workers <= 0:
        raise ValueError("workers must be positive.")

    database = job._state.db or "default"
    connection = connections[database]

    if connection.vendor != "postgresql":
        raise ValueError("This implementation requires PostgreSQL.")

    if connection.in_atomic_block:
        raise ValueError("Call save_features outside transaction.atomic().")

    start = time.perf_counter()
    saved_rows = 0
    build_seconds = 0.0
    insert_seconds = 0.0
    failure = None

    with transaction.atomic(using=database):
        # Coordinate savers without locking the parent job row.
        # A parent row lock can block worker foreign-key checks.
        lock_name = f"log-feature-save:{job.pk}"

        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", [lock_name])

        current_job = LogJob.objects.using(database).get(pk=job.pk)

        if current_job.status != LogJob.Status.CONVERTING:
            raise ValueError("Job must have status 'converting'.")

        feature_query = BlockFeature.objects.using(database).filter(job_id=current_job.pk)

        if feature_query.exists():
            raise ValueError("Job already has features. Inspect partial data before retrying.")

        event_ids = tuple(current_job.event_ids)
        items = iter(block_counts.items())
        batch_number = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            try:
                exhausted = False

                while not exhausted:
                    # Submit at most workers batches at once.
                    futures = []

                    for _ in range(workers):
                        batch_items = list(islice(items, batch_size))

                        if not batch_items:
                            exhausted = True
                            break

                        batch_number += 1
                        futures.append(executor.submit(_save_feature_batch, current_job.pk, event_ids, batch_items, batch_number, database, insert_batch_size))

                    if not futures:
                        break

                    # Finish submitted batches before checking results.
                    wait(futures)

                    for future in futures:
                        result = future.result()
                        saved_rows += result["rows"]
                        build_seconds += result["build_seconds"]
                        insert_seconds += result["insert_seconds"]

                    print(f"Committed feature rows: {saved_rows:,}", flush=True)

            except Exception as exc:
                failure = exc

        # Executor shutdown has waited for all submitted workers.
        if failure is not None:
            feature_query.delete()
        else:
            actual_rows = feature_query.count()

            if actual_rows != len(block_counts):
                failure = RuntimeError(f"Feature count mismatch: expected {len(block_counts):,}, found {actual_rows:,}.")
                feature_query.delete()
            else:
                current_job.total_blocks = actual_rows
                current_job.status = LogJob.Status.CONVERTED
                current_job.save(using=database, update_fields=["total_blocks", "status"])
                saved_rows = actual_rows

    # Raise after the cleanup transaction commits.
    if failure is not None:
        raise RuntimeError("Feature saving failed; this attempt's rows were removed.") from failure

    job.total_blocks = saved_rows
    job.status = LogJob.Status.CONVERTED

    print(f"Saving finished | rows={saved_rows:,} | workers={workers} | summed_build={build_seconds:.3f}s | summed_insert+commit={insert_seconds:.3f}s | wall_time={time.perf_counter() - start:.3f}s", flush=True)

    return job

def convert_and_save_logs(source_url, batch_size=100_000, job_id=None, resources=None, workers=10, chunk_size=200_000):
    """Claim, convert and save a job while measuring process memory."""
    if min(batch_size, workers, chunk_size) <= 0:
        raise ValueError("batch_size, workers and chunk_size must be positive.")

    start = time.perf_counter()
    statistics = {}

    if job_id is None:
        job = LogJob.objects.create(source_url=source_url, status=LogJob.Status.CONVERTING)
    else:
        claimed = LogJob.objects.filter(pk=job_id, source_url=source_url, status=LogJob.Status.PENDING).update(status=LogJob.Status.CONVERTING)

        if not claimed:
            raise ValueError("Job is missing or already started.")

        job = LogJob.objects.get(pk=job_id)

    with MemoryMonitor(interval=0.5) as memory:
        try:
            if resources is None:
                resources = load_conversion_resources()

            job.event_ids = resources["event_ids"]
            job.threshold = resources["threshold"]
            job.save(update_fields=["event_ids", "threshold"])
            print(f"Conversion job: {job.pk}", flush=True)

            block_counts, statistics = convert_logs(source_url, resources, workers=workers, chunk_size=chunk_size)
            memory.sample()
            print(f"Memory after conversion | main RSS={memory.process.memory_info().rss / (1024 ** 2):,.1f} MB | blocks={len(block_counts):,}", flush=True)

            job.statistics = statistics
            job.save(update_fields=["statistics"])

            save_features(job, block_counts, batch_size=batch_size)
            memory.sample()
            print(f"Memory after saving | main RSS={memory.process.memory_info().rss / (1024 ** 2):,.1f} MB", flush=True)

            del block_counts
            print(f"Convert and save total: {time.perf_counter() - start:.3f}s", flush=True)
            return job

        except Exception as exc:
            LogJob.objects.filter(pk=job.pk).update(status=LogJob.Status.FAILED, error=str(exc), statistics=statistics, finished_at=timezone.now())
            raise
    """Existing Kafka entry point: claim, convert, save."""
    if min(batch_size, workers, chunk_size) <= 0:
        raise ValueError("batch_size, workers and chunk_size must be positive.")

    start = time.perf_counter()
    statistics = {}

    if job_id is None:
        job = LogJob.objects.create(source_url=source_url, status=LogJob.Status.CONVERTING)
    else:
        claimed = LogJob.objects.filter(pk=job_id, source_url=source_url, status=LogJob.Status.PENDING).update(status=LogJob.Status.CONVERTING)

        if not claimed:
            raise ValueError("Job is missing or already started.")

        job = LogJob.objects.get(pk=job_id)

    try:
        if resources is None:
            resources = load_conversion_resources()

        job.event_ids = resources["event_ids"]
        job.threshold = resources["threshold"]
        job.save(update_fields=["event_ids", "threshold"])

        print(f"Conversion job: {job.pk}", flush=True)

        block_counts, statistics = convert_logs(source_url, resources, workers=workers, chunk_size=chunk_size)

        job.statistics = statistics
        job.save(update_fields=["statistics"])

        save_features(job, block_counts, batch_size=batch_size)

        print(f"Convert and save total: {time.perf_counter() - start:.3f}s", flush=True)

        return job

    except Exception as exc:
        LogJob.objects.filter(pk=job.pk).update(status=LogJob.Status.FAILED, error=str(exc), statistics=statistics, finished_at=timezone.now())
        raise