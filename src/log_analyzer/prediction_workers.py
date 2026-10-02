import time

import numpy as np
from threadpoolctl import threadpool_limits


_MODEL = None
_ANOMALY_COLUMN = None
_THREAD_LIMITER = None


def initialise_worker(model, anomaly_column):
    global _MODEL, _ANOMALY_COLUMN, _THREAD_LIMITER

    _MODEL = model
    _ANOMALY_COLUMN = anomaly_column

    # Avoid each process starting additional CPU threads.
    _THREAD_LIMITER = threadpool_limits(limits=1)

    if hasattr(_MODEL, "get_params"):
        parameters = _MODEL.get_params(deep=True)

        updates = {
            name: 1
            for name in parameters
            if name == "n_jobs" or name.endswith("__n_jobs")
        }

        if updates:
            _MODEL.set_params(**updates)


def score_batch(features):
    start = time.perf_counter()

    scores = np.asarray(
        _MODEL.predict_proba(features)[:, _ANOMALY_COLUMN],
        dtype=np.float64,
    )

    if (
        scores.shape != (len(features),)
        or not np.all(np.isfinite(scores))
        or np.any((scores < 0) | (scores > 1))
    ):
        raise ValueError("Model returned invalid probabilities.")

    scoring_seconds = time.perf_counter() - start

    return scores, scoring_seconds