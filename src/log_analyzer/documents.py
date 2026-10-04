def prediction_document(prediction):
    return {
        "job_id": str(prediction.feature.job_id),
        "block_id": prediction.feature.block_id,
        "label": prediction.label,
        "anomaly_score": prediction.anomaly_score,
        "created_at": prediction.created_at.isoformat(),
    }


def prediction_document_id(prediction):
    return (
        f"{prediction.feature.job_id}:"
        f"{prediction.feature.block_id}"
    )