from django.conf import settings
from elasticsearch import Elasticsearch

from .models import BlockPrediction


def get_client():
    options = {
        "request_timeout": 30,
    }

    if settings.ELASTICSEARCH_API_KEY:
        options["api_key"] = settings.ELASTICSEARCH_API_KEY

    return Elasticsearch(
        settings.ELASTICSEARCH_URL,
        **options,
    )


def create_prediction_index(client):
    index = settings.ELASTICSEARCH_PREDICTION_INDEX

    if not client.indices.exists(index=index):
        client.indices.create(
            index=index,
            mappings={
                "properties": {
                    "job_id": {"type": "keyword"},
                    "block_id": {"type": "keyword"},
                    "label": {"type": "keyword"},
                    "anomaly_score": {"type": "double"},
                    "created_at": {"type": "date"},
                }
            },
        )


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


def index_prediction(prediction_id):
    prediction = (
        BlockPrediction.objects
        .select_related("feature")
        .get(pk=prediction_id)
    )

    with get_client() as client:
        client.index(
            index=settings.ELASTICSEARCH_PREDICTION_INDEX,
            id=prediction_document_id(prediction),
            document=prediction_document(prediction),
        )