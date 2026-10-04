from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch.helpers import bulk

from log_analyzer.models import BlockPrediction
from log_analyzer.search import (
    create_prediction_index,
    get_client,
    prediction_document,
    prediction_document_id,
)


class Command(BaseCommand):
    help = "Index existing block predictions in Elasticsearch."

    def handle(self, *args, **options):
        predictions = (
            BlockPrediction.objects
            .select_related("feature")
            .order_by("pk")
        )

        def actions():
            for prediction in predictions.iterator(chunk_size=1000):
                yield {
                    "_op_type": "index",
                    "_index": settings.ELASTICSEARCH_PREDICTION_INDEX,
                    "_id": prediction_document_id(prediction),
                    "_source": prediction_document(prediction),
                }

        with get_client() as client:
            create_prediction_index(client)

            count, _ = bulk(
                client,
                actions(),
                chunk_size=1000,
            )

            client.indices.refresh(
                index=settings.ELASTICSEARCH_PREDICTION_INDEX
            )

        self.stdout.write(
            self.style.SUCCESS(f"Indexed {count} predictions.")
        )