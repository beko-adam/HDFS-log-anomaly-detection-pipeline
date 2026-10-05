from django.shortcuts import render

# Create your views here.

import logging

from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import generics, status
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import BlockPrediction, LogJob, BlockFeature
from .serializers import (
    CreateJobSerializer,
    JobSerializer,
    PredictionSerializer,
)
from .kafka_queue import publish_log_job

logger = logging.getLogger(__name__)


import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .kafka_queue import publish_log_job
from .models import LogJob, BlockPrediction, BlockFeature
from .serializers import CreateJobSerializer, JobSerializer
from django.http import JsonResponse
from django.conf import settings
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.permissions import AllowAny

import csv

from django.http import StreamingHttpResponse
from django.shortcuts import get_object_or_404
from rest_framework import generics

class Echo:
    def write(self, value):
        return value

logger = logging.getLogger(__name__)




def index(request):


   
    LogJob.objects.all().first().delete()
    return JsonResponse({"status": "ok", "service": "hdfs-anomaly"})




def delete(self, request, job_id):
    job = get_object_or_404(LogJob, pk=job_id)
    if job.status not in [LogJob.Status.COMPLETED, LogJob.Status.FAILED]:
        return Response({"detail": "Only completed or failed jobs can be deleted."}, status=status.HTTP_409_CONFLICT)
    job.delete()
    return Response(status=status.HTTP_204_NO_CONTENT)



class CreateJobView(APIView):

    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = CreateJobSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job = LogJob.objects.create(
            source_url=serializer.validated_data["source_url"],
            status=LogJob.Status.PENDING,
        )

        try:
            publish_log_job(job.pk)

        except Exception:
            logger.exception(
                "Kafka publishing failed for job %s",
                job.pk,
            )

            # A delivery timeout can have an uncertain outcome.
            # Preserve the job so its status can be inspected.
            return Response(
                {
                    "id": str(job.pk),
                    "detail": (
                        "Kafka delivery could not be confirmed. "
                        "Check this job's status before submitting again."
                    ),
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        job.refresh_from_db()

        return Response(
            JobSerializer(job).data,
            status=status.HTTP_202_ACCEPTED,
        )
    



class PredictionPagination(PageNumberPagination):
    page_size = 100



class JobDetailView(APIView):
    authentication_classes = []
    permission_classes = [AllowAny]

    def get(self, request, job_id):
        job = get_object_or_404(LogJob, pk=job_id)
        return Response(JobSerializer(job).data)

    def delete(self, request, job_id):
        job = get_object_or_404(LogJob, pk=job_id)
        if job.status not in [LogJob.Status.COMPLETED, LogJob.Status.FAILED]:
            return Response({"detail": "Only completed or failed jobs can be deleted."}, status=status.HTTP_409_CONFLICT)
        job.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
    

# class PredictionListView(generics.ListAPIView):
#     serializer_class = PredictionSerializer
#     pagination_class = PredictionPagination

#     def get_queryset(self):
#         job = get_object_or_404(
#             LogJob,
#             pk=self.kwargs["job_id"],
#         )

#         if job.status != LogJob.Status.COMPLETED:
#             return BlockPrediction.objects.none()

#         queryset = (
#             BlockPrediction.objects
#             .filter(feature__job=job)
#             .select_related("feature")
#             .order_by("pk")
#         )

#         label = self.request.query_params.get("label")

#         if label:
#             queryset = queryset.filter(label=label)

#         return queryset



class PredictionListView(generics.ListAPIView):
    serializer_class = PredictionSerializer
    pagination_class = PredictionPagination

    def get_queryset(self):
        job = get_object_or_404(
            LogJob,
            pk=self.kwargs["job_id"],
        )

        if job.status != LogJob.Status.COMPLETED:
            return BlockPrediction.objects.none()

        queryset = (
            BlockPrediction.objects
            .filter(feature__job=job)
            .select_related("feature")
            .order_by("pk")
        )

        label = self.request.query_params.get("label")

        if label:
            queryset = queryset.filter(label=label)

        return queryset

    def list(self, request, *args, **kwargs):
        if request.query_params.get("export") != "csv":
            return super().list(request, *args, **kwargs)

        queryset = self.filter_queryset(self.get_queryset())

        writer = csv.writer(Echo())

        def rows():
            yield writer.writerow(["BlockId", "Prediction"])

            # Adjust feature__block_id to your actual model field.
            records = queryset.values_list(
                "feature__block_id",
                "label",
            ).iterator(chunk_size=2000)

            for block_id, label in records:
                yield writer.writerow([block_id, label])

        response = StreamingHttpResponse(
            rows(),
            content_type="text/csv",
        )
        response["Content-Disposition"] = (
            'attachment; filename="predictions.csv"'
        )
        return response





from .search import get_client


@api_view(["GET"])
def search_predictions(request):
    filters = []

    for field in ("job_id", "block_id", "label"):
        value = request.query_params.get(field)

        if value:
            filters.append({"term": {field: value}})

    query = (
        {"bool": {"filter": filters}}
        if filters
        else {"match_all": {}}
    )

    with get_client() as client:
        result = client.search(
            index=settings.ELASTICSEARCH_PREDICTION_INDEX,
            query=query,
            size=50,
            sort=[{"created_at": "desc"}],
        )

    return Response({
        "results": [
            hit["_source"]
            for hit in result["hits"]["hits"]
        ]
    })