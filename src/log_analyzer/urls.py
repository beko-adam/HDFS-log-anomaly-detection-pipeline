from django.contrib import admin
from django.urls import include, path

from .views import (
    CreateJobView,
    JobDetailView,
    PredictionListView,
    index,
    search_predictions,
)


urlpatterns = [
    path("", index, name="index"),
    path("jobs/", CreateJobView.as_view()),
    path("jobs/<uuid:pk>/", JobDetailView.as_view()),
    path(
        "jobs/<uuid:job_id>/predictions/",
        PredictionListView.as_view(),
    ),
        path(
        "predictions/search/",
        search_predictions,
        name="search-predictions",
    ),
    path("jobs/<uuid:job_id>/", JobDetailView.as_view(), name="job-detail"),
]
