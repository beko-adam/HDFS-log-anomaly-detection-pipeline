from django.contrib import admin
from django.urls import include, path

from .views import (
    CreateJobView,
    JobDetailView,
    PredictionListView,
    index,
)


urlpatterns = [
    path("", index, name="index"),
    path("jobs/", CreateJobView.as_view()),
    path("jobs/<uuid:pk>/", JobDetailView.as_view()),
    path(
        "jobs/<uuid:job_id>/predictions/",
        PredictionListView.as_view(),
    ),
]
