from django.conf import settings
from rest_framework import serializers

from .models import BlockPrediction, LogJob


class CreateJobSerializer(serializers.Serializer):
    source_url = serializers.URLField(max_length=2048)

    def validate_source_url(self, value):
        if value not in settings.HDFS_ALLOWED_LOG_URLS:
            raise serializers.ValidationError(
                "This log URL is not configured as an allowed source."
            )

        return value


class JobSerializer(serializers.ModelSerializer):
    class Meta:
        model = LogJob
        fields = [
            "id",
            "source_url",
            "status",
            "event_ids",
            "threshold",
            "statistics",
            "total_blocks",
            "normal_count",
            "anomaly_count",
            "error",
            "created_at",
            "finished_at",
        ]


class PredictionSerializer(serializers.ModelSerializer):
    block_id = serializers.CharField(
        source="feature.block_id",
        read_only=True,
    )

    class Meta:
        model = BlockPrediction
        fields = [
            "block_id",
            "label",
            "anomaly_score",
            "created_at",
        ]