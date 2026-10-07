from django.db import models
import uuid


# Create your models here.




class LogJob(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        CONVERTING = "converting", "Converting"
        CONVERTED = "converted", "Converted"
        PREDICTING = "predicting", "Predicting"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    source_url = models.URLField(max_length=2048)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    event_ids = models.JSONField(default=list)
    threshold = models.FloatField(null=True, blank=True)
    statistics = models.JSONField(default=dict)
    total_blocks = models.PositiveBigIntegerField(default=0)
    normal_count = models.PositiveBigIntegerField(default=0)
    anomaly_count = models.PositiveBigIntegerField(default=0)
    expected_chunks = models.PositiveIntegerField(null=True, blank=True)
    expected_lines = models.PositiveBigIntegerField(null=True, blank=True)
    error = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.id} — {self.status}"


class BlockFeature(models.Model):
    job = models.ForeignKey(LogJob, on_delete=models.CASCADE, related_name="features")
    block_id = models.CharField(max_length=100)
    event_counts = models.JSONField(default=dict)
    feature_vector = models.JSONField(default=list)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["job", "block_id"], name="unique_block_per_log_job")]

    def __str__(self):
        return self.block_id


class BlockPrediction(models.Model):
    class Label(models.TextChoices):
        NORMAL = "Normal", "Normal"
        ANOMALY = "Anomaly", "Anomaly"

    feature = models.OneToOneField(BlockFeature, on_delete=models.CASCADE, related_name="prediction")
    label = models.CharField(max_length=7, choices=Label.choices)
    anomaly_score = models.FloatField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.feature.block_id} — {self.label}"



class JobEvaluation(models.Model):
    job = models.OneToOneField(
        LogJob,
        on_delete=models.CASCADE,
        related_name="evaluation",
    )
    accuracy = models.FloatField()
    precision = models.FloatField()
    recall = models.FloatField()
    f1 = models.FloatField()
    confusion_matrix = models.JSONField()
    evaluated_blocks = models.PositiveIntegerField()
    updated_at = models.DateTimeField(auto_now=True)

    

# class LogJob(models.Model):
#     class Status(models.TextChoices):
#         PENDING = "pending", "Pending"
#         CONVERTING = "converting", "Converting"
#         CONVERTED = "converted", "Converted"
#         PREDICTING = "predicting", "Predicting"
#         COMPLETED = "completed", "Completed"
#         FAILED = "failed", "Failed"

#     id = models.UUIDField(
#         primary_key=True,
#         default=uuid.uuid4,
#         editable=False,
#     )

#     source_url = models.URLField(max_length=2048)

#     status = models.CharField(
#         max_length=20,
#         choices=Status.choices,
#         default=Status.PENDING,
#     )

#     # Defines the column order for every feature vector in this job.
#     event_ids = models.JSONField(default=list)

#     threshold = models.FloatField(null=True, blank=True)

#     statistics = models.JSONField(default=dict)

#     total_blocks = models.PositiveBigIntegerField(default=0)
#     normal_count = models.PositiveBigIntegerField(default=0)
#     anomaly_count = models.PositiveBigIntegerField(default=0)

#     #######################################################

#     expected_chunks = models.PositiveIntegerField(
#     null=True,
#     blank=True,
# )

#     expected_lines = models.PositiveBigIntegerField(
#         null=True,
#         blank=True,
#     )



#     error = models.TextField(blank=True)

#     created_at = models.DateTimeField(auto_now_add=True)
#     finished_at = models.DateTimeField(null=True, blank=True)

#     def __str__(self):
#         return f"{self.id} — {self.status}"


# class BlockFeature(models.Model):
#     job = models.ForeignKey(
#         LogJob,
#         on_delete=models.CASCADE,
#         related_name="features",
#     )

#     block_id = models.CharField(max_length=100)

#     # Example: {"E5": 3, "E11": 1}
#     event_counts = models.JSONField(default=dict)

#     # Example: [3, 1, 0] for event_ids ["E5", "E11", "E22"]
#     feature_vector = models.JSONField(default=list)

#     class Meta:
#         constraints = [
#             models.UniqueConstraint(
#                 fields=["job", "block_id"],
#                 name="unique_block_per_log_job",
#             ),
#         ]

#     def __str__(self):
#         return self.block_id


# class BlockPrediction(models.Model):
#     class Label(models.TextChoices):
#         NORMAL = "Normal", "Normal"
#         ANOMALY = "Anomaly", "Anomaly"

#     feature = models.OneToOneField(
#         BlockFeature,
#         on_delete=models.CASCADE,
#         related_name="prediction",
#     )

#     label = models.CharField(
#         max_length=7,
#         choices=Label.choices,
#     )

#     anomaly_score = models.FloatField()

#     created_at = models.DateTimeField(auto_now_add=True)

#     def __str__(self):
#         return f"{self.feature.block_id} — {self.label}"





# #################################
# class LogChunkResult(models.Model):
#     job = models.ForeignKey(
#         LogJob,
#         on_delete=models.CASCADE,
#         related_name="chunk_results",
#     )

#     chunk_id = models.PositiveIntegerField()
#     payload_sha256 = models.CharField(max_length=64)

#     block_counts = models.JSONField(default=dict)
#     statistics = models.JSONField(default=dict)

#     created_at = models.DateTimeField(auto_now_add=True)

#     class Meta:
#         constraints = [
#             models.UniqueConstraint(
#                 fields=["job", "chunk_id"],
#                 name="unique_chunk_result_per_job",
#             ),
#         ]
