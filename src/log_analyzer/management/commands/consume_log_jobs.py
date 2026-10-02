import json
import logging
from uuid import UUID

from confluent_kafka import Consumer, KafkaException
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections
from django.utils import timezone

from log_analyzer.kafka_queue import publish_log_job
from log_analyzer.models import LogJob
from log_analyzer.services import (
    convert_and_save_logs,load_conversion_resources
    
)
from log_analyzer.ML_predict import (
    predict_saved_blocks,
    load_prediction_resources
   
)

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Run the Kafka conversion or prediction consumer."

    def add_arguments(self, parser):
        parser.add_argument(
            "--stage",
            choices=["conversion", "prediction"],
            default="conversion",
        )

    def handle(self, *args, **options):
        stage = options["stage"]
        if stage == "conversion":
            topic = settings.KAFKA_JOB_TOPIC
            group = settings.KAFKA_CONSUMER_GROUP
        else:
            topic = settings.KAFKA_PREDICTION_TOPIC
            group = settings.KAFKA_PREDICTION_GROUP

        consumer = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "broker.address.family": "v4",
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": 3_600_000,
        })
        consumer.subscribe([topic])
        resources = None
        self.stdout.write(
            self.style.SUCCESS(
                f"Kafka {stage} consumer is running."
            )
        )

        try:
            while True:
                message = consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())

                payload = json.loads(
                    message.value().decode("utf-8")
                )
                job_id = UUID(payload["job_id"])
                close_old_connections()
                
                try:
                    job = LogJob.objects.get(pk=job_id)

                except LogJob.DoesNotExist:
                    self.stderr.write(
                        f"Skipping stale message for missing job {job_id} | "
                        f"topic={message.topic()} | "
                        f"partition={message.partition()} | "
                        f"offset={message.offset()}"
                    )
                    consumer.commit(
                        message=message,
                        asynchronous=False,
                    )
                    continue

                # Replayed messages for terminal jobs can be skipped.
                if job.status in (
                    LogJob.Status.COMPLETED,
                    LogJob.Status.FAILED,
                ):
                    consumer.commit(
                        message=message,
                        asynchronous=False,
                    )
                    continue

                if stage == "conversion":
                    allowed_statuses = (
                        LogJob.Status.PENDING,
                        LogJob.Status.CONVERTED,
                        LogJob.Status.PREDICTING,
                    )
                else:
                    allowed_statuses = (
                        LogJob.Status.CONVERTED,
                    )

                if job.status not in allowed_statuses:
                    raise CommandError(
                        f"Job {job.pk} has status '{job.status}'. "
                        "Inspect it before retrying."
                    )
                self.stdout.write(
                    f"{stage.capitalize()} job: {job.pk}"
                )


                # Perform conversion or prediction.
                """#######################################################"""
                try:

                    # here we check if the job needs processing based on its status and the current stage
                    needs_processing = (
                        stage == "prediction" or job.status == LogJob.Status.PENDING
                    )

                    if needs_processing and resources is None:
                        resources_conversion = load_conversion_resources()

                    if stage == "conversion":
                        if job.status == LogJob.Status.PENDING:
                            job = convert_and_save_logs(
                                source_url=job.source_url,
                                job_id=job.pk,
                                resources=resources_conversion,
                                workers=settings.LOG_PARSE_WORKERS,
                                chunk_size=settings.LOG_CHUNK_SIZE,
                                batch_size=(
                                    settings.LOG_FEATURE_BATCH_SIZE
                                ),
                            )
                    else:

                        resources_prediction = load_prediction_resources()
                        predict_saved_blocks(
                            job_id,
                            batch_size=10_000,
                            workers=2,
                        )
                        
                        # predict_saved_blocks(
                        #     str(job.pk),
                        #     resources=resources_prediction,
                        #     batch_size=(
                        #         settings.LOG_PREDICTION_BATCH_SIZE
                        #     ),
                        # )


                    """#######################################################"""



                except Exception as exc:
                    logger.exception(
                        "%s failed for job %s",
                        stage,
                        job.pk,
                    )

                    # Handle resource-loading failures that happen
                    # before a processing function claims the job.
                    expected_status = (
                        LogJob.Status.PENDING
                        if stage == "conversion"
                        else LogJob.Status.CONVERTED
                    )

                    LogJob.objects.filter(
                        pk=job.pk,
                        status=expected_status,
                    ).update(
                        status=LogJob.Status.FAILED,
                        error=str(exc),
                        finished_at=timezone.now(),
                    )

                    job.refresh_from_db()

                    if job.status != LogJob.Status.FAILED:
                        raise

                    self.stderr.write(
                        f"Job {job.pk} failed: {job.error}"
                    )

                    consumer.commit(
                        message=message,
                        asynchronous=False,
                    )
                    continue

                # Handoff happens AFTER conversion has committed.
                if stage == "conversion":
                    job.refresh_from_db()

                    if job.status == LogJob.Status.CONVERTED:
                        try:
                            publish_log_job(
                                job.pk,
                                topic=(
                                    settings.KAFKA_PREDICTION_TOPIC
                                ),
                            )
                        except Exception as exc:
                            # Keep converted features and leave the
                            # source message unacknowledged.
                            raise CommandError(
                                f"Prediction dispatch failed for "
                                f"{job.pk}. Restart this consumer "
                                f"to retry the handoff."
                            ) from exc

                        self.stdout.write(
                            f"Prediction queued: {job.pk}"
                        )

                # consumer.commit(
                #     message=message,
                #     asynchronous=False,
                # )


                consumer = Consumer({
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    "broker.address.family": "v4",
                    "group.id": group,
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,

                    # Allow up to 30 minutes between poll calls.
                    "max.poll.interval.ms": 1_800_000,
                })


                close_old_connections()

        except KeyboardInterrupt:
            self.stdout.write(
                f"Stopping {stage} consumer."
            )

        finally:
            consumer.close()
            close_old_connections()

