import json
import logging
from uuid import UUID

from confluent_kafka import Consumer, KafkaException
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections
from django.utils import timezone

from log_analyzer.evaluation import evaluate_job
from log_analyzer.kafka_queue import publish_log_job
from log_analyzer.models import LogJob
from log_analyzer.services import (
    convert_and_save_logs,load_conversion_resources
    
)
from log_analyzer.ML_predict import (
    predict_saved_blocks,
    load_prediction_resources
   
)

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)
class Command(BaseCommand):
    help = "Run the Kafka conversion, prediction, or evaluation consumer."

    def add_arguments(self, parser):
        parser.add_argument("--stage", choices=["conversion", "prediction", "evaluation"], default="conversion")


    def ensure_topic(self, topic):
        admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "broker.address.family": "v4"})
        futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)], request_timeout=30)

        try:
            futures[topic].result()
            self.stdout.write(self.style.SUCCESS(f"Kafka topic created: {topic}"))
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise

            self.stdout.write(f"Kafka topic already exists: {topic}")


    def handle(self, *args, **options):
        stage = options["stage"]

        if stage == "conversion":
            topic = settings.KAFKA_JOB_TOPIC
            group = settings.KAFKA_CONSUMER_GROUP
        elif stage == "prediction":
            topic = settings.KAFKA_PREDICTION_TOPIC
            group = settings.KAFKA_PREDICTION_GROUP
        else:
            topic = settings.KAFKA_EVALUATION_TOPIC
            group = settings.KAFKA_EVALUATION_GROUP



        self.ensure_topic(topic)
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
        resources_conversion = None

        self.stdout.write(self.style.SUCCESS(f"Kafka {stage} consumer is running."))



        try:
            while True:
                message = consumer.poll(timeout=1.0)

                if message is None:
                    continue

                if message.error():
                    raise KafkaException(message.error())

                payload = json.loads(message.value().decode("utf-8"))
                job_id = UUID(payload["job_id"])
                close_old_connections()

                try:
                    job = LogJob.objects.get(pk=job_id)
                except LogJob.DoesNotExist:
                    self.stderr.write(f"Skipping missing job {job_id} | topic={message.topic()} | partition={message.partition()} | offset={message.offset()}")
                    consumer.commit(message=message, asynchronous=False)
                    continue

                if job.status == LogJob.Status.FAILED:
                    consumer.commit(message=message, asynchronous=False)
                    continue

                # Completed predictions must still be dispatched for evaluation.
                if stage == "conversion" and job.status == LogJob.Status.COMPLETED:
                    consumer.commit(message=message, asynchronous=False)
                    continue

                if stage == "conversion":
                    allowed_statuses = (LogJob.Status.PENDING, LogJob.Status.CONVERTED, LogJob.Status.PREDICTING)
                elif stage == "prediction":
                    allowed_statuses = (LogJob.Status.CONVERTED, LogJob.Status.COMPLETED)
                else:
                    allowed_statuses = (LogJob.Status.COMPLETED,)

                if job.status not in allowed_statuses:
                    raise CommandError(f"Job {job.pk} has status '{job.status}'. Inspect it before retrying.")

                self.stdout.write(f"{stage.capitalize()} job: {job.pk}")

                try:
                    if stage == "conversion":
                        if job.status == LogJob.Status.PENDING:
                            if resources_conversion is None:
                                resources_conversion = load_conversion_resources()

                            job = convert_and_save_logs(source_url=job.source_url, job_id=job.pk, resources=resources_conversion, workers=settings.LOG_PARSE_WORKERS, chunk_size=settings.LOG_CHUNK_SIZE, batch_size=settings.LOG_FEATURE_BATCH_SIZE)

                    elif stage == "prediction":
                        if job.status == LogJob.Status.CONVERTED:
                            predict_saved_blocks(job_id, batch_size=10_000, workers=2)

                    else:
                        metrics = evaluate_job(job_id)
                        self.stdout.write(self.style.SUCCESS(f"Evaluation completed: {job.pk} | Accuracy={metrics['accuracy']:.4f} | Precision={metrics['precision']:.4f} | Recall={metrics['recall']:.4f} | F1={metrics['f1']:.4f}"))
                        self.stdout.write(f"Confusion matrix: {metrics['confusion_matrix']}")

                except Exception as exc:
                    logger.exception("%s failed for job %s", stage, job.pk)

                    # Evaluation failure must not change the prediction job's status.
                    # Leave its message uncommitted and stop so a restart retries it.
                    if stage == "evaluation":
                        raise CommandError(f"Evaluation failed for {job.pk}: {exc}. Restart this consumer after fixing the error.") from exc

                    expected_status = LogJob.Status.PENDING if stage == "conversion" else LogJob.Status.CONVERTED
                    LogJob.objects.filter(pk=job.pk, status=expected_status).update(status=LogJob.Status.FAILED, error=str(exc), finished_at=timezone.now())
                    job.refresh_from_db()

                    if job.status != LogJob.Status.FAILED:
                        raise

                    self.stderr.write(f"Job {job.pk} failed: {job.error}")
                    consumer.commit(message=message, asynchronous=False)
                    continue

                job.refresh_from_db()

                # Publish the next stage before committing the current message.
                # Replayed prediction messages skip scoring and retry publication.
                if stage == "conversion" and job.status == LogJob.Status.CONVERTED:
                    try:
                        publish_log_job(job.pk, topic=settings.KAFKA_PREDICTION_TOPIC)
                    except Exception as exc:
                        raise CommandError(f"Prediction dispatch failed for {job.pk}. Restart this consumer to retry the handoff.") from exc

                    self.stdout.write(f"Prediction queued: {job.pk}")

                elif stage == "prediction":
                    if job.status != LogJob.Status.COMPLETED:
                        raise CommandError(f"Prediction returned with job {job.pk} in status '{job.status}'; evaluation was not queued.")

                    try:
                        publish_log_job(job.pk, topic=settings.KAFKA_EVALUATION_TOPIC)
                    except Exception as exc:
                        raise CommandError(f"Evaluation dispatch failed for {job.pk}. Restart this consumer to retry the handoff.") from exc

                    self.stdout.write(f"Evaluation queued: {job.pk}")

                consumer.commit(message=message, asynchronous=False)
                close_old_connections()

        except KeyboardInterrupt:
            self.stdout.write(f"Stopping {stage} consumer.")

        finally:
            consumer.close()
            close_old_connections()