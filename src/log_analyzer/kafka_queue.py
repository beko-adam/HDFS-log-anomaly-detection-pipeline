import json

from confluent_kafka import Producer
from django.conf import settings


def publish_log_job(job_id, *, topic=None):
    producer = Producer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 10_000,
    })

    delivery = {"completed": False, "error": None}

    def on_delivery(error, message):
        delivery["completed"] = True
        delivery["error"] = error

    producer.produce(
        topic=topic or settings.KAFKA_JOB_TOPIC,
        key=str(job_id).encode("utf-8"),
        value=json.dumps({
            "job_id": str(job_id),
        }).encode("utf-8"),
        on_delivery=on_delivery,
    )

    remaining = producer.flush(timeout=15)

    if remaining or not delivery["completed"]:
        raise RuntimeError("Kafka delivery was not confirmed.")

    if delivery["error"] is not None:
        raise RuntimeError(
            f"Kafka delivery failed: {delivery['error']}"
        )

    

# import json

# from confluent_kafka import Producer
# from django.conf import settings


# def publish_log_job(job_id):
#     producer = Producer({
#         "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
#         "enable.idempotence": True,
#         "acks": "all",
#         "delivery.timeout.ms": 10_000,
#     })

#     delivery = {"completed": False, "error": None}

#     def on_delivery(error, message):
#         delivery["completed"] = True
#         delivery["error"] = error

#     producer.produce(
#         topic=settings.KAFKA_JOB_TOPIC,
#         key=str(job_id).encode("utf-8"),
#         value=json.dumps({
#             "job_id": str(job_id),
#         }).encode("utf-8"),
#         on_delivery=on_delivery,
#     )

#     remaining = producer.flush(timeout=15)

#     if remaining or not delivery["completed"]:
#         raise RuntimeError("Kafka delivery was not confirmed.")

#     if delivery["error"] is not None:
#         raise RuntimeError(
#             f"Kafka delivery failed: {delivery['error']}"
#         )