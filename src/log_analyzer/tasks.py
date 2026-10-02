
import logging

from celery import shared_task

from .models import LogJob
from .services import (
    convert_and_save_logs,
    load_resources,
    predict_saved_blocks,
)


from .ML_predict import (
    convert_and_save_logs,
    load_conversion_resources
)

logger = logging.getLogger(__name__)


@shared_task
def process_log_job(job_id):
    job = LogJob.objects.get(pk=job_id)

    if job.status != LogJob.Status.PENDING:
        return

    try:
        # Conversion loads resources after claiming the job.
        # Passing a shared dictionary lets prediction reuse them.
        #
        # Load explicitly here, then handle resource-loading failure below.
        resources = load_resources()


        # job = convert_and_save_logs(
        #     source_url=job.source_url,
        #     job_id=job.pk,
        #     resources=resources,
        #     workers=2,
        #     chunk_size=20_000,
        # )


        job = convert_and_save_logs(
            source_url=job.source_url,
            job_id=job.pk,
            resources=resources,
            workers=7,
            chunk_size=30_000,
        )

        

        predict_saved_blocks(
            job_id=job.pk,
            resources=resources,
        )


        # job = convert_and_save_logs(
        #     source_url=job.source_url,
        #     job_id=job.pk,
        #     resources=resources,
        # )

        # predict_saved_blocks(
        #     job_id=job.pk,
        #     resources=resources,
        # )

    except Exception as exc:
        from django.utils import timezone

        logger.exception("Processing failed for job %s", job_id)

        # Resource-loading errors occur before conversion claims the job.
        LogJob.objects.filter(
            pk=job_id,
            status=LogJob.Status.PENDING,
        ).update(
            status=LogJob.Status.FAILED,
            error=str(exc),
            finished_at=timezone.now(),
        )


# import logging

# from celery import shared_task
# from django.utils import timezone

# from .models import LogJob
# from .services import convert_and_save_logs, predict_saved_blocks


# logger = logging.getLogger(__name__)


# @shared_task
# def process_log_job(job_id):
#     job = LogJob.objects.get(pk=job_id)

#     if job.status != LogJob.Status.PENDING:
#         return

#     try:
#         job = convert_and_save_logs(
#             source_url=job.source_url,
#             job_id=job.pk,
#         )

#         predict_saved_blocks(str(job.pk))

#     except Exception as exc:
#         logger.exception("Processing failed for job %s", job_id)

#         # The service functions also record their processing failures.
#         LogJob.objects.filter(
#             pk=job_id,
#             status__in=[
#                 LogJob.Status.PENDING,
#                 LogJob.Status.CONVERTING,
#                 LogJob.Status.PREDICTING,
#             ],
#         ).update(
#             status=LogJob.Status.FAILED,
#             error=str(exc),
#             finished_at=timezone.now(),
#         )