import os

from celery import Celery
from celery.schedules import crontab

_DEFAULT_BROKER_URL = "redis://127.0.0.1:6379/0"
_DEFAULT_RESULT_BACKEND = "redis://127.0.0.1:6379/1"

app = Celery(
    "smart_files",
    broker=os.environ.get("CELERY_BROKER_URL", _DEFAULT_BROKER_URL),
    backend=os.environ.get("CELERY_RESULT_BACKEND", _DEFAULT_RESULT_BACKEND),
    include=["tasks"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Native replacement for the old `clean-runs` Prefect-admin script: results just expire.
    result_expires=3600,
    task_routes={
        "tasks.ingest_pdf_task": {"queue": "pdf-ingest"},
        "tasks.ingest_markitdown_task": {"queue": "markitdown-ingest"},
        "tasks.ingest_xlsx_task": {"queue": "xlsx-ingest"},
        "tasks.mark_unsupported_task": {"queue": "default"},
        "tasks.poll_and_enqueue_task": {"queue": "default"},
        "tasks.dispatch_item_task": {"queue": "default"},
        "tasks.finalize_task": {"queue": "default"},
    },
    beat_schedule={
        "poll-and-ingest": {
            "task": "tasks.poll_and_enqueue_task",
            "schedule": crontab(minute="*"),
        },
    },
)
