import os
from celery import Celery

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "pdf_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,   # optional
    include=["tasks"]
)

# Optional: configure worker concurrency
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_concurrency=2,        # Number of PDFs processed simultaneously
    task_acks_late=True,         # Ensure tasks are not lost if worker crashes
    task_reject_on_worker_lost=True,
)