# celery_worker.py
import os
from celery import Celery
from config import config

celery = Celery(
    __name__,
    broker=config.REDIS_URL,
    backend=config.REDIS_URL,
)

celery.conf.task_queues = {
    "fast_scrape":  {"exchange": "fast_scrape",  "binding_key": "fast_scrape"},
    "slow_scrape":  {"exchange": "slow_scrape",  "binding_key": "slow_scrape"},
    "score":        {"exchange": "score",        "binding_key": "score"},
}
celery.conf.task_default_queue = "fast_scrape"

celery.conf.task_routes = {
    "dispatch_scrapes": {"queue": "fast_scrape"},
    "process_job_work": {"queue": "fast_scrape"},
    "reconcile_job_counts": {"queue": "fast_scrape"},
    "score_job": {"queue": "score"},
}

tasks_per_second = os.getenv("CELERY_RATE_LIMIT", "10/s")
celery.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_concurrency=int(os.getenv("WORKER_CONCURRENCY", 2)),
    task_annotations={"*": {"rate_limit": tasks_per_second}},
)

celery.conf.beat_schedule = {
    "run-dispatch-every-10s": {
        "task": "dispatch_scrapes",
        "schedule": 10.0,
    },
    "reconcile-job-counts-every-5m": {
        "task": "reconcile_job_counts",
        "schedule": 300.0,
    },
    "score-pending-every-5m": {
        "task": "score_pending_jobs",
        "schedule": 300.0,
    },
}


import tasks  # noqa: E402, F401
