from celery import Celery
from celery.schedules import crontab
from app.config import settings

# To run workers with beat scheduler (single process, dev/stage):
#   celery -A app.celery_app worker --loglevel=info -Q translation,similarity,sync -B
# For production, run beat as a separate process:
#   celery -A app.celery_app beat --loglevel=info

celery_app = Celery(
    "cosh",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.tasks.translation", "app.tasks.similarity", "app.tasks.sync"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "app.tasks.translation.*": {"queue": "translation"},
        "app.tasks.similarity.*": {"queue": "similarity"},
        "app.tasks.sync.*": {"queue": "sync"},
    },
    beat_schedule={
        "daily-similarity-scan": {
            "task": "app.tasks.similarity.detect_similarity_all_cores",
            "schedule": crontab(hour=2, minute=0),
        },
    },
)
