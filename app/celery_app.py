from celery import Celery
from app.config import settings

celery_app = Celery(
    "cosh",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.tasks.translation"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "app.tasks.translation.*": {"queue": "translation"},
    },
)
