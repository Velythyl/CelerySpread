from __future__ import annotations

from dataclasses import dataclass
import os

from celery import Celery


@dataclass(frozen=True)
class RuntimeContext:
    broker_url: str
    result_backend: str
    firestore_backend_settings: dict[str, str]
    broker_transport_options: dict[str, int | float | str]
    result_expires_seconds: int


def build_runtime_context(*, use_memory_transport: bool = False) -> RuntimeContext:
    default_project = os.environ.get("GCP_PROJECT_ID", "local-project")

    if use_memory_transport:
        broker_url = "memory://"
        result_backend = "cache+memory://"
    else:
        broker_url = os.environ.get("CELERY_BROKER_URL", f"gcpubsub://projects/{default_project}")
        result_backend = os.environ.get("CELERY_RESULT_BACKEND", "firestore://")

    firestore_backend_settings = {
        "project_id": os.environ.get("GCP_PROJECT_ID", default_project),
        "collection": os.environ.get("CELERY_FIRESTORE_COLLECTION", "celery_task_results"),
    }

    broker_transport_options = {
        "queue_name_prefix": os.environ.get("CELERY_GCPUBSUB_QUEUE_PREFIX", "celeryspread-"),
        "ack_deadline_seconds": int(os.environ.get("CELERY_GCPUBSUB_ACK_DEADLINE", "240")),
        "expiration_seconds": int(os.environ.get("CELERY_GCPUBSUB_EXPIRATION", "86400")),
        "polling_interval": float(os.environ.get("CELERY_GCPUBSUB_POLLING_INTERVAL", "0.3")),
    }

    return RuntimeContext(
        broker_url=broker_url,
        result_backend=result_backend,
        firestore_backend_settings=firestore_backend_settings,
        broker_transport_options=broker_transport_options,
        result_expires_seconds=int(os.environ.get("CELERY_RESULT_EXPIRES_SECONDS", "3600")),
    )


def configure_celery_app(runtime_context: RuntimeContext) -> Celery:
    app = Celery(
        "celeryspread",
        broker=runtime_context.broker_url,
        backend=runtime_context.result_backend,
    )
    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_default_queue="default",
        result_expires=runtime_context.result_expires_seconds,
        broker_transport_options=runtime_context.broker_transport_options,
        firestore_backend_settings=runtime_context.firestore_backend_settings,
    )
    return app


def initialize_celery_app(*, use_memory_transport: bool = False) -> tuple[Celery, RuntimeContext]:
    runtime_context = build_runtime_context(use_memory_transport=use_memory_transport)
    app = configure_celery_app(runtime_context)
    return app, runtime_context