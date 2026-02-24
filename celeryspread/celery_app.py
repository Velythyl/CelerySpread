from __future__ import annotations

from dataclasses import dataclass
import os

from celery import Celery


@dataclass(frozen=True)
class RuntimeContext:
    broker_url: str
    result_backend: str
    #firestore_backend_settings: dict[str, str]
    #broker_transport_options: dict[str, int | float | str]
    #result_expires_seconds: int


def build_runtime_context(*, use_memory_transport: bool = False) -> RuntimeContext:
    default_project = os.environ.get("GCP_PROJECT_ID", "local-project")

    broker_url = "memory://"
    result_backend = "cache+memory://"

    return RuntimeContext(
        broker_url=broker_url,
        result_backend=result_backend,
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
    )
    return app


def initialize_celery_app(*, use_memory_transport: bool = False) -> tuple[Celery, RuntimeContext]:
    runtime_context = build_runtime_context(use_memory_transport=use_memory_transport)
    app = configure_celery_app(runtime_context)
    return app, runtime_context