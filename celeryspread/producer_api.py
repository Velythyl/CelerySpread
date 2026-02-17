from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from celery import Celery

from .utils import capabilities_to_queue_name, normalize_capabilities

_app: Celery | None = None


def configure(app: Celery) -> None:
    global _app
    _app = app


def _require_app() -> Celery:
    if _app is None:
        raise RuntimeError("celeryspread is not configured. Call configure(app=...) first.")
    return _app


def _is_simple_requirement(capabilities: list[str]) -> bool:
    return len(capabilities) == 1


def send_task(
    task_name: str,
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    requirements: Iterable[str] | None = None,
):
    app = _require_app()
    requirements = normalize_capabilities(requirements)

    if not requirements:
        return app.send_task(task_name, args=args, kwargs=kwargs, queue="default")

    queue_name = capabilities_to_queue_name(requirements)

    if _is_simple_requirement(requirements):
        return app.send_task(task_name, args=args, kwargs=kwargs, queue=queue_name)

    result = app.send_task(task_name, args=args, kwargs=kwargs, queue=queue_name)
    app.send_task(
        "celeryspread.tasks.awaken_complex_queue_workers",
        args=[queue_name, requirements],
        queue="default",
    )
    return result
