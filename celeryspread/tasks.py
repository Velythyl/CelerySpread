from __future__ import annotations

from collections.abc import Callable, Iterable
import logging
from typing import Any

from celery import Celery

from .utils import normalize_capabilities


logger = logging.getLogger(__name__)


def find_matching_workers(inspector: Any, requirements: Iterable[str]) -> list[str]:
    needed = set(normalize_capabilities(requirements))
    active_queues = inspector.active_queues() or {}

    matching_workers: list[str] = []
    for worker_id, queues in active_queues.items():
        subscribed = set(normalize_capabilities(queue["name"] for queue in queues))
        if needed.issubset(subscribed):
            matching_workers.append(worker_id)
    return matching_workers


def register_awakening_task(
    app: Celery,
    notify_admin: Callable[[str], None] | None = None,
    inspect_timeout: float = 2.0,
):
    @app.task(
        name="celeryspread.tasks.awaken_complex_queue_workers",
        bind=True,
        queue=app.conf.task_default_queue,
    )
    def awaken_complex_queue_workers(self, queue_name: str, requirements: list[str]):
        inspector = app.control.inspect(timeout=inspect_timeout)
        matching_workers = find_matching_workers(inspector, requirements)

        if not matching_workers:
            logger.warning(
                "No workers available for queue '%s' with requirements %s",
                queue_name,
                normalize_capabilities(requirements),
            )
            if notify_admin is not None:
                notify_admin(f"No workers available for queue: {queue_name}")
            return {"status": "no_matching_workers", "queue": queue_name}

        app.control.add_consumer(queue=queue_name, destination=matching_workers)
        return {
            "status": "awakened",
            "queue": queue_name,
            "workers": matching_workers,
        }

    return awaken_complex_queue_workers
