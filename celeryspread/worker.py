from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TypeVar

from celery import Celery

from .constants import (
    REQUIRED_CAPS_ATTR,
    SKIP_REASON_ATTR,
    SKIPPED_REGISTRATION_ATTR,
    WORKER_CAPS_ATTR,
)
from .utils import normalize_capabilities, normalize_hostname
from .tasks import register_awakening_task

F = TypeVar("F", bound=Callable)


class Worker:
    def __init__(
        self,
        app: Celery,
        hostname: str,
        capabilities: Iterable[str] | None = None,
    ):
        self.app = app

        self.hostname = hostname.strip()
        name, location = normalize_hostname(self.hostname)

        self.name = name
        self.location = normalize_capabilities([location])
        if capabilities is None:
            capabilities = []
        self._capabilities: set[str] = set(normalize_capabilities([location, *capabilities]))
        self.queues: list[str] = []
        
        self._subscribe_to_single_capability_queues()
        self.app.control.add_consumer(app.conf.task_default_queue, destination=[self.hostname])
        register_awakening_task(self.app)   # all workers can wake up

    @property
    def capabilities(self) -> set[str]:
        return set(self._capabilities)

    def _subscribe_to_single_capability_queues(self) -> None:
        queue_names = normalize_capabilities(self.capabilities)
        self.queues = queue_names
        for queue_name in queue_names:
            self.app.control.add_consumer(queue=queue_name, destination=[self.hostname])

    def task(self, *task_args, **task_kwargs):
        celery_task_decorator = self.app.task(*task_args, **task_kwargs)

        def decorator(func: F):
            required_capabilities_list = normalize_capabilities(
                getattr(func, REQUIRED_CAPS_ATTR, [])
            )
            required_capabilities: set[str] = set(required_capabilities_list)
            worker_capabilities: set[str] = self._capabilities

            setattr(func, REQUIRED_CAPS_ATTR, required_capabilities)
            setattr(func, WORKER_CAPS_ATTR, worker_capabilities)

            if required_capabilities and not required_capabilities.issubset(worker_capabilities):
                setattr(func, SKIPPED_REGISTRATION_ATTR, True)
                missing = sorted(required_capabilities - worker_capabilities)
                skip_reason = (
                    "Task registration skipped: worker lacks required capabilities "
                    f"{missing}."
                )
                setattr(func, SKIP_REASON_ATTR, skip_reason)
                return func

            setattr(func, SKIPPED_REGISTRATION_ATTR, False)
            setattr(func, SKIP_REASON_ATTR, None)
            return celery_task_decorator(func)

        return decorator


def get_task_registration_diagnostics(func: Callable) -> dict[str, object]:
    return {
        "required_capabilities": getattr(func, REQUIRED_CAPS_ATTR, []),
        "worker_capabilities": getattr(func, WORKER_CAPS_ATTR, []),
        "skipped": getattr(func, SKIPPED_REGISTRATION_ATTR, False),
        "reason": getattr(func, SKIP_REASON_ATTR, None),
    }
