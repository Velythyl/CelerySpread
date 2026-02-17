from __future__ import annotations

from collections.abc import Callable, Iterable
from functools import wraps
from typing import TypeVar

from celery import Celery

from .utils import generate_worker_id, normalize_capabilities

F = TypeVar("F", bound=Callable)
REQUIRED_CAPS_ATTR = "__celeryspread_required_capabilities__"
SKIPPED_REGISTRATION_ATTR = "__celeryspread_skipped_registration__"
SKIP_REASON_ATTR = "__celeryspread_skip_reason__"
WORKER_CAPS_ATTR = "__celeryspread_worker_capabilities__"


class Worker:
    def __init__(
        self,
        app: Celery,
        name: str,
        location: str,
        capabilities: Iterable[str] | None = None,
    ):
        self.app = app
        self.name = name
        self.location = location
        self.id = generate_worker_id(name=name, location=location)
        self._capabilities = normalize_capabilities(capabilities)
        self.queues: list[str] = []

        self._subscribe_to_single_capability_queues()

    @property
    def capabilities(self) -> list[str]:
        return list(self._capabilities)

    def _subscribe_to_single_capability_queues(self) -> None:
        queue_names = normalize_capabilities(self.capabilities)
        self.queues = queue_names
        for queue_name in queue_names:
            self.app.control.add_consumer(queue=queue_name, destination=[self.id])

    def task(self, *task_args, **task_kwargs):
        celery_task_decorator = self.app.task(*task_args, **task_kwargs)

        def decorator(func: F):
            required_capabilities_list = normalize_capabilities(
                getattr(func, REQUIRED_CAPS_ATTR, [])
            )
            worker_capabilities_list = normalize_capabilities(self.capabilities)
            required_capabilities = set(required_capabilities_list)
            worker_capabilities = set(worker_capabilities_list)

            setattr(func, REQUIRED_CAPS_ATTR, required_capabilities_list)
            setattr(func, WORKER_CAPS_ATTR, worker_capabilities_list)

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


def task_spec(capabilities: Iterable[str]):
    required = normalize_capabilities(capabilities)

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapped(*args, **kwargs):
            return func(*args, **kwargs)

        setattr(wrapped, REQUIRED_CAPS_ATTR, required)
        setattr(wrapped, SKIPPED_REGISTRATION_ATTR, False)
        setattr(wrapped, SKIP_REASON_ATTR, None)
        setattr(wrapped, WORKER_CAPS_ATTR, [])
        return wrapped  # type: ignore[return-value]

    return decorator


def get_task_registration_diagnostics(func: Callable) -> dict[str, object]:
    return {
        "required_capabilities": getattr(func, REQUIRED_CAPS_ATTR, []),
        "worker_capabilities": getattr(func, WORKER_CAPS_ATTR, []),
        "skipped": getattr(func, SKIPPED_REGISTRATION_ATTR, False),
        "reason": getattr(func, SKIP_REASON_ATTR, None),
    }
