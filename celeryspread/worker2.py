from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import List, TypeVar, Union

from celery import Celery

from .celery_entity import CeleryEntity
from .constants import (
    REQUIRED_CAPS_ATTR,
    SKIP_REASON_ATTR,
    SKIPPED_REGISTRATION_ATTR,
    WORKER_CAPS_ATTR,
)
from .utils import normalize_capabilities, normalize_hostname
from .tasks import register_awakening_task

F = TypeVar("F", bound=Callable)


class Worker(CeleryEntity):
    def __init__(
            self,
            app: Celery,
            hostname: str,
            capabilities: Iterable[str] | None = None,
            *args,
            worker: bool | None = None,
            **kwargs
    ):
        if worker not in (None, True):
            raise TypeError("Worker only supports worker=True.")
        super().__init__(app)

        name, location = normalize_hostname(hostname.strip())
        self.hostname = f"{name}@{location}"
        self.self_queue_name = self.hostname.replace("@", "-at-")

        self.name = name
        self.location = normalize_capabilities([location])
        if capabilities is None:
            capabilities = []
        self._capabilities: set[str] = set(normalize_capabilities([location, *capabilities]))
        self.queues: list[str] = []
        self._worker_ready_handler = None

        self._amqp_instance = None
        self._subscribe_to_single_capability_queues()  # app.conf.task_default_queue
        # self.app.control.add_consumer("celeryspread", destination=[self.hostname])
        register_awakening_task(self.app)  # all workers can wake up

    @property
    def capabilities(self) -> set[str]:
        return set(self._capabilities)

    def subscribe_to_queue(self, queue_name: Union[str, List[str]]) -> None:
        if isinstance(queue_name, str):
            queue_name = [queue_name]

        for q in queue_name:
            self._amqp_instance.queues.select_add(q)

    def _subscribe_to_single_capability_queues(self) -> None:
        queue_names = normalize_capabilities(self.capabilities)
        self.queues = queue_names + ["celeryspread", self.self_queue]

        from celery.signals import celeryd_after_setup

        def _setup_queues(sender, instance, **kwargs):
            self._amqp_instance = instance.app.amqp
            for q in self.queues:
                self._amqp_instance.queues.select_add(q)
            # self.subscribe_to_queue(self.queues)

        self._worker_ready_handler = _setup_queues
        celeryd_after_setup.connect(self._worker_ready_handler, weak=False)

    @property
    def amqp_instance(self):
        if self._amqp_instance is None:
            raise AttributeError("Celery AMQP instance has not been initialized.")
        return self._amqp_instance

    def task(self, *task_args, **task_kwargs):

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
            return self.app.task(*task_args, **task_kwargs)(func)

        return decorator


def get_task_registration_diagnostics(func: Callable) -> dict[str, object]:
    return {
        "required_capabilities": getattr(func, REQUIRED_CAPS_ATTR, []),
        "worker_capabilities": getattr(func, WORKER_CAPS_ATTR, []),
        "skipped": getattr(func, SKIPPED_REGISTRATION_ATTR, False),
        "reason": getattr(func, SKIP_REASON_ATTR, None),
    }
