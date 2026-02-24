from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from celery import Celery, Task

from .constants import CSR, REQUIRED_CAPS_ATTR
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




def _dispatch_with_requirements(
    app: Celery,
    task_name: str,
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    requirements: Iterable[str] | None = None,
    *,
    apply_async_callback: Any | None = None,
    apply_async_options: dict[str, Any] | None = None,
):
    requirements_list = normalize_capabilities(requirements)

    if apply_async_options is None:
        apply_async_options = {}

    if not requirements_list:
        if apply_async_callback is None:
            return app.send_task(task_name, args=args, kwargs=kwargs, queue=app.conf.task_default_queue)
        options = dict(apply_async_options)
        options.setdefault("queue", app.conf.task_default_queue)
        return apply_async_callback(args=args, kwargs=kwargs, **options)

    queue_name = capabilities_to_queue_name(requirements_list)


    if not _is_simple_requirement(requirements_list):
        app.send_task(
            "celeryspread.tasks.awaken_complex_queue_workers",
            args=[queue_name, requirements_list],
            queue=app.conf.task_default_queue,
        )

    if apply_async_callback is None:
        result = app.send_task(task_name, args=args, kwargs=kwargs, queue=queue_name)
    else:
        options = dict(apply_async_options)
        options.setdefault("queue", queue_name)
        result = apply_async_callback(args=args, kwargs=kwargs, **options)
    return result


def send_task(
    task_name: str,
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    requirements: Iterable[str] | None = None,
):
    app = _require_app()
    return _dispatch_with_requirements(
        app,
        task_name,
        args=args,
        kwargs=kwargs,
        requirements=requirements,
    )


class Producer:
    def __init__(self, app: Celery):
        self.app = app
        configure(app)

    def task(self, *task_args, **task_kwargs):
        def decorator(func):
            # Store task_spec requirements as metadata (used only for worker registration)
            task_spec_requirements: set[str] = set(getattr(func, REQUIRED_CAPS_ATTR, set()))
            setattr(func, REQUIRED_CAPS_ATTR, task_spec_requirements)

            class ProducerAwareTask(Task):
                abstract = True
                # task_spec requirements stored for metadata/introspection only
                _celeryspread_task_spec = task_spec_requirements

                def apply_async(self, args=None, kwargs=None, **options):
                    kwargs_for_dispatch = dict(kwargs or {})
                    # Producer requirements are ADDITIONAL - they control routing
                    # If not specified, default to None (routes to default queue)
                    runtime_requirements = kwargs_for_dispatch.pop(
                        CSR,
                        None,
                    )

                    if "queue" in options:
                        return super().apply_async(args=args, kwargs=kwargs_for_dispatch, **options)

                    super_apply_async = super().apply_async

                    return _dispatch_with_requirements(
                        self.app,
                        self.name,
                        args=args,
                        kwargs=kwargs_for_dispatch,
                        requirements=runtime_requirements,
                        apply_async_callback=lambda args, kwargs, **dispatch_options: super_apply_async(
                            args=args, kwargs=kwargs, **dispatch_options
                        ),
                        apply_async_options=options,
                    )

            return self.app.task(*task_args, base=ProducerAwareTask, **task_kwargs)(func)

        return decorator
