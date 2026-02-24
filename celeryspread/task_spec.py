from __future__ import annotations

from collections.abc import Callable, Iterable
from functools import wraps
from typing import TypeVar

from .constants import (
    REQUIRED_CAPS_ATTR,
    SKIP_REASON_ATTR,
    SKIPPED_REGISTRATION_ATTR,
    WORKER_CAPS_ATTR,
)
from .utils import normalize_capabilities

F = TypeVar("F", bound=Callable)


def task_spec(capabilities: Iterable[str]):
    required: set[str] = set(normalize_capabilities(capabilities))

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapped(*args, **kwargs):
            return func(*args, **kwargs)

        setattr(wrapped, REQUIRED_CAPS_ATTR, required)
        setattr(wrapped, SKIPPED_REGISTRATION_ATTR, False)
        setattr(wrapped, SKIP_REASON_ATTR, None)
        setattr(wrapped, WORKER_CAPS_ATTR, set())
        return wrapped  # type: ignore[return-value]

    return decorator
