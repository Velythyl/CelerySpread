from __future__ import annotations

from collections.abc import Callable, Iterable
from functools import wraps
from typing import TypeVar, overload

from .constants import (
    REQUIRED_CAPS_ATTR,
    SKIP_REASON_ATTR,
    SKIPPED_REGISTRATION_ATTR,
    WORKER_CAPS_ATTR,
)
from .utils import normalize_capabilities

F = TypeVar("F", bound=Callable)


def _decorate(func: F, capabilities: Iterable[str] | None) -> F:
    required: set[str] = set(normalize_capabilities(capabilities or []))

    @wraps(func)
    def wrapped(*args, **kwargs):
        return func(*args, **kwargs)

    setattr(wrapped, REQUIRED_CAPS_ATTR, required)
    setattr(wrapped, SKIPPED_REGISTRATION_ATTR, False)
    setattr(wrapped, SKIP_REASON_ATTR, None)
    setattr(wrapped, WORKER_CAPS_ATTR, set())
    return wrapped  # type: ignore[return-value]


@overload
def task_spec(func: F) -> F: ...


@overload
def task_spec(capabilities: Iterable[str] | None = None) -> Callable[[F], F]: ...


@overload
def task_spec(*, capabilities: Iterable[str] | None) -> Callable[[F], F]: ...


def task_spec(
    func_or_capabilities: F | Iterable[str] | None = None,
    /,
    *,
    capabilities: Iterable[str] | None = None,
):
    if callable(func_or_capabilities) and capabilities is None:
        return _decorate(func_or_capabilities, [])

    if func_or_capabilities is not None and capabilities is not None:
        raise TypeError("task_spec accepts capabilities either positionally or by keyword, not both")

    resolved_capabilities = capabilities if capabilities is not None else func_or_capabilities

    def decorator(func: F) -> F:
        return _decorate(func, resolved_capabilities)

    return decorator
