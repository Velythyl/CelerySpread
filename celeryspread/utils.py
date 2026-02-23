from __future__ import annotations

import uuid
from collections.abc import Iterable


def normalize_capabilities(capabilities: Iterable[str] | None) -> list[str]:
    if capabilities is None:
        return []
    normalized = [cap.strip() for cap in capabilities if cap and cap.strip()]
    return sorted(set(normalized))


def capabilities_to_queue_name(capabilities: Iterable[str]) -> str:
    normalized = normalize_capabilities(capabilities)
    return ".".join(normalized)


def generate_worker_id() -> str:
    return str(uuid.uuid4().hex)
