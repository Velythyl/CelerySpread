from __future__ import annotations

import uuid
from collections.abc import Iterable


def normalize_capabilities(capabilities: Iterable[str] | None) -> list[str]:
    if capabilities is None:
        return []
    normalized = [cap.strip().lower() for cap in capabilities if cap and cap.strip()]
    return sorted(set(normalized))


def capabilities_to_queue_name(capabilities: Iterable[str]) -> str:
    normalized = normalize_capabilities(capabilities)
    return ".".join(normalized)


def generate_worker_id() -> str:
    return str(uuid.uuid4().hex)


def normalize_hostname(hostname: str) -> tuple[str, str]:
    normalized = hostname.strip()
    if "@" not in normalized:
        raise ValueError(f"hostname must be in 'name@location' format, got: {hostname}")

    name, location = normalized.split("@", 1)
    normalized_name = name.strip().lower()
    normalized_location = location.strip().lower()
    if not normalized_name or not normalized_location:
        raise ValueError(f"hostname must be in 'name@location' format, got: {hostname}")

    return normalized_name, normalized_location
