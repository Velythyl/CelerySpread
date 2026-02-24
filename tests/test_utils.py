import pytest

from celeryspread.utils import (
    capabilities_to_queue_name,
    generate_worker_id,
    normalize_capabilities,
    normalize_hostname,
)


def test_capabilities_to_queue_name_sorts_and_deduplicates():
    assert capabilities_to_queue_name(["gpu", "FactoryB", "gpu"]) == "factoryb.gpu"


def test_capabilities_to_queue_name_empty_list():
    assert capabilities_to_queue_name([]) == ""


def test_capabilities_to_queue_name_single_capability():
    assert capabilities_to_queue_name(["gpu"]) == "gpu"


def test_generate_worker_id():
    assert generate_worker_id()


def test_generate_worker_id_uniqueness():
    ids = [generate_worker_id() for _ in range(100)]
    assert len(set(ids)) == 100


# --- normalize_capabilities edge cases ---


def test_normalize_capabilities_returns_empty_for_none():
    assert normalize_capabilities(None) == []


def test_normalize_capabilities_returns_empty_for_empty_list():
    assert normalize_capabilities([]) == []


def test_normalize_capabilities_strips_whitespace():
    assert normalize_capabilities([" gpu ", "  FactoryB  "]) == ["factoryb", "gpu"]


def test_normalize_capabilities_removes_empty_strings():
    assert normalize_capabilities(["gpu", "", "FactoryB", "   "]) == ["factoryb", "gpu"]


def test_normalize_capabilities_deduplicates():
    assert normalize_capabilities(["gpu", "gpu", "GPU"]) == ["gpu"]


def test_normalize_capabilities_sorts_alphabetically():
    assert normalize_capabilities(["zeta", "alpha", "beta"]) == ["alpha", "beta", "zeta"]


def test_normalize_capabilities_lowercases_values():
    # Capabilities are case-insensitive and normalized to lowercase
    result = normalize_capabilities(["GPU", "gpu"])
    assert "gpu" in result


def test_normalize_capabilities_invariants_hold_for_varied_inputs():
    inputs = [
        ["gpu", "GPU", " gpu ", ""],
        ["  alpha", "beta  ", "alpha", "BETA"],
        ["x", "y", "x", "z", " y "],
        ["  ", "", "single"],
    ]

    for raw in inputs:
        normalized = normalize_capabilities(raw)
        assert normalized == sorted(normalized)
        assert len(normalized) == len(set(normalized))
        assert all(item == item.strip().lower() for item in normalized)
        assert normalize_capabilities(normalized) == normalized


def test_normalize_hostname_lowercases_name_and_location():
    name, location = normalize_hostname(" WorkerA@FactoryB ")
    assert name == "workera"
    assert location == "factoryb"


def test_normalize_hostname_requires_name_and_location():
    with pytest.raises(ValueError, match="name@location"):
        normalize_hostname("worker-without-location")
    with pytest.raises(ValueError, match="name@location"):
        normalize_hostname("worker@")
