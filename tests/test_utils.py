from celeryspread.utils import capabilities_to_queue_name, generate_worker_id


def test_capabilities_to_queue_name_sorts_and_deduplicates():
    assert capabilities_to_queue_name(["gpu", "FactoryB", "gpu"]) == "FactoryB.gpu"


def test_generate_worker_id():
    assert generate_worker_id()
