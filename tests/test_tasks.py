from celery.contrib.testing.worker import start_worker

from celeryspread.tasks import find_matching_workers, register_awakening_task


def test_find_matching_workers_filters_correctly(celery_app):
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="worker1@factory_b",
        queues=["FactoryB", "gpu"],
    ), start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="worker2@cloud",
        queues=["cloud"],
    ):
        inspector = celery_app.control.inspect(timeout=2)
        workers = find_matching_workers(inspector, ["FactoryB", "gpu"])
        assert workers == ["worker1@factory_b"]


def test_register_awakening_task_registers_and_awakens_workers(celery_app):
    task = register_awakening_task(celery_app, inspect_timeout=1.5)

    assert "celeryspread.tasks.awaken_complex_queue_workers" in celery_app.tasks

    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="worker1@factory_b",
        queues=["default", "FactoryB", "gpu"],
    ), start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="worker2@factory_b",
        queues=["default", "FactoryB"],
    ):
        result = task.delay("FactoryB.gpu", ["FactoryB", "gpu"]).get(timeout=5)

    assert result["status"] == "awakened"
    assert result["queue"] == "FactoryB.gpu"
    assert result["workers"] == ["worker1@factory_b"]
