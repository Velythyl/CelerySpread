from unittest.mock import MagicMock

from celery.contrib.testing.worker import start_worker

from celeryspread.tasks import find_matching_workers, register_awakening_task
from celeryspread.worker import Worker


# --- find_matching_workers tests ---


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
        worker1 = Worker(app=celery_app, hostname="worker1@factory_b", capabilities=["FactoryB", "gpu"])
        worker2 = Worker(app=celery_app, hostname="worker2@cloud", capabilities=["cloud"])
        inspector = celery_app.control.inspect(timeout=2)
        workers = find_matching_workers(inspector, ["FactoryB", "gpu"])
        assert workers == ["worker1@factory_b"]


def test_find_matching_workers_empty_requirements():
    """Empty requirements should match all workers."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = {
        "worker1@loc1": [{"name": "gpu"}],
        "worker2@loc2": [{"name": "cpu"}],
    }
    workers = find_matching_workers(mock_inspector, [])
    assert set(workers) == {"worker1@loc1", "worker2@loc2"}


def test_find_matching_workers_single_requirement():
    """Single requirement should match workers subscribed to that queue."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = {
        "worker1@loc1": [{"name": "gpu"}, {"name": "FactoryB"}],
        "worker2@loc2": [{"name": "cpu"}],
        "worker3@loc3": [{"name": "gpu"}],
    }
    workers = find_matching_workers(mock_inspector, ["gpu"])
    assert set(workers) == {"worker1@loc1", "worker3@loc3"}


def test_find_matching_workers_no_active_queues():
    """Should return empty list when no active queues."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = None
    workers = find_matching_workers(mock_inspector, ["gpu"])
    assert workers == []


def test_find_matching_workers_no_matching_workers():
    """Should return empty list when no workers match."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = {
        "worker1@loc1": [{"name": "cpu"}],
        "worker2@loc2": [{"name": "arm"}],
    }
    workers = find_matching_workers(mock_inspector, ["gpu"])
    assert workers == []


def test_find_matching_workers_multiple_workers_match():
    """Multiple workers can match the same requirements."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = {
        "worker1@loc1": [{"name": "gpu"}, {"name": "FactoryB"}],
        "worker2@loc2": [{"name": "gpu"}, {"name": "FactoryB"}, {"name": "extra"}],
        "worker3@loc3": [{"name": "cpu"}],
    }
    workers = find_matching_workers(mock_inspector, ["gpu", "FactoryB"])
    assert set(workers) == {"worker1@loc1", "worker2@loc2"}


def test_find_matching_workers_partial_match_excluded():
    """Worker with only some required queues should not match."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = {
        "worker1@loc1": [{"name": "gpu"}],  # Missing FactoryB
        "worker2@loc2": [{"name": "gpu"}, {"name": "FactoryB"}],
    }
    workers = find_matching_workers(mock_inspector, ["gpu", "FactoryB"])
    assert workers == ["worker2@loc2"]


def test_find_matching_workers_superset_queues_match():
    """Worker with superset of required queues should match."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = {
        "worker1@loc1": [{"name": "gpu"}, {"name": "FactoryB"}, {"name": "arm"}, {"name": "extra"}],
    }
    workers = find_matching_workers(mock_inspector, ["gpu", "FactoryB"])
    assert workers == ["worker1@loc1"]


def test_find_matching_workers_normalizes_requirements():
    """Requirements should be normalized (whitespace stripped)."""
    mock_inspector = MagicMock()
    mock_inspector.active_queues.return_value = {
        "worker1@loc1": [{"name": "gpu"}, {"name": "FactoryB"}],
    }
    workers = find_matching_workers(mock_inspector, ["  gpu  ", "FactoryB"])
    assert workers == ["worker1@loc1"]


# --- register_awakening_task tests ---


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
        worker1 = Worker(app=celery_app, hostname="worker1@factory_b", capabilities=["FactoryB", "gpu"])
        worker2 = Worker(app=celery_app, hostname="worker2@factory_b", capabilities=["FactoryB"])
        result = task.delay("FactoryB.gpu", ["FactoryB", "gpu"]).get(timeout=10)

    assert result["status"] == "awakened"
    assert result["queue"] == "FactoryB.gpu"
    assert result["workers"] == ["worker1@factory_b"]


def test_register_awakening_task_no_matching_workers(celery_app):
    """When no workers match, should return no_matching_workers status."""
    notifications = []

    def notify(msg):
        notifications.append(msg)

    task = register_awakening_task(celery_app, notify_admin=notify, inspect_timeout=1.5)

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="worker1@factory_b",
        queues=["default", "cpu"],  # No gpu capability
    ):
        worker = Worker(app=celery_app, hostname="worker1@factory_b", capabilities=["cpu"])
        result = task.delay("gpu.arm", ["gpu", "arm"]).get(timeout=10)

    assert result["status"] == "no_matching_workers"
    assert result["queue"] == "gpu.arm"
    # Note: notify_admin callback runs in worker process, not test process
    # Use the eager execution test below to verify callback behavior


def test_register_awakening_task_no_matching_workers_notify_callback(celery_app):
    """Verify notify_admin callback is called when no workers match (eager mode)."""
    notifications = []

    def notify(msg):
        notifications.append(msg)

    # Use eager execution to test callback in same process
    celery_app.conf.task_always_eager = True

    task = register_awakening_task(celery_app, notify_admin=notify, inspect_timeout=0.1)
    result = task.delay("gpu.arm", ["gpu", "arm"]).get(timeout=10)

    celery_app.conf.task_always_eager = False

    assert result["status"] == "no_matching_workers"
    assert result["queue"] == "gpu.arm"
    assert len(notifications) == 1
    assert "gpu.arm" in notifications[0]


def test_register_awakening_task_logs_warning_when_no_matching_workers(celery_app, caplog):
    celery_app.conf.task_always_eager = True

    task = register_awakening_task(celery_app, inspect_timeout=0.1)
    with caplog.at_level("WARNING"):
        result = task.delay("gpu.arm", ["gpu", "arm"]).get(timeout=10)

    celery_app.conf.task_always_eager = False

    assert result["status"] == "no_matching_workers"
    assert "No workers available for queue 'gpu.arm'" in caplog.text


def test_register_awakening_task_without_notify_admin(celery_app):
    """When notify_admin is None, no exception should be raised."""
    task = register_awakening_task(celery_app, notify_admin=None, inspect_timeout=1.5)

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="worker1@factory_b",
        queues=["default"],
    ):
        worker = Worker(app=celery_app, hostname="worker1@factory_b", capabilities=[])
        result = task.delay("nonexistent.queue", ["nonexistent"]).get(timeout=10)

    assert result["status"] == "no_matching_workers"


def test_register_awakening_task_single_requirement(celery_app):
    """Awakening with single requirement should still work."""
    task = register_awakening_task(celery_app, inspect_timeout=1.5)

    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="worker1@factory_b",
        queues=["default", "gpu"],
    ):
        worker = Worker(app=celery_app, hostname="worker1@factory_b", capabilities=["gpu"])
        result = task.delay("gpu", ["gpu"]).get(timeout=10)

    assert result["status"] == "awakened"
    assert result["workers"] == ["worker1@factory_b"]


def test_register_awakening_task_awakens_multiple_workers(celery_app):
    """Multiple matching workers should all be awakened."""
    task = register_awakening_task(celery_app, inspect_timeout=1.5)

    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="worker1@factory_b",
        queues=["default", "gpu", "FactoryB"],
    ), start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="worker2@factory_b",
        queues=["default", "gpu", "FactoryB"],
    ):
        worker1 = Worker(app=celery_app, hostname="worker1@factory_b", capabilities=["gpu", "FactoryB"])
        worker2 = Worker(app=celery_app, hostname="worker2@factory_b", capabilities=["gpu", "FactoryB"])
        result = task.delay("FactoryB.gpu", ["FactoryB", "gpu"]).get(timeout=10)

    assert result["status"] == "awakened"
    assert set(result["workers"]) == {"worker1@factory_b", "worker2@factory_b"}


def test_register_awakening_task_idempotent_registration(celery_app):
    """Calling register_awakening_task multiple times should be safe."""
    task1 = register_awakening_task(celery_app, inspect_timeout=1.5)
    task2 = register_awakening_task(celery_app, inspect_timeout=1.5)

    assert "celeryspread.tasks.awaken_complex_queue_workers" in celery_app.tasks
    # Both should return a valid task
    assert task1 is not None
    assert task2 is not None
