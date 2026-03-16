import time

import pytest

from celeryspread.constants import (
    REQUIRED_CAPS_ATTR,
    SKIP_REASON_ATTR,
    SKIPPED_REGISTRATION_ATTR,
    WORKER_CAPS_ATTR,
)
from celeryspread.task_spec import task_spec
from celeryspread.worker import (
    Worker,
    get_task_registration_diagnostics,
)


def _wait_for_worker_queues(app, worker_id: str, expected: set[str], timeout: float = 3.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        active = app.control.inspect(timeout=1.0).active_queues() or {}
        queues = {entry["name"] for entry in active.get(worker_id, [])}
        if expected.issubset(queues):
            return True
        time.sleep(0.1)
    return False


# --- task_spec tests ---


def test_task_spec_marks_required_capabilities():
    @task_spec(capabilities=["FactoryB", "gpu"])
    def process_video(video_id: str):
        return video_id

    assert getattr(process_video, REQUIRED_CAPS_ATTR) == {"factoryb", "gpu"}


def test_task_spec_as_bare_decorator_defaults_to_empty_requirements():
    @task_spec
    def simple_task(x: int):
        return x

    assert getattr(simple_task, REQUIRED_CAPS_ATTR) == set()
    assert getattr(simple_task, SKIPPED_REGISTRATION_ATTR) is False


def test_task_spec_with_empty_positional_capabilities():
    @task_spec([])
    def simple_task(x: int):
        return x

    assert getattr(simple_task, REQUIRED_CAPS_ATTR) == set()


def test_task_spec_with_positional_capabilities():
    @task_spec(["factory A"])
    def tagged_task(x: int):
        return x

    assert getattr(tagged_task, REQUIRED_CAPS_ATTR) == {"factory a"}


def test_task_spec_with_empty_capabilities():
    """Task with no requirements should be registrable on any worker."""
    @task_spec(capabilities=[])
    def simple_task(x: int):
        return x

    assert getattr(simple_task, REQUIRED_CAPS_ATTR) == set()
    assert getattr(simple_task, SKIPPED_REGISTRATION_ATTR) is False


def test_task_spec_normalizes_capabilities():
    """Capabilities should be normalized (stripped, deduped, sorted)."""
    @task_spec(capabilities=["  gpu  ", "FactoryB", "gpu"])
    def process(x: int):
        return x

    caps = getattr(process, REQUIRED_CAPS_ATTR)
    assert caps == {"factoryb", "gpu"}


def test_task_spec_capabilities_are_sets():
    """Capabilities should be stored as sets, not lists."""
    @task_spec(capabilities=["alpha", "beta"])
    def my_task():
        pass

    caps = getattr(my_task, REQUIRED_CAPS_ATTR)
    assert isinstance(caps, set)


def test_task_spec_preserves_function_metadata():
    """task_spec should preserve function name and docstring."""
    @task_spec(capabilities=["gpu"])
    def my_function(x: int) -> int:
        """A docstring."""
        return x

    assert my_function.__name__ == "my_function"
    assert my_function.__doc__ == "A docstring."


# --- Worker capability tests ---


def test_worker_capabilities_are_sets(celery_app):
    """Worker capabilities should be stored as sets."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu", "arm"],
    )
    assert isinstance(worker.capabilities, set)


def test_worker_capabilities_include_location_from_hostname(celery_app):
    """Hostname is metadata only and should not affect capabilities."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu"],
    )
    assert "gpu" in worker.capabilities
    assert "factoryb" not in worker.capabilities


def test_worker_capabilities_with_none(celery_app):
    """Worker with capabilities=None should have no capabilities."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@datacenter1",
        capabilities=None,
    )
    assert worker.capabilities == set()


def test_worker_capabilities_deduplicate(celery_app):
    """Duplicate capabilities should be deduplicated."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["FactoryB", "gpu", "gpu"],
    )
    assert worker.capabilities == {"factoryb", "gpu"}


def test_worker_capabilities_property_returns_copy(celery_app):
    """Modifying returned capabilities should not affect worker state."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu"],
    )
    caps = worker.capabilities
    caps.add("hacked")
    assert "hacked" not in worker.capabilities


def test_worker_hostname_empty_raises_value_error(celery_app):
    with pytest.raises(ValueError, match="non-empty"):
        Worker(
            app=celery_app,
            hostname="   ",
            capabilities=["gpu"],
        )


def test_worker_hostname_keeps_original_format(celery_app):
    worker = Worker(
        app=celery_app,
        hostname="worker1@factory@zone",
        capabilities=["gpu"],
    )

    assert worker.hostname == "worker1@factory@zone"


def test_worker_hostname_preserves_case(celery_app):
    worker = Worker(
        app=celery_app,
        hostname="WorkerName@FactoryB",
        capabilities=["gpu"],
    )

    assert worker.hostname == "WorkerName@FactoryB"


# --- Worker task registration tests ---


def test_worker_task_registers_when_capabilities_match(celery_app, run_worker):
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["FactoryB", "gpu"],
    )
    before_tasks = set(celery_app.tasks.keys())

    @worker.task(bind=True)
    @task_spec(capabilities=["FactoryB", "gpu"])
    def process_video(self, video_id: str):
        return video_id

    assert process_video.name in celery_app.tasks
    assert process_video.name not in before_tasks
    assert getattr(process_video.run, SKIPPED_REGISTRATION_ATTR) is False
    diagnostics = get_task_registration_diagnostics(process_video.run)
    assert diagnostics["skipped"] is False
    assert diagnostics["reason"] is None

    with run_worker(
        worker.hostname,
        capabilities=["FactoryB", "gpu"],
        include_default_queue=True,
    ):
        result = process_video.delay("vid-1")
        assert result.get(timeout=20) == "vid-1"


def test_worker_subscribes_to_single_capability_queues(celery_app):
    """Test that Worker computes correct queue names from capabilities."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["FactoryB", "gpu", "arm"],
    )
    # Worker subscribes to capability queues + app default + celeryspread + random self queue
    queue_set = set(worker.queues)
    assert {"factoryb", "arm", "gpu"}.issubset(queue_set)
    assert celery_app.conf.task_default_queue in queue_set
    assert "celeryspread" in queue_set
    assert worker.self_queue_name in queue_set
    assert worker.self_queue_name.startswith("celeryspread.self.")


def test_worker_queues_actually_subscribed_at_runtime(celery_app, run_worker):
    """Integration test: verify queues are actually subscribed when worker runs."""
    # Start worker with the computed queues directly
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["FactoryB", "gpu", "arm"],
    )
    with run_worker(worker.hostname, capabilities=["FactoryB", "gpu", "arm"]) as runtime_worker:
        assert _wait_for_worker_queues(celery_app, runtime_worker.hostname, set(runtime_worker.queues))


def test_worker_can_enqueue_task_to_its_own_self_queue(celery_app, run_worker):
    """A task running on a worker should be able to enqueue work onto that worker's self queue."""
    worker = Worker(
        app=celery_app,
        hostname="worker-self@FactoryB",
        capabilities=["FactoryB", "gpu"],
    )

    @worker.task(bind=True, name="tasks.self_queue_child")
    def self_queue_child(self, value: str) -> str:
        return f"child:{value}"

    @worker.task(bind=True, name="tasks.self_queue_parent")
    def self_queue_parent(self, value: str, self_queue_name: str) -> str:
        child_result = self.app.send_task(
            "tasks.self_queue_child",
            args=(value,),
            queue=self_queue_name,
        )
        return child_result.id

    with run_worker(worker.hostname, capabilities=["FactoryB", "gpu"]) as runtime_worker:
        parent_result = self_queue_parent.delay("ok", runtime_worker.self_queue_name)
        child_task_id = parent_result.get(timeout=20)
        child_result = celery_app.AsyncResult(child_task_id)
        assert child_result.get(timeout=20) == "child:ok"


def test_worker_task_skips_registration_when_capabilities_do_not_match(celery_app):
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["FactoryB", "cpu"],
    )
    before_tasks = set(celery_app.tasks.keys())

    @worker.task(bind=True)
    @task_spec(capabilities=["FactoryB", "gpu"])
    def process_video(video_id: str):
        return video_id

    assert set(celery_app.tasks.keys()) == before_tasks
    assert getattr(process_video, SKIPPED_REGISTRATION_ATTR) is True
    assert "gpu" in getattr(process_video, SKIP_REASON_ATTR)
    diagnostics = get_task_registration_diagnostics(process_video)
    assert diagnostics["skipped"] is True
    assert diagnostics["required_capabilities"] == {"factoryb", "gpu"}


def test_worker_task_skip_logs_warning(celery_app, caplog):
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["cpu"],
    )

    with caplog.at_level("WARNING"):
        @worker.task(bind=True)
        @task_spec(capabilities=["gpu"])
        def process_video(video_id: str):
            return video_id

    assert getattr(process_video, SKIPPED_REGISTRATION_ATTR) is True
    assert "Task registration skipped" in caplog.text


def test_worker_task_registers_when_empty_task_spec(celery_app):
    """Task with no requirements should register on any worker."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu"],
    )
    before_tasks = set(celery_app.tasks.keys())

    @worker.task(bind=True)
    @task_spec(capabilities=[])
    def universal_task(self, x: int):
        return x * 2

    assert universal_task.name in celery_app.tasks
    assert universal_task.name not in before_tasks
    assert getattr(universal_task.run, SKIPPED_REGISTRATION_ATTR) is False


def test_worker_task_registers_without_task_spec(celery_app):
    """Task without task_spec decorator should register normally."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu"],
    )

    @worker.task(bind=True)
    def unrestricted_task(self, x: int):
        return x + 1

    assert unrestricted_task.name in celery_app.tasks
    assert getattr(unrestricted_task.run, SKIPPED_REGISTRATION_ATTR) is False


def test_worker_task_partial_capability_match_skips(celery_app):
    """Worker with only some required capabilities should skip registration."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu"],  # Has gpu but not arm
    )

    @worker.task(bind=True)
    @task_spec(capabilities=["gpu", "arm"])
    def specialized_task(self, x: int):
        return x

    assert getattr(specialized_task, SKIPPED_REGISTRATION_ATTR) is True
    skip_reason = getattr(specialized_task, SKIP_REASON_ATTR)
    assert "arm" in skip_reason


def test_worker_task_superset_capabilities_registers(celery_app):
    """Worker with more capabilities than required should still register."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu", "arm", "extra"],
    )

    @worker.task(bind=True)
    @task_spec(capabilities=["gpu"])
    def simple_gpu_task(self, x: int):
        return x

    assert simple_gpu_task.name in celery_app.tasks
    assert getattr(simple_gpu_task.run, SKIPPED_REGISTRATION_ATTR) is False


def test_worker_stores_capabilities_on_registered_task(celery_app):
    """Registered task should have worker capabilities stored."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["gpu", "arm"],
    )

    @worker.task(bind=True)
    @task_spec(capabilities=["gpu"])
    def my_task(self):
        pass

    worker_caps = getattr(my_task.run, WORKER_CAPS_ATTR)
    assert isinstance(worker_caps, set)
    assert "gpu" in worker_caps
    assert "arm" in worker_caps


def test_worker_stores_capabilities_on_skipped_task(celery_app):
    """Even skipped tasks should have worker capabilities stored."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["cpu"],
    )

    @worker.task(bind=True)
    @task_spec(capabilities=["gpu"])
    def skipped_task(self):
        pass

    worker_caps = getattr(skipped_task, WORKER_CAPS_ATTR)
    assert isinstance(worker_caps, set)
    assert "cpu" in worker_caps


def test_get_task_registration_diagnostics_complete(celery_app):
    """Diagnostics should return all expected fields."""
    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["cpu"],
    )

    @worker.task(bind=True)
    @task_spec(capabilities=["gpu", "arm"])
    def complex_task(self):
        pass

    diagnostics = get_task_registration_diagnostics(complex_task)
    assert "required_capabilities" in diagnostics
    assert "worker_capabilities" in diagnostics
    assert "skipped" in diagnostics
    assert "reason" in diagnostics
    assert diagnostics["skipped"] is True
    assert diagnostics["required_capabilities"] == {"gpu", "arm"}


def test_worker_task_runs_on_configured_default_queue(celery_app, run_worker):
    celery_app.conf.task_default_queue = "primary"

    worker = Worker(
        app=celery_app,
        hostname="worker1@FactoryB",
        capabilities=["FactoryB", "gpu"],
    )

    @worker.task(bind=True)
    @task_spec(capabilities=["FactoryB", "gpu"])
    def process_video(self, video_id: str):
        return video_id

    with run_worker(
        worker.hostname,
        capabilities=["FactoryB", "gpu"],
        include_default_queue=True,
    ):
        result = process_video.delay("vid-primary")
        assert result.get(timeout=20) == "vid-primary"
