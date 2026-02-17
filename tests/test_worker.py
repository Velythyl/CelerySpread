import time

from celery.contrib.testing.worker import start_worker

from celeryspread.worker import (
    REQUIRED_CAPS_ATTR,
    SKIP_REASON_ATTR,
    SKIPPED_REGISTRATION_ATTR,
    Worker,
    get_task_registration_diagnostics,
    task_spec,
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


def test_task_spec_marks_required_capabilities():
    @task_spec(capabilities=["FactoryB", "gpu"])
    def process_video(video_id: str):
        return video_id

    assert getattr(process_video, REQUIRED_CAPS_ATTR) == ["FactoryB", "gpu"]


def test_worker_task_registers_when_capabilities_match(celery_app):
    worker = Worker(
        app=celery_app,
        name="worker1",
        location="FactoryB",
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

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="worker1@FactoryB",
        queues=["default"],
    ):
        result = process_video.delay("vid-1")
        assert result.get(timeout=5) == "vid-1"


def test_worker_subscribes_to_single_capability_queues(celery_app):
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="worker1@FactoryB",
        queues=["default"],
    ):
        worker = Worker(
            app=celery_app,
            name="worker1",
            location="FactoryB",
            capabilities=["FactoryB", "gpu", "arm"],
        )
        assert set(worker.queues) == {"FactoryB", "arm", "gpu"}
        assert _wait_for_worker_queues(celery_app, worker.id, set(worker.queues))


def test_worker_task_skips_registration_when_capabilities_do_not_match(celery_app):
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="worker1@FactoryB",
        queues=["default"],
    ):
        worker = Worker(
            app=celery_app,
            name="worker1",
            location="FactoryB",
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
        assert diagnostics["required_capabilities"] == ["FactoryB", "gpu"]
