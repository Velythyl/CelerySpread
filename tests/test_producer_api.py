from celery.contrib.testing.worker import start_worker

from celeryspread.producer_api import configure, send_task
from celeryspread.tasks import register_awakening_task


def test_send_task_default_queue_when_empty_requirements(celery_app):
    @celery_app.task(name="tasks.ping")
    def ping() -> str:
        return "pong"

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-default@local",
        queues=["default"],
    ):
        result = send_task("tasks.ping", requirements=[])
        assert result.get(timeout=5) == "pong"


def test_send_task_simple_requirement_routes_directly(celery_app):
    @celery_app.task(name="tasks.inference")
    def inference() -> str:
        return "gpu-ok"

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-gpu@local",
        queues=["gpu"],
    ):
        result = send_task("tasks.inference", requirements=["gpu"])
        assert result.get(timeout=5) == "gpu-ok"


def test_send_task_complex_requirement_triggers_awakening(celery_app):
    register_awakening_task(celery_app, inspect_timeout=1.5)

    @celery_app.task(name="tasks.process_video")
    def process_video() -> str:
        return "processed"

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="producer-complex@FactoryB",
        queues=["default", "FactoryB", "gpu"],
    ):
        result = send_task("tasks.process_video", requirements=["FactoryB", "gpu"])
        assert result.get(timeout=5) == "processed"
