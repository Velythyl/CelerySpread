import pytest
from celery.contrib.testing.worker import start_worker

from celeryspread import CeleryEntity
from celeryspread.constants import CSR, SKIPPED_REGISTRATION_ATTR
from celeryspread.producer import Producer
from celeryspread.task_spec import task_spec
from celeryspread.worker import Worker


def test_celery_entity_worker_false_returns_producer(celery_app):
    entity = CeleryEntity(app=celery_app, worker=False)

    assert isinstance(entity, Producer)
    assert not isinstance(entity, Worker)


def test_celery_entity_worker_true_returns_worker(celery_app):
    entity = CeleryEntity(
        app=celery_app,
        worker=True,
        hostname="worker1@factoryb",
        capabilities=["gpu"],
    )

    assert isinstance(entity, Worker)
    assert "factoryb" in entity.capabilities


def test_celery_entity_requires_explicit_worker_mode(celery_app):
    with pytest.raises(TypeError, match="worker=True"):
        CeleryEntity(app=celery_app)


def test_celery_entity_producer_path_always_registers_task(celery_app):
    app_entity = CeleryEntity(app=celery_app, worker=False)

    @app_entity.task(name="tasks.entity_producer_registers")
    @task_spec(capabilities=["gpu"])
    def my_task() -> str:
        return "ok"

    assert my_task.name in celery_app.tasks


def test_celery_entity_worker_path_keeps_skip_behavior(celery_app):
    app_entity = CeleryEntity(
        app=celery_app,
        worker=True,
        hostname="worker1@factoryb",
        capabilities=["cpu"],
    )

    before_tasks = set(celery_app.tasks.keys())

    @app_entity.task(name="tasks.entity_worker_skip")
    @task_spec(capabilities=["gpu"])
    def my_task() -> str:
        return "ok"

    assert set(celery_app.tasks.keys()) == before_tasks
    assert getattr(my_task, SKIPPED_REGISTRATION_ATTR) is True


def test_celery_entity_producer_task_registers_and_executes(celery_app):
    producer = CeleryEntity(app=celery_app, worker=False)

    @producer.task(name="tasks.entity_producer_add")
    def add(x: int, y: int) -> int:
        return x + y

    assert add.name in celery_app.tasks

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="entity-producer-add@local",
        queues=["default"],
    ):
        worker = Worker(app=celery_app, hostname="entity-producer-add@local", capabilities=[])
        result = add.delay(2, 3)
        assert result.get(timeout=10) == 5


def test_celery_entity_producer_task_delay_uses_csr_routing(celery_app):
    producer = CeleryEntity(app=celery_app, worker=False)

    @producer.task(name="tasks.entity_producer_gpu")
    def gpu_task(value: int) -> int:
        return value * 2

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="entity-producer-gpu@local",
        queues=["gpu"],
    ):
        worker = Worker(app=celery_app, hostname="entity-producer-gpu@local", capabilities=["gpu"])
        result = gpu_task.delay(21, **{CSR: ["gpu"]})
        assert result.get(timeout=10) == 42
