from concurrent.futures import ThreadPoolExecutor

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker

from celeryspread.constants import CSR, REQUIRED_CAPS_ATTR
from celeryspread.producer import Producer, configure, send_task, _require_app, _is_simple_requirement
from celeryspread.task_spec import task_spec
from celeryspread.tasks import register_awakening_task
from celeryspread.worker import (
    Worker,
    get_task_registration_diagnostics,
)



# --- Configuration tests ---


def test_require_app_raises_when_not_configured():
    """Should raise RuntimeError when celeryspread is not configured."""
    with pytest.raises(RuntimeError, match="not configured"):
        _require_app()


def test_configure_sets_app(celery_app):
    """configure() should set the app for send_task to use."""
    configure(celery_app)
    assert _require_app() is celery_app


def test_configure_concurrent_calls_do_not_crash():
    app_one = Celery("concurrent-one", broker="memory://", backend="cache+memory://")
    app_two = Celery("concurrent-two", broker="memory://", backend="cache+memory://")

    def configure_many(app: Celery) -> None:
        for _ in range(1000):
            configure(app)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(configure_many, app_one),
            pool.submit(configure_many, app_two),
        ]
        for future in futures:
            future.result()

    assert _require_app() in {app_one, app_two}


# --- _is_simple_requirement tests ---


def test_is_simple_requirement_single():
    assert _is_simple_requirement(["gpu"]) is True


def test_is_simple_requirement_multiple():
    assert _is_simple_requirement(["gpu", "FactoryB"]) is False


def test_is_simple_requirement_empty():
    assert _is_simple_requirement([]) is False


# --- send_task tests ---


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
        assert result.get(timeout=10) == "pong"


def test_send_task_default_queue_when_none_requirements(celery_app):
    """requirements=None should route to default queue."""
    @celery_app.task(name="tasks.ping_none")
    def ping_none() -> str:
        return "pong-none"

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-none@local",
        queues=["default"],
    ):
        result = send_task("tasks.ping_none", requirements=None)
        assert result.get(timeout=10) == "pong-none"


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
        assert result.get(timeout=10) == "gpu-ok"


def test_send_task_simple_requirement_normalizes_input(celery_app):
    """Requirements should be normalized (whitespace stripped)."""
    @celery_app.task(name="tasks.inference_normalize")
    def inference() -> str:
        return "normalized"

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-normalize@local",
        queues=["gpu"],
    ):
        result = send_task("tasks.inference_normalize", requirements=["  gpu  "])
        assert result.get(timeout=10) == "normalized"


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
        assert result.get(timeout=10) == "processed"


def test_send_task_with_args_and_kwargs(celery_app):
    """send_task should pass args and kwargs correctly."""
    @celery_app.task(name="tasks.add_with_kwargs")
    def add(x: int, y: int, multiplier: int = 1) -> int:
        return (x + y) * multiplier

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-args@local",
        queues=["default"],
    ):
        result = send_task(
            "tasks.add_with_kwargs",
            args=(3, 4),
            kwargs={"multiplier": 2},
        )
        assert result.get(timeout=10) == 14


# --- Producer class tests ---


def test_producer_task_registers_with_capability_metadata(celery_app):
    producer = Producer(celery_app)

    @producer.task(name="tasks.annotate")
    @task_spec(capabilities=["FactoryB", "gpu"])
    def annotate(payload: str) -> str:
        return payload

    assert annotate.name in celery_app.tasks
    assert getattr(annotate.run, REQUIRED_CAPS_ATTR) == {"factoryb", "gpu"}


def test_producer_task_without_task_spec(celery_app):
    """Producer task without task_spec should work normally."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.simple_task")
    def simple_task(x: int) -> int:
        return x * 2

    assert simple_task.name in celery_app.tasks

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-simple@local",
        queues=["default"],
    ):
        result = simple_task.delay(5)
        assert result.get(timeout=10) == 10


def test_producer_task_delay_routes_to_default_queue_without_requirements(celery_app):
    """Per spec: if no additional requirements specified, task goes to default queue.
    task_spec only affects which workers register the task, not routing."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.render")
    @task_spec(capabilities=["FactoryB", "gpu"])
    def render(frame_id: str) -> str:
        return frame_id

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-delay@FactoryB",
        queues=["default"],
    ):
        result = render.delay("frame-42")
        assert result.get(timeout=10) == "frame-42"


def test_producer_task_delay_routes_using_requirements_kwarg(celery_app):
    producer = Producer(celery_app)

    @producer.task(name="tasks.add")
    def add(x: int, y: int) -> int:
        return x + y

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-kwargs@gpu",
        queues=["gpu"],
    ):
        result = add.delay(4, 4, CSR=["gpu"])
        assert result.get(timeout=10) == 8


def test_producer_task_delay_requirements_kwarg_triggers_complex_awakening(celery_app):
    register_awakening_task(celery_app, inspect_timeout=1.5)
    producer = Producer(celery_app)

    @producer.task(name="tasks.render_with_kwargs")
    def render(frame_id: str) -> str:
        return frame_id

    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="producer-kwargs-complex@FactoryB",
        queues=["default", "FactoryB", "gpu"],
    ):
        result = render.delay("frame-77", CSR=["FactoryB", "gpu"])
        assert result.get(timeout=10) == "frame-77"


def test_producer_task_apply_async_with_explicit_queue_bypasses_routing(celery_app):
    """When explicit queue is provided in options, routing should be bypassed."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.explicit_queue_task")
    def my_task(x: int) -> int:
        return x * 3

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-explicit@local",
        queues=["custom_queue"],
    ):
        # Explicit queue should bypass CSR routing
        result = my_task.apply_async(args=(7,), queue="custom_queue")
        assert result.get(timeout=10) == 21


def test_producer_task_apply_async_with_csr_in_kwargs(celery_app):
    """CSR should be extracted from kwargs and used for routing."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.apply_async_csr")
    def compute(x: int) -> int:
        return x + 10

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-async-csr@local",
        queues=["gpu"],
    ):
        result = my_task = compute.apply_async(args=(5,), kwargs={CSR: ["gpu"]})
        assert result.get(timeout=10) == 15


def test_producer_task_csr_not_passed_to_function(celery_app):
    """CSR kwarg should be stripped and not passed to the actual function."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.csr_stripped")
    def my_func(**kwargs) -> dict:
        return kwargs

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-strip@local",
        queues=["gpu"],
    ):
        result = my_func.delay(other_arg="value", CSR=["gpu"])
        returned_kwargs = result.get(timeout=10)
        assert CSR not in returned_kwargs
        assert returned_kwargs == {"other_arg": "value"}


def test_producer_task_with_task_spec_and_csr_combined(celery_app):
    """task_spec affects worker registration, CSR affects routing."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.combined_spec_csr")
    @task_spec(capabilities=["FactoryB", "gpu"])
    def specialized_compute(x: int) -> int:
        return x * 4

    # CSR=["arm"] routes to arm queue, but task_spec requires FactoryB+gpu on worker
    # For this test, we just verify CSR routes correctly (worker registration is separate)
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-combined@local",
        queues=["arm"],
    ):
        result = specialized_compute.delay(3, CSR=["arm"])
        assert result.get(timeout=10) == 12


def test_producer_task_csr_normalizes_requirements(celery_app):
    """CSR requirements should be normalized."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.csr_normalize")
    def task_norm(x: int) -> int:
        return x

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-csr-norm@local",
        queues=["gpu"],
    ):
        result = task_norm.delay(42, CSR=["  gpu  "])
        assert result.get(timeout=10) == 42


def test_producer_task_csr_empty_list_routes_to_default(celery_app):
    """CSR=[] should route to default queue."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.csr_empty")
    def task_empty(x: int) -> int:
        return x + 1

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-csr-empty@local",
        queues=["default"],
    ):
        result = task_empty.delay(10, CSR=[])
        assert result.get(timeout=10) == 11


def test_producer_task_multiple_csr_requirements_triggers_awakening(celery_app):
    """Multiple CSR requirements should trigger the awakening mechanism."""
    register_awakening_task(celery_app, inspect_timeout=1.5)
    producer = Producer(celery_app)

    @producer.task(name="tasks.multi_csr")
    def multi_req_task(data: str) -> str:
        return data.upper()

    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="producer-multi-csr@local",
        queues=["default", "gpu", "arm", "FactoryB"],
    ):
        result = multi_req_task.delay("hello", CSR=["gpu", "arm", "FactoryB"])
        assert result.get(timeout=10) == "HELLO"


def test_producer_stores_task_spec_metadata(celery_app):
    """Producer should store task_spec metadata on the task class."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.metadata_check")
    @task_spec(capabilities=["gpu", "FactoryB"])
    def metadata_task():
        pass

    # Check the task has the task_spec stored
    task_class = type(celery_app.tasks["tasks.metadata_check"])
    assert hasattr(task_class, "_celeryspread_task_spec")
    assert task_class._celeryspread_task_spec == {"gpu", "factoryb"}


def test_producer_task_apply_async_with_other_options(celery_app):
    """apply_async options besides queue should be passed through."""
    producer = Producer(celery_app)

    @producer.task(name="tasks.with_options")
    def task_with_opts(x: int) -> int:
        return x

    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-opts@local",
        queues=["default"],
    ):
        # countdown and other options should work
        result = task_with_opts.apply_async(args=(99,), countdown=0)
        assert result.get(timeout=10) == 99


def test_send_task_queue_name_deterministic(celery_app):
    """Queue name for requirements should be deterministic (sorted, deduped)."""
    @celery_app.task(name="tasks.queue_name_test")
    def queue_test() -> str:
        return "ok"

    configure(celery_app)

    # Both orderings should produce the same queue name
    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="producer-qname@local",
        queues=["default", "factoryb.gpu"],
    ):
        register_awakening_task(celery_app, inspect_timeout=1.5)
        # Order 1
        result1 = send_task("tasks.queue_name_test", requirements=["gpu", "FactoryB"])
        # Order 2
        result2 = send_task("tasks.queue_name_test", requirements=["FactoryB", "gpu"])

        assert result1.get(timeout=10) == "ok"
        assert result2.get(timeout=10) == "ok"


def test_send_task_routes_to_configured_default_queue(celery_app):
    celery_app.conf.task_default_queue = "primary"

    @celery_app.task(name="tasks.ping_primary")
    def ping_primary() -> str:
        return "pong-primary"

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        hostname="producer-primary@local",
        queues=["primary"],
    ):
        result = send_task("tasks.ping_primary", requirements=[])
        assert result.get(timeout=10) == "pong-primary"


def test_send_task_complex_requirements_use_configured_default_queue_for_awakening(celery_app):
    celery_app.conf.task_default_queue = "primary"
    register_awakening_task(celery_app, inspect_timeout=1.5)

    @celery_app.task(name="tasks.process_video_primary")
    def process_video_primary() -> str:
        return "processed-primary"

    configure(celery_app)
    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="producer-complex-primary@FactoryB",
        queues=["primary", "FactoryB", "gpu"],
    ):
        result = send_task("tasks.process_video_primary", requirements=["FactoryB", "gpu"])
        assert result.get(timeout=10) == "processed-primary"


def test_send_task_complex_requirements_use_configured_default_queue_for_awakening_not_in_explicit_queues(celery_app):
    celery_app.conf.task_default_queue = "agealk"
    register_awakening_task(celery_app, inspect_timeout=1.5)

    @celery_app.task(name="tasks.process_video_primary")
    def process_video_primary() -> str:
        return "processed-primary"

    configure(celery_app)
    
    with start_worker(
        celery_app,
        pool="threads",
        concurrency=2,
        perform_ping_check=False,
        hostname="producer-complex-primary@FactoryB",
    ):
        worker = Worker(
            app=celery_app,
            hostname="producer-complex-primary@FactoryB",
            capabilities=["FactoryB", "gpu"],
        )
        result = send_task("tasks.process_video_primary", requirements=["FactoryB", "gpu"])
        assert result.get(timeout=10) == "processed-primary"
