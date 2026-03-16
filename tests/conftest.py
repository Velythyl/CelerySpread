from __future__ import annotations

from contextlib import contextmanager
import subprocess
import time
import socket
from typing import Iterator

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker

from celeryspread import producer
from celeryspread.celery_app import initialize_celery_app
from celeryspread.worker import Worker


def _wait_for_redis(host: str = "127.0.0.1", port: int = 6379, timeout: float = 5.0) -> bool:
    """Wait for Redis to be available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((host, port))
            sock.close()
            return True
        except (socket.error, socket.timeout):
            time.sleep(0.1)
    return False


@pytest.fixture(scope="session")
def redis_server():
    """Start Redis server as a subprocess for the test session."""
    proc = subprocess.Popen(
        ["redis-server", "--port", "6379", "--daemonize", "no"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if not _wait_for_redis():
        proc.terminate()
        raise RuntimeError("Failed to start Redis server")
    yield proc
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture
def celery_app(redis_server, monkeypatch: pytest.MonkeyPatch) -> Celery:
    # Use Redis for proper integration testing
    monkeypatch.setenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
    monkeypatch.setenv("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1")
    app, _ = initialize_celery_app()
    app.main = "celeryspread-test"
    app.conf.task_always_eager = False
    app.conf.task_store_eager_result = True
    return app


@pytest.fixture(autouse=True)
def reset_producer_api_state() -> None:
    producer._app = None
    yield
    producer._app = None


@pytest.fixture
def run_worker(celery_app: Celery):
    @contextmanager
    def _run_worker(
        hostname: str,
        capabilities: list[str] | None = None,
        *,
        include_default_queue: bool = False,
        extra_queues: list[str] | None = None,
        pool: str = "solo",
        concurrency: int | None = None,
        perform_ping_check: bool = False,
    ) -> Iterator[Worker]:
        worker = Worker(app=celery_app, hostname=hostname, capabilities=capabilities or [])
        queue_names = set(worker.queues)

        if include_default_queue:
            queue_names.add(celery_app.conf.task_default_queue)
        if extra_queues:
            queue_names.update(extra_queues)

        kwargs = {
            "pool": pool,
            "perform_ping_check": perform_ping_check,
            "hostname": worker.hostname,
            "queues": sorted(queue_names),
        }
        if concurrency is not None:
            kwargs["concurrency"] = concurrency

        with start_worker(celery_app, **kwargs):
            yield worker

    return _run_worker