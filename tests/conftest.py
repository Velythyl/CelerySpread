from __future__ import annotations

import pytest
from celery import Celery

from celeryspread import producer
from celeryspread.celery_app import initialize_celery_app


@pytest.fixture
def celery_app() -> Celery:
    app, _ = initialize_celery_app(use_memory_transport=True)
    app.main = "celeryspread-test"
    app.conf.task_always_eager = False
    app.conf.task_store_eager_result = True
    return app


@pytest.fixture(autouse=True)
def reset_producer_api_state() -> None:
    producer._app = None
    yield
    producer._app = None