from __future__ import annotations

from celery import Celery

from .producer_api import configure
from .tasks import register_awakening_task


def build_default_app() -> Celery:
    app = Celery("celeryspread")
    configure(app)
    register_awakening_task(app)
    return app
