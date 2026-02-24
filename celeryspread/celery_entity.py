from __future__ import annotations

from celery import Celery




class CeleryEntity:
    def __new__(cls, *args, worker: bool | None = None, **kwargs):
        if cls is CeleryEntity:
            if worker is None:
                raise TypeError("CeleryEntity requires explicit 'worker=True' or 'worker=False'.")

            from .producer import Producer
            from .worker import Worker

            target_cls = Worker if worker else Producer
            return super().__new__(target_cls)
        return super().__new__(cls)

    def __init__(self, app: Celery, *args, **kwargs):
        self.app = app

    def task(self, *task_args, **task_kwargs):
        raise NotImplementedError("Subclasses must implement task().")
