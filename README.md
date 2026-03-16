# CelerySpread
Different workers like different spreads - but they all like it on celery, that's for sure

## Runtime Defaults

For local development and test runs, CelerySpread defaults to a local Redis server:

- `CELERY_BROKER_URL=redis://127.0.0.1:6379/0`
- `CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/1`

In production, set your own broker/backend via environment variables:

- `CELERY_BROKER_URL`
- `CELERY_RESULT_BACKEND`
