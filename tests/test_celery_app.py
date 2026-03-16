from celeryspread.celery_app import build_runtime_context, configure_celery_app


def test_build_runtime_context_defaults_to_local_redis(monkeypatch):
    monkeypatch.delenv("CELERY_BROKER_URL", raising=False)
    monkeypatch.delenv("CELERY_RESULT_BACKEND", raising=False)

    context = build_runtime_context()

    assert context.broker_url == "redis://127.0.0.1:6379/0"
    assert context.result_backend == "redis://127.0.0.1:6379/1"


def test_build_runtime_context_uses_environment_overrides(monkeypatch):
    monkeypatch.setenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
    monkeypatch.setenv("CELERY_RESULT_BACKEND", "rpc://")

    context = build_runtime_context()

    assert context.broker_url == "amqp://guest:guest@localhost:5672//"
    assert context.result_backend == "rpc://"


def test_configure_celery_app_does_not_set_firestore_or_gcp_transport_options(monkeypatch):
    monkeypatch.delenv("CELERY_BROKER_URL", raising=False)
    monkeypatch.delenv("CELERY_RESULT_BACKEND", raising=False)
    context = build_runtime_context()
    app = configure_celery_app(context)

    assert app.conf.broker_url == "redis://127.0.0.1:6379/0"
    assert app.conf.result_backend == "redis://127.0.0.1:6379/1"
    assert app.conf.get("firestore_backend_settings") is None
    assert app.conf.get("broker_transport_options") == {}
