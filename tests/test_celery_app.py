from celeryspread.celery_app import build_runtime_context, configure_celery_app


def test_build_runtime_context_defaults_to_memory_backend():
    context = build_runtime_context()

    assert context.broker_url == "memory://"
    assert context.result_backend == "cache+memory://"


def test_configure_celery_app_does_not_set_firestore_or_gcp_transport_options():
    context = build_runtime_context(use_memory_transport=True)
    app = configure_celery_app(context)

    assert app.conf.broker_url == "memory://"
    assert app.conf.result_backend == "cache+memory://"
    assert app.conf.get("firestore_backend_settings") is None
    assert app.conf.get("broker_transport_options") == {}
