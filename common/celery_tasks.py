from common.task_queue import execute_payload

try:
    from archery.celery import app
except ImportError:
    app = None


if app is not None:

    @app.task(name="common.execute_payload")
    def execute_payload_task(payload):
        return execute_payload(payload)
