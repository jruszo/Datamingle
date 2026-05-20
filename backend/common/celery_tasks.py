from common.task_queue import execute_payload
from common.authenticate.workos_directory import process_directory_event

try:
    from archery.celery import app
except ImportError:
    app = None


if app is not None:

    @app.task(name="common.execute_payload")
    def execute_payload_task(payload):
        return execute_payload(payload)

    @app.task(name="common.process_workos_webhook")
    def process_workos_webhook_task(event):
        return process_directory_event(event)

else:

    class InlineWorkOSWebhookTask:
        def delay(self, event):
            return process_directory_event(event)

        def __call__(self, event):
            return process_directory_event(event)

    process_workos_webhook_task = InlineWorkOSWebhookTask()
