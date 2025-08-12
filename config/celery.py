import os
from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('config')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_annotations={
        '*': {'rate_limit': '10/s'}
    },
    task_default_queue='default',
    task_queues={
        'high': {},
        'default': {},
        'low': {},
    },
    task_routes={
        'consumer.tasks.scrape_event': {'queue': 'high'},
    },
    task_always_eager=False,
    task_store_eager_result=True,
    result_expires=3600,
    worker_max_tasks_per_child=1000,
    worker_disable_rate_limits=True,
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s',
)