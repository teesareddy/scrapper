import json
import os
import logging
import pika
from typing import Dict, Any
from .message_types import MessageResponse
from django.utils import timezone
from django.core.serializers.json import DjangoJSONEncoder

logger = logging.getLogger(__name__)


class SafeJSONEncoder(DjangoJSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        if hasattr(obj, '_meta') and hasattr(obj, 'pk'):
            return str(obj.pk)
        return super().default(obj)


def safe_json_dumps(obj, **kwargs):
    try:
        return json.dumps(obj, cls=SafeJSONEncoder, **kwargs)
    except Exception as e:
        logger.error(f"JSON serialization failed: {e}")
        return json.dumps({
            'error': f'Serialization failed: {str(e)}',
            'timestamp': timezone.now().isoformat()
        })


class RabbitMQClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        self._setup_connection()

    def _setup_connection(self):
        credentials = pika.PlainCredentials(
            os.environ.get('RABBITMQ_USER', 'admin'),
            os.environ.get('RABBITMQ_PASSWORD', 'admin123')
        )

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.environ.get('RABBITMQ_HOST', 'rabbitmq'),
                port=int(os.environ.get('RABBITMQ_PORT', 5672)),
                credentials=credentials
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='django_to_nest', durable=True)

    def send_message(self, message: MessageResponse):
        if not self.connection or self.connection.is_closed:
            self._setup_connection()

        try:
            self.channel.basic_publish(
                exchange='',
                routing_key='django_to_nest',
                body=safe_json_dumps(message.to_dict()),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                )
            )
            logger.info(f"Message sent with pattern: {message.pattern}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()