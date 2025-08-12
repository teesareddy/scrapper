# consumer/venue_reply_consumer.py
import json
import os
import logging
import django
import pika
from datetime import datetime

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from consumer.venue_task_handlers import VENUE_TASK_HANDLERS

logger = logging.getLogger(__name__)


def start_venue_reply_consumer():
    """
    Consumer that handles venue/event/performance requests with reply queue pattern
    """
    # Setup RabbitMQ connection
    credentials = pika.PlainCredentials(
        os.environ.get('RABBITMQ_USER', 'user'),
        os.environ.get('RABBITMQ_PASSWORD', 'password')
    )

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ.get('RABBITMQ_HOST', 'localhost'),
            port=int(os.environ.get('RABBITMQ_PORT', 5672)),
            credentials=credentials
        )
    )

    channel = connection.channel()

    # Declare the queue that NestJS will send requests to
    channel.queue_declare(queue='django_queue', durable=False)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        """Process venue/event/performance requests and send replies"""
        try:
            # Parse the incoming message
            message = json.loads(body.decode())
            logger.info(f"Received venue request: {message}")
            print(f"=== RECEIVED MESSAGE ===")
            print(f"Full message: {message}")
            print(f"Message type: {type(message)}")
            pattern = message.get('pattern')
            data = message.get('data', {})

            # Process the request using registered handlers
            if pattern in VENUE_TASK_HANDLERS:
                logger.info(f"Processing pattern: {pattern}")
                print(f"Processing pattern: {pattern} with data: {data}")
                result = VENUE_TASK_HANDLERS[pattern](data)

                # Clean response - no internal metadata needed

                logger.info(f"Successfully processed {pattern}")
            else:
                logger.warning(f"Unknown pattern: {pattern}")
                result = {
                    'success': False,
                    'error': f'Unknown pattern: {pattern}',
                    'available_patterns': list(VENUE_TASK_HANDLERS.keys())
                }

            # Send reply back to NestJS
            if properties.reply_to:
                response_body = json.dumps(result)

                ch.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id,
                        content_type='application/json'
                    ),
                    body=response_body
                )

                logger.info(f"Sent reply for pattern {pattern} to {properties.reply_to}")
            else:
                logger.warning("No reply_to property found in message")

            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except Exception as e:
            logger.error(f"Error processing venue request: {e}", exc_info=True)

            # Send error response if possible
            if properties and properties.reply_to:
                error_response = {
                    'success': False,
                    'error': str(e),
                    'pattern': 'error',
                    'processed_at': datetime.now().isoformat()
                }

                try:
                    ch.basic_publish(
                        exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(
                            correlation_id=properties.correlation_id,
                            content_type='application/json'
                        ),
                        body=json.dumps(error_response)
                    )
                except Exception as publish_error:
                    logger.error(f"Failed to send error response: {publish_error}")

            # Acknowledge to prevent requeue loop
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # Set up consumer
    channel.basic_consume(
        queue='django_queue',
        on_message_callback=callback,
        auto_ack=False
    )

    logger.info(' [*] Waiting for venue requests. To exit press CTRL+C')
    logger.info(f' [*] Available handlers: {list(VENUE_TASK_HANDLERS.keys())}')
    print(f'Available handlers: {list(VENUE_TASK_HANDLERS.keys())}')

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Shutting down venue reply consumer...")
        channel.stop_consuming()
    finally:
        if connection and connection.is_open:
            connection.close()
            logger.info("Connection closed")


if __name__ == "__main__":
    start_venue_reply_consumer()