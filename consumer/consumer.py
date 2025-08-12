import json
import os
import logging
import django
import pika

# CRITICAL: Setup Django BEFORE importing any Django modules
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

# Import Django modules AFTER django.setup()
from consumer.models import PerformanceScrapeData
from consumer.message_types import create_acknowledgment_message, MessagePattern
from consumer.rabbitmq_client import RabbitMQClient

logger = logging.getLogger(__name__)


def start_consumer():
    # Log environment setup for debugging
    logger.info("🔧 Consumer starting - Environment check:")
    logger.info(f"   CELERY_BROKER_URL: {os.environ.get('CELERY_BROKER_URL', 'NOT SET')}")
    logger.info(f"   DATABASE_URL: {os.environ.get('DATABASE_URL', 'NOT SET')[:50]}...")
    logger.info(f"   DJANGO_SETTINGS_MODULE: {os.environ.get('DJANGO_SETTINGS_MODULE', 'NOT SET')}")
    
    # Test Celery connection and task registration
    try:
        from config.celery import app as celery_app
        logger.info(f"✅ Celery app imported successfully: {celery_app.conf.broker_url}")
        
        # Test task import and registration
        from consumer.tasks import scrape_performance
        registered_tasks = list(celery_app.tasks.keys())
        logger.info(f"🔍 Registered tasks: {registered_tasks}")
        
        if 'consumer.tasks.scrape_performance' in registered_tasks:
            logger.info("✅ scrape_performance task properly registered")
        else:
            logger.error("❌ scrape_performance task NOT registered!")
            
    except Exception as e:
        logger.error(f"❌ Celery setup failed: {e}")
    
    credentials = pika.PlainCredentials(
        os.environ.get('RABBITMQ_USER', 'admin'),
        os.environ.get('RABBITMQ_PASSWORD', 'admin123')
    )

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ.get('RABBITMQ_HOST', 'rabbitmq'),
            port=int(os.environ.get('RABBITMQ_PORT', 5672)),
            credentials=credentials
        )
    )

    channel = connection.channel()
    channel.queue_declare(queue='nest_to_django', durable=True)
    channel.queue_declare(queue='django_to_nest', durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        rabbitmq_client = None
        try:
            print("dlsjfhkjds hfkjdsb fjksdhfjkhdsfjhsdb jf")
            message = json.loads(body.decode())
            pattern = message.get('pattern')
            data = message.get('data', {})
            
            # Extract basic fields
            url = data.get('url')
            scrape_job_id = data.get('scrapeJobId')
            user_id = data.get('userId')
            
            # Extract enriched data object (present or None)
            enriched_data = data.get('enrichedData')
            print(data)
            # Determine POS flag from enriched data or legacy field
            pos_enabled = enriched_data.get('posEnabled', False) if enriched_data else data.get('pos', False)
            
            if not url:
                logger.error("No URL found in message")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if not scrape_job_id:
                logger.error("No scrape job ID found in message")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            rabbitmq_client = RabbitMQClient()
            
            ack_message = create_acknowledgment_message(scrape_job_id, enriched_data, user_id)
            rabbitmq_client.send_message(ack_message)
            logger.info(f"✅ Acknowledgment sent for scrape_job_id: {scrape_job_id}")

            try:
                logger.info(f"🗄️ Creating database record for scrape request: {scrape_job_id}")
                PerformanceScrapeData.objects.create(
                    pattern=pattern or MessagePattern.SCRAPE_REQUEST.value,
                    scrape_job_id=scrape_job_id,
                    data=json.dumps({
                        'url': url,
                        'scrape_job_id': scrape_job_id,
                        'user_id': user_id,
                        'enriched_data': enriched_data,
                        'raw_message': message
                    })
                )
                logger.info(f"✅ Database record created for scrape_job_id: {scrape_job_id}")
            except Exception as e:
                logger.error(f"❌ Database creation failed for scrape_job_id {scrape_job_id}: {e}")
                raise

            try:
                logger.info(f"🚀 Queuing Celery task for scrape_job_id: {scrape_job_id}, pos_enabled: {pos_enabled}")
                # Import task here to ensure Django/Celery is fully set up
                from consumer.tasks import scrape_performance
                task = scrape_performance.delay(url, scrape_job_id, pos_enabled, enriched_data)
                logger.info(f"✅ Celery task queued successfully! Task ID: {task.id}, scrape_job_id: {scrape_job_id}")
            except Exception as e:
                logger.error(f"❌ Celery task queueing failed for scrape_job_id {scrape_job_id}: {e}")
                logger.error(f"   Error details: {str(e)}")
                raise

            try:
                logger.info(f"🗄️ Creating task started record for scrape_job_id: {scrape_job_id}")
                PerformanceScrapeData.objects.create(
                    pattern=MessagePattern.SCRAPE_STARTED.value,
                    scrape_job_id=scrape_job_id,
                    data=json.dumps({
                        'task_id': task.id,
                        'url': url,
                        'scrape_job_id': scrape_job_id,
                        'user_id': user_id
                    })
                )
                logger.info(f"✅ Task started record created for scrape_job_id: {scrape_job_id}")
            except Exception as e:
                logger.error(f"❌ Task started record creation failed for scrape_job_id {scrape_job_id}: {e}")
                # Don't raise here - task is already queued

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        finally:
            if rabbitmq_client:
                rabbitmq_client.close()

    channel.basic_consume(
        queue='nest_to_django',
        on_message_callback=callback,
        auto_ack=False
    )

    logger.info(' [*] Waiting for messages. To exit press CTRL+C')

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        if connection and connection.is_open:
            connection.close()


if __name__ == "__main__":
    start_consumer()