import asyncio
import gc
import json
import logging

import psutil
from celery import shared_task
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone

from consumer.message_types import create_start_message, create_success_message, create_error_message, \
    create_retry_message, MessagePattern
from consumer.models import PerformanceScrapeData
from consumer.rabbitmq_client import RabbitMQClient
from scrapers.core.stubhub_inventory_creator import StubHubInventoryCreator
from scrapers.factory import ScraperFactory
from scrapers.models import Performance

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


def determine_pos_status(enriched_data, performance):
    """
    Determine POS status using fallback hierarchy:
    1. enriched_data.posEnabled (if available)
    2. performance.pos_enabled
    3. performance.venue_id.pos_enabled
    4. False (conservative default)
    """
    if enriched_data and enriched_data.get('posEnabled') is not None:
        return enriched_data['posEnabled']

    if hasattr(performance, 'pos_enabled'):
        return performance.pos_enabled

    if performance.venue_id and hasattr(performance.venue_id, 'pos_enabled'):
        return performance.venue_id.pos_enabled

    return False


@shared_task(bind=True)
def sync_performance_pos(self, performance_id, enriched_data=None):
    """
    Synchronize seat packs for a performance with POS system.
    Only executes if POS is enabled, uses existing start/success messages.
    
    Args:
        performance_id: Internal performance ID to sync
        enriched_data: Optional enriched data containing POS configuration
        
    Returns:
        Dict with sync results and counts
    """
    logger.info(f"POS sync requested for performance: {performance_id}")

    rabbitmq_client = None

    try:
        # Step 1: Early POS check (fail fast)
        performance = Performance.objects.select_related('venue_id').get(
            internal_performance_id=performance_id
        )

        pos_enabled = determine_pos_status(enriched_data, performance)

        if not pos_enabled:
            logger.info(f"POS not enabled for performance {performance_id}, skipping sync")
            return {'status': 'skipped', 'reason': 'POS not enabled'}

        # Step 2: Send start message using existing function
        rabbitmq_client = RabbitMQClient()
        start_message = create_start_message(f"pos_sync_{performance_id}", self.request.id, enriched_data,
                                             pattern=MessagePattern.POS_SYNC_SUCCESS.value)
        rabbitmq_client.send_message(start_message)

        logger.info(f"POS enabled for performance {performance_id}, starting sync")

        # Step 3: Execute sync using existing StubHubInventoryCreator
        creator = StubHubInventoryCreator(pos_enabled=pos_enabled)
        results = creator.sync_pending_packs(performance_id)

        # Step 4: Send success message using existing function
        success_message = create_success_message(f"pos_sync_{performance_id}", {
            'performance_id': performance_id,
            'packs_created': results.get('created', 0),
            'packs_deleted': results.get('deleted', 0),
            'packs_failed': results.get('failed', 0),
            'total_processed': results.get('created', 0) + results.get('deleted', 0),
            'sync_type': 'pos_sync'
        }, enriched_data)
        rabbitmq_client.send_message(success_message)

        logger.info(f"POS sync completed for performance {performance_id}: "
                    f"{results.get('created', 0)} created, {results.get('deleted', 0)} deleted, "
                    f"{results.get('failed', 0)} failed")

        return {
            'status': 'success',
            'performance_id': performance_id,
            'packs_created': results.get('created', 0),
            'packs_deleted': results.get('deleted', 0),
            'packs_failed': results.get('failed', 0)
        }

    except Performance.DoesNotExist:
        error_msg = f"Performance {performance_id} not found"
        logger.error(error_msg)
        return {
            'status': 'failed',
            'performance_id': performance_id,
            'error': error_msg
        }

    except Exception as e:
        logger.error(f"POS sync failed for performance {performance_id}: {e}")

        # Send error message using existing function
        if rabbitmq_client:
            error_message = create_error_message(f"pos_sync_{performance_id}", str(e), enriched_data)
            rabbitmq_client.send_message(error_message)

        return {
            'status': 'failed',
            'performance_id': performance_id,
            'error': str(e)
        }

    finally:
        if rabbitmq_client:
            rabbitmq_client.close()


@shared_task(bind=True, max_retries=1)
def scrape_performance(self, url, scrape_job_id, pos=False, enriched_data=None):
    logger.info(f"Starting scrape for URL: {url}, scrape_job_id: {scrape_job_id}")
    logger.info(enriched_data)

    # Memory monitoring

    initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
    logger.info(f"Initial memory usage: {initial_memory:.2f} MB")

    rabbitmq_client = None
    scraper = None
    try:
        rabbitmq_client = RabbitMQClient()

        start_message = create_start_message(scrape_job_id, self.request.id, enriched_data)
        rabbitmq_client.send_message(start_message)

        # Try to create scraper with defaults first
        try:
            scraper = ScraperFactory.create_scraper_with_defaults(url, scrape_job_id)
        except Exception as e:
            logger.warning(f"Failed to create scraper with defaults, trying basic: {e}")
            scraper = ScraperFactory.create_scraper(url, scrape_job_id)

        if not scraper:
            raise Exception(f"No scraper found for URL: {url}")

        # Pass enriched_data to scraper if it supports it (e.g., DemoScraper)
        if enriched_data and hasattr(scraper, 'enriched_data'):
            scraper.enriched_data = enriched_data
            # Also update the extractor context if available
            if hasattr(scraper, 'extractor') and hasattr(scraper.extractor, 'set_scrape_context'):
                scraper.extractor.set_scrape_context(scrape_job_id, scraper.venue_name, enriched_data)
        if hasattr(scraper, 'scrape') and asyncio.iscoroutinefunction(scraper.scrape):
            result = asyncio.run(scraper.scrape())

            if result.status.value not in ['success', 'partial_success']:
                error_messages = [error.message for error in result.errors if error.message]
                if error_messages:
                    raise Exception(f"Scraping failed: {'; '.join(error_messages)}")
                elif result.errors:
                    raise Exception(f"Scraping failed with {len(result.errors)} error(s) but no error messages")
                else:
                    raise Exception(f"Scraping failed with status: {result.status.value}")

            if result.status.value == 'partial_success':
                if result.validation and result.validation.errors:
                    logger.warning(f"Data validation issues (continuing anyway): {result.validation.errors}")
                if result.errors:
                    logger.warning(
                        f"Non-fatal errors occurred (continuing anyway): {[e.message for e in result.errors]}")

            performance_key = result.database_key
            if not performance_key:
                raise Exception("Database storage failed - no performance key returned")
        else:
            raise Exception("Scraper does not support async architecture")

        internal_event_id = None
        internal_performance_id = None
        internal_venue_id = None
        venue_timezone = None

        try:
            performance_record = Performance.objects.select_related('event_id', 'venue_id').get(
                internal_performance_id=performance_key)
            internal_event_id = performance_record.event_id.internal_event_id
            internal_performance_id = performance_record.internal_performance_id
            internal_venue_id = performance_record.venue_id.internal_venue_id
            venue_timezone = performance_record.venue_id.venue_timezone
        except Exception as e:
            if 'Performance' in str(type(e).__name__):
                logger.warning(f"Performance record not found for key: {performance_key}")
            else:
                logger.warning(f"Error retrieving internal IDs for performance {performance_key}: {e}")

        success_message = create_success_message(scrape_job_id, {
            'url': url,
            'performance_key': performance_key,
            'internal_event_id': internal_event_id,
            'internal_performance_id': internal_performance_id,
            'internal_venue_id': internal_venue_id,
            'venue_timezone': venue_timezone,
            'scraper_name': scraper.name,
            'status': result.status.value,
            'scraped_at': result.scraped_at
        }, enriched_data)
        rabbitmq_client.send_message(success_message)

        if pos:
            logger.info(f"Triggering POS sync for performance: {internal_performance_id}")
            # Fire and forget - don't block scraping on POS sync
            sync_performance_pos.delay(internal_performance_id, enriched_data)

        # Memory cleanup and monitoring
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_diff = final_memory - initial_memory
        logger.info(f"Final memory usage: {final_memory:.2f} MB (diff: {memory_diff:+.2f} MB)")

        # Force garbage collection
        gc.collect()

        return {
            'status': 'success' if result.status.value in ['success', 'partial_success'] else 'failed',
            'scrape_job_id': scrape_job_id,
            'internal_event_id': internal_event_id,
            'internal_performance_id': internal_performance_id,
            'internal_venue_id': internal_venue_id,
            'venue_timezone': venue_timezone
        }

    except Exception as e:
        error_str = str(e).strip() if str(e).strip() else 'Unknown error occurred'
        error_message = f"Error scraping {url}: {error_str}"
        logger.error(error_message)

        if rabbitmq_client:
            error_str = str(e).strip() if str(e).strip() else 'Unknown error occurred'
            error_message_obj = create_error_message(scrape_job_id, error_str, enriched_data)
            rabbitmq_client.send_message(error_message_obj)

        PerformanceScrapeData.objects.create(
            pattern=MessagePattern.SCRAPE_ERROR.value,
            scrape_job_id=scrape_job_id,
            data=safe_json_dumps({
                'url': url,
                'error': str(e).strip() if str(e).strip() else 'Unknown error occurred',
                'status': 'failed',
                'scraper_version': 'v5.0',
                'failed_at': timezone.now().isoformat(),
                'retry_attempt': self.request.retries
            })
        )

        try:
            retry_delay = 60 * (self.request.retries + 1)
            current_attempt = self.request.retries + 1

            if rabbitmq_client:
                retry_message = create_retry_message(
                    scrape_job_id,
                    str(e),
                    current_attempt,
                    self.max_retries + 1,
                    retry_delay,
                    enriched_data
                )
                rabbitmq_client.send_message(retry_message)

            self.retry(countdown=retry_delay)
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for {url}")

            if rabbitmq_client:
                final_error_message = create_error_message(
                    scrape_job_id,
                    f"Max retries exceeded after {self.max_retries + 1} attempts. Last error: {str(e)}",
                    enriched_data
                )
                rabbitmq_client.send_message(final_error_message)

        return {
            'status': 'failed',
            'url': url,
            'scrape_job_id': scrape_job_id,
            'error': str(e),
            'retries': self.request.retries
        }
    finally:
        # Cleanup resources
        if scraper and hasattr(scraper, 'cleanup'):
            try:
                scraper.cleanup()
            except Exception as e:
                logger.warning(f"Error during scraper cleanup: {e}")

        if rabbitmq_client:
            rabbitmq_client.close()

        # Final memory cleanup
        gc.collect()
