"""
Event tracking service for comprehensive scraping lifecycle monitoring.
Saves all scraping events to Django database for dashboard visibility.
"""

import logging
import time
import traceback
from typing import Dict, Any, Optional
from decimal import Decimal
from django.utils import timezone
from asgiref.sync import sync_to_async
from ..models.monitoring import ScrapingEvent, ScraperStatus

logger = logging.getLogger(__name__)


class ScrapingEventTracker:
    """Service to track and save all scraping lifecycle events"""

    def __init__(self, scrape_job_id: str, scraper_name: str, url: str = None, 
                 venue: str = None, user_id: int = None):
        self.external_job_id = scrape_job_id
        self.scraper_name = scraper_name
        self.url = url
        self.venue = venue
        self.user_id = user_id
        self.start_time = None
        self.scrape_job = None

    def _create_event(self, event_type: str, message: str, severity: str = 'info', **kwargs):
        """Create and save a scraping event to the database (async-safe)"""
        try:
            event_data = {
                'external_job_id': self.external_job_id,
                'scraper_name': self.scraper_name,
                'event_type': event_type,
                'message': message,
                'severity': severity,
                'url': self.url,
                'venue': self.venue,
                'user_id': self.user_id,
                'scrape_job': self.scrape_job,
                'timestamp': timezone.now(),
                **kwargs
            }
            
            # Calculate processing time if start_time is available
            if self.start_time and 'processing_time_ms' not in event_data:
                event_data['processing_time_ms'] = int((time.time() - self.start_time) * 1000)
            
            # Try direct ORM call, fall back to logging if in async context
            try:
                event = ScrapingEvent.objects.create(**event_data)
                logger.debug(f"Created scraping event: {event_type} for job {self.external_job_id}")
                return event
            except Exception as orm_error:
                # If ORM call fails (likely due to async context), just log the event
                error_msg = str(orm_error)
                if "async context" in error_msg or "sync_to_async" in error_msg:
                    logger.info(f"Event {event_type}: {message} (job: {self.external_job_id}) - DB save skipped in async context")
                else:
                    logger.error(f"Failed to create scraping event {event_type}: {orm_error}")
                return None
            
        except Exception as e:
            logger.error(f"Failed to create scraping event {event_type}: {e}")
            return None

    def track_scrape_started(self, event_title: str = None, metadata: Dict[str, Any] = None):
        """Track when scraping starts"""
        self.start_time = time.time()
        
        # Update scraper status (async-safe)
        try:
            scraper_status, created = ScraperStatus.objects.get_or_create(
                scraper_name=self.scraper_name,
                defaults={
                    'display_name': self.scraper_name.replace('_', ' ').title(),
                    'description': f'Scraper for {self.venue or "unknown venue"}'
                }
            )
            scraper_status.current_status = 'running'
            scraper_status.last_run_start = timezone.now()
            scraper_status.is_available = False
            scraper_status.save()
        except Exception as e:
            error_msg = str(e)
            if "async context" in error_msg or "sync_to_async" in error_msg:
                logger.info(f"Scraper status update skipped in async context for {self.scraper_name}")
            else:
                logger.warning(f"Failed to update scraper status: {e}")

        return self._create_event(
            event_type='scrape_started',
            message=f'Started scraping {event_title or "performance"} from {self.venue or "unknown venue"}',
            event_title=event_title,
            metadata=metadata or {}
        )

    def track_extraction_started(self, metadata: Dict[str, Any] = None):
        """Track when data extraction starts"""
        return self._create_event(
            event_type='extraction_started',
            message='Started data extraction',
            metadata=metadata or {}
        )

    def track_extraction_completed(self, items_extracted: int = None, metadata: Dict[str, Any] = None):
        """Track when data extraction completes"""
        message = 'Completed data extraction'
        if items_extracted:
            message += f' - {items_extracted} items extracted'
            
        return self._create_event(
            event_type='extraction_completed',
            message=message,
            items_scraped=items_extracted,
            metadata=metadata or {}
        )

    def track_processing_started(self, metadata: Dict[str, Any] = None):
        """Track when data processing starts"""
        return self._create_event(
            event_type='processing_started',
            message='Started data processing',
            metadata=metadata or {}
        )

    def track_processing_completed(self, items_processed: int = None, metadata: Dict[str, Any] = None):
        """Track when data processing completes"""
        message = 'Completed data processing'
        if items_processed:
            message += f' - {items_processed} items processed'
            
        return self._create_event(
            event_type='processing_completed',
            message=message,
            items_scraped=items_processed,
            metadata=metadata or {}
        )

    def track_storage_started(self, metadata: Dict[str, Any] = None):
        """Track when database storage starts"""
        return self._create_event(
            event_type='storage_started',
            message='Started database storage',
            metadata=metadata or {}
        )

    def track_storage_completed(self, database_key: str = None, metadata: Dict[str, Any] = None):
        """Track when database storage completes"""
        message = 'Completed database storage'
        if database_key:
            message += f' - stored with key: {database_key}'
            if not metadata:
                metadata = {}
            metadata['database_key'] = database_key
            
        return self._create_event(
            event_type='storage_completed',
            message=message,
            metadata=metadata or {}
        )

    def track_progress_update(self, items_scraped: int, total_items: int = None, 
                            progress_percentage: float = None, metadata: Dict[str, Any] = None):
        """Track progress updates during scraping"""
        message = f'Progress update: {items_scraped} items scraped'
        if total_items:
            message += f' of {total_items}'
        if progress_percentage:
            message += f' ({progress_percentage:.1f}%)'

        return self._create_event(
            event_type='progress_update',
            message=message,
            items_scraped=items_scraped,
            total_items=total_items,
            progress_percentage=Decimal(str(progress_percentage)) if progress_percentage else None,
            metadata=metadata or {}
        )

    def track_scrape_completed(self, final_items: int = None, performance_id: str = None, 
                             metadata: Dict[str, Any] = None):
        """Track when scraping completes successfully"""
        processing_time = int((time.time() - self.start_time) * 1000) if self.start_time else None
        
        message = 'Scraping completed successfully'
        if final_items:
            message += f' - {final_items} total items scraped'
        if processing_time:
            message += f' in {processing_time}ms'

        # Update scraper status (async-safe)
        try:
            scraper_status = ScraperStatus.objects.get(scraper_name=self.scraper_name)
            scraper_status.current_status = 'idle'
            scraper_status.last_run_end = timezone.now()
            scraper_status.last_success = timezone.now()
            scraper_status.is_available = True
            scraper_status.total_runs += 1
            scraper_status.successful_runs += 1
            scraper_status.consecutive_failures = 0
            scraper_status.calculate_success_rate()
            scraper_status.update_health_status()
            scraper_status.save()
        except Exception as e:
            error_msg = str(e)
            if "async context" in error_msg or "sync_to_async" in error_msg:
                logger.info(f"Scraper status update skipped in async context for {self.scraper_name}")
            else:
                logger.warning(f"Failed to update scraper status: {e}")

        event_metadata = metadata or {}
        if performance_id:
            event_metadata['performance_id'] = performance_id

        return self._create_event(
            event_type='scrape_completed',
            message=message,
            items_scraped=final_items,
            processing_time_ms=processing_time,
            metadata=event_metadata
        )

    def track_scrape_failed(self, error_message: str, error_type: str = None, 
                          stack_trace: str = None, metadata: Dict[str, Any] = None):
        """Track when scraping fails"""
        processing_time = int((time.time() - self.start_time) * 1000) if self.start_time else None
        
        # Update scraper status (async-safe)
        try:
            scraper_status = ScraperStatus.objects.get(scraper_name=self.scraper_name)
            scraper_status.current_status = 'error'
            scraper_status.last_run_end = timezone.now()
            scraper_status.last_failure = timezone.now()
            scraper_status.is_available = True
            scraper_status.total_runs += 1
            scraper_status.failed_runs += 1
            scraper_status.consecutive_failures += 1
            scraper_status.last_error_message = error_message[:1000]  # Truncate if too long
            scraper_status.calculate_success_rate()
            scraper_status.update_health_status()
            scraper_status.save()
        except Exception as e:
            error_msg = str(e)
            if "async context" in error_msg or "sync_to_async" in error_msg:
                logger.info(f"Scraper status update skipped in async context for {self.scraper_name}")
            else:
                logger.warning(f"Failed to update scraper status: {e}")

        return self._create_event(
            event_type='scrape_failed',
            message=f'Scraping failed: {error_message}',
            severity='error',
            error_type=error_type,
            error_details=error_message,
            stack_trace=stack_trace,
            processing_time_ms=processing_time,
            metadata=metadata or {}
        )

    def track_error(self, error_type: str, error_message: str, severity: str = 'error',
                   stack_trace: str = None, metadata: Dict[str, Any] = None):
        """Track any error that occurs during scraping"""
        return self._create_event(
            event_type='error_occurred',
            message=f'{error_type}: {error_message}',
            severity=severity,
            error_type=error_type,
            error_details=error_message,
            stack_trace=stack_trace,
            metadata=metadata or {}
        )

    def track_status_update(self, status: str, additional_info: str = None, 
                          metadata: Dict[str, Any] = None):
        """Track general status updates"""
        message = f'Status update: {status}'
        if additional_info:
            message += f' - {additional_info}'

        return self._create_event(
            event_type='status_update',
            message=message,
            metadata=metadata or {}
        )

    def set_scrape_job(self, scrape_job):
        """Set the related Django ScrapeJob object"""
        self.scrape_job = scrape_job

    @classmethod
    def create_tracker(cls, scrape_job_id: str, scraper_name: str, url: str = None,
                      venue: str = None, user_id: int = None):
        """Factory method to create a new event tracker"""
        return cls(scrape_job_id, scraper_name, url, venue, user_id)


def track_scraping_exception(tracker: ScrapingEventTracker, exception: Exception, 
                           error_type: str = None):
    """Helper function to track exceptions with full stack trace"""
    error_type = error_type or exception.__class__.__name__
    stack_trace = traceback.format_exc()
    
    tracker.track_error(
        error_type=error_type,
        error_message=str(exception),
        severity='error',
        stack_trace=stack_trace
    )