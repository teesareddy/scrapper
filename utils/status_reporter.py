"""
Status reporting utility for scrapers to send updates to NestJS backend
"""

import time
import logging
from typing import Dict, Any, Optional
from contextlib import contextmanager
from consumer.scrape_status_sender import scrape_status_sender

logger = logging.getLogger(__name__)

class ScrapeStatusReporter:
    """
    Helper class to manage scrape status reporting throughout the scraping process
    """
    
    def __init__(self, scrape_job_id: str, venue: str, user_id: Optional[int] = None):
        self.scrape_job_id = scrape_job_id
        self.venue = venue
        self.user_id = user_id
        self.start_time = None
        self.items_scraped = 0
        self.event_title = None
        
    def set_event_title(self, title: str):
        """Set the event title being scraped"""
        self.event_title = title
        
    def start_scraping(self):
        """Mark the start of scraping process"""
        self.start_time = time.time()
        scrape_status_sender.send_scrape_started(
            scrape_job_id=self.scrape_job_id,
            venue=self.venue,
            event_title=self.event_title,
            user_id=self.user_id,
        )
        logger.info(f"Started scraping job {self.scrape_job_id} for {self.venue}")
    
    def update_progress(self, items_scraped: int, additional_data: Optional[Dict[str, Any]] = None):
        """Update scraping progress"""
        self.items_scraped = items_scraped
        
        # Send heartbeat with progress
        processing_time = int((time.time() - self.start_time) * 1000) if self.start_time else None
        
        scrape_status_sender.send_status_update(
            scrape_job_id=self.scrape_job_id,
            status='processing',
            venue=self.venue,
            event_title=self.event_title,
            items_scraped=items_scraped,
            processing_time_ms=processing_time,
            data_extracted=additional_data,
            user_id=self.user_id,
        )
        
        logger.debug(f"Progress update: {items_scraped} items scraped for job {self.scrape_job_id}")
    
    def complete_scraping(self, final_data: Optional[Dict[str, Any]] = None):
        """Mark scraping as completed"""
        processing_time = int((time.time() - self.start_time) * 1000) if self.start_time else None
        
        scrape_status_sender.send_scrape_completed(
            scrape_job_id=self.scrape_job_id,
            venue=self.venue,
            event_title=self.event_title,
            data_extracted=final_data,
            processing_time_ms=processing_time,
            items_scraped=self.items_scraped,
            user_id=self.user_id,
        )
        
        # Send performance data if available
        if final_data:
            scrape_status_sender.send_performance_data(
                venue=self.venue,
                event_title=self.event_title or 'Unknown Event',
                performance_data=final_data,
                scrape_job_id=self.scrape_job_id,
            )
        
        # Note: Venue configuration is now included in scrape completion message automatically
        
        logger.info(f"Completed scraping job {self.scrape_job_id}: {self.items_scraped} items in {processing_time}ms")
    
    def fail_scraping(self, error_message: str):
        """Mark scraping as failed"""
        processing_time = int((time.time() - self.start_time) * 1000) if self.start_time else None
        
        scrape_status_sender.send_scrape_failed(
            scrape_job_id=self.scrape_job_id,
            venue=self.venue,
            error_details=error_message,
            event_title=self.event_title,
            processing_time_ms=processing_time,
            user_id=self.user_id,
        )
        
        logger.error(f"Failed scraping job {self.scrape_job_id}: {error_message}")
    
    def send_error(self, error_type: str, error_message: str, severity: str = 'medium'):
        """Send an error notification"""
        scrape_status_sender.send_error_notification(
            error_type=error_type,
            error_message=error_message,
            venue=self.venue,
            scrape_job_id=self.scrape_job_id,
            user_id=self.user_id,
            severity=severity,
        )
    


@contextmanager
def scrape_status_context(scrape_job_id: str, venue: str, user_id: Optional[int] = None):
    """
    Context manager for automatic status reporting
    
    Usage:
        with scrape_status_context('job_123', 'Broadway SF') as reporter:
            reporter.set_event_title('Hamilton')
            # Do scraping work
            reporter.update_progress(10)
            # More scraping
            reporter.complete_scraping({'seats': 100, 'available': 50})
    """
    reporter = ScrapeStatusReporter(scrape_job_id, venue, user_id)
    reporter.start_scraping()
    
    try:
        yield reporter
    except Exception as e:
        reporter.fail_scraping(str(e))
        raise
    finally:
        # Cleanup if needed
        pass


def create_status_reporter(scrape_job_id: str, venue: str, user_id: Optional[int] = None) -> ScrapeStatusReporter:
    """
    Factory function to create a status reporter
    """
    return ScrapeStatusReporter(scrape_job_id, venue, user_id)