"""
Notification System Usage Examples

This file demonstrates how to use the new plug-and-play notification system
in various scenarios throughout the scraper dashboard.
"""

import uuid
from datetime import datetime, timedelta
from typing import Dict, Any

# Import the notification helpers
from .notification_helpers import (
    notify_scrape_acknowledged,
    notify_scrape_queued,
    notify_scrape_started,
    notify_scrape_progress,
    notify_scrape_retry,
    notify_scrape_success,
    notify_scrape_error,
    notify_pos_sync_success,
    notify_pos_sync_error,
    ProgressTracker,
    notify_batch_scrape_started,
    notify_batch_scrape_completed,
)

# Direct import of the full-featured sender
from .scrape_status_sender import scrape_status_sender


def example_scrape_lifecycle():
    """
    Example of complete scrape lifecycle with notifications
    """
    scrape_job_id = str(uuid.uuid4())
    venue = "Broadway SF"
    event_title = "Hamilton"
    performance_date = datetime(2024, 1, 1, 19, 0)
    user_id = 123
    
    # Step 1: Acknowledged
    notify_scrape_acknowledged(
        scrape_job_id=scrape_job_id,
        venue=venue,
        event_title=event_title,
        performance_date=performance_date,
        user_id=user_id
    )
    
    # Step 2: Queued
    notify_scrape_queued(
        scrape_job_id=scrape_job_id,
        venue=venue,
        event_title=event_title,
        performance_date=performance_date,
        queue_position=3,
        estimated_delay_minutes=5,
        user_id=user_id
    )
    
    # Step 3: Started
    notify_scrape_started(
        scrape_job_id=scrape_job_id,
        venue=venue,
        event_title=event_title,
        performance_date=performance_date,
        user_id=user_id
    )
    
    # Step 4: Progress updates
    notify_scrape_progress(
        scrape_job_id=scrape_job_id,
        venue=venue,
        step="Extracting seat data",
        completed_items=50,
        total_items=200,
        progress_type="data_extraction",
        event_title=event_title,
        performance_date=performance_date,
        user_id=user_id
    )
    
    notify_scrape_progress(
        scrape_job_id=scrape_job_id,
        venue=venue,
        step="Processing seat data",
        completed_items=150,
        total_items=200,
        progress_type="processing",
        event_title=event_title,
        performance_date=performance_date,
        user_id=user_id
    )
    
    # Step 5: Success
    scraped_data = {
        "url": "https://broadwaysf.com/hamilton",
        "performance_key": "hamilton_2024_01_01_19_00",
        "internal_performance_id": "perf_123",
        "internal_event_id": "event_456",
        "internal_venue_id": "venue_789",
        "seat_count": 200,
        "available_seats": 180,
        "event_info": {
            "title": "Hamilton",
            "date": "2024-01-01T19:00:00Z"
        }
    }
    
    notify_scrape_success(
        scrape_job_id=scrape_job_id,
        venue=venue,
        scraped_data=scraped_data,
        event_title=event_title,
        performance_date=performance_date,
        processing_time_ms=45000,
        user_id=user_id
    )


def example_scrape_with_retry():
    """
    Example of scrape with retry logic
    """
    scrape_job_id = str(uuid.uuid4())
    venue = "Washington Pavilion"
    event_title = "Les Misérables"
    user_id = 456
    
    # First attempt fails
    notify_scrape_started(
        scrape_job_id=scrape_job_id,
        venue=venue,
        event_title=event_title,
        user_id=user_id
    )
    
    # Error occurs
    notify_scrape_error(
        scrape_job_id=scrape_job_id,
        venue=venue,
        error_message="Connection timeout after 30 seconds",
        event_title=event_title,
        error_type="connection_error",
        processing_time_ms=30000,
        retryable=True,
        user_id=user_id
    )
    
    # Retry notification
    notify_scrape_retry(
        scrape_job_id=scrape_job_id,
        venue=venue,
        error_message="Connection timeout after 30 seconds",
        retry_attempt=2,
        max_retries=3,
        retry_delay_minutes=5,
        event_title=event_title,
        retry_reason="temporary_network_error",
        user_id=user_id
    )


def example_pos_sync_notifications():
    """
    Example of POS sync notifications
    """
    operation_id = str(uuid.uuid4())
    performance_id = "perf_123"
    venue = "David H. Koch Theater"
    
    # Success case
    sync_results = {
        "total_packs": 15,
        "processed": 15,
        "errors": 0,
        "created": 10,
        "updated": 3,
        "deleted": 2
    }
    
    notify_pos_sync_success(
        operation_id=operation_id,
        performance_id=performance_id,
        venue=venue,
        sync_results=sync_results,
        sync_type="create",
        event_title="Swan Lake",
        processing_time_ms=8000
    )
    
    # Error case
    notify_pos_sync_error(
        operation_id=operation_id,
        performance_id=performance_id,
        venue=venue,
        error_message="StubHub API timeout after 10 seconds",
        sync_type="create",
        error_type="api_error",
        packs_attempted=15,
        packs_successful=8,
        rollback_performed=True,
        rollback_successful=True,
        processing_time_ms=12000
    )


def example_progress_tracker():
    """
    Example using the ProgressTracker context manager
    """
    scrape_job_id = str(uuid.uuid4())
    venue = "Demo Venue"
    event_title = "Test Event"
    user_id = 789
    
    # Using the progress tracker
    with ProgressTracker(
        scrape_job_id=scrape_job_id,
        venue=venue,
        process_name="Data Extraction",
        total_items=100,
        progress_type="data_extraction",
        event_title=event_title,
        user_id=user_id
    ) as tracker:
        
        # Simulate work with progress updates
        for i in range(0, 101, 25):
            # Do some work here
            tracker.update(i, f"Processing item {i}")
            # time.sleep(1)  # Simulate work


def example_batch_notifications():
    """
    Example of batch notification processing
    """
    venue = "Multiple Venues"
    
    # Batch of scrape jobs
    scrape_jobs = [
        {
            "scrape_job_id": str(uuid.uuid4()),
            "event_title": "Hamilton",
            "user_id": 123,
            "success": True,
            "scraped_data": {"seat_count": 200, "available_seats": 180},
            "processing_time_ms": 45000
        },
        {
            "scrape_job_id": str(uuid.uuid4()),
            "event_title": "Les Misérables",
            "user_id": 456,
            "success": False,
            "error_message": "Venue website is down",
            "processing_time_ms": 15000
        },
        {
            "scrape_job_id": str(uuid.uuid4()),
            "event_title": "Phantom of the Opera",
            "user_id": 789,
            "success": True,
            "scraped_data": {"seat_count": 150, "available_seats": 120},
            "processing_time_ms": 38000
        }
    ]
    
    # Send batch start notifications
    started_count = notify_batch_scrape_started(scrape_jobs, venue)
    print(f"Started {started_count} scrape jobs")
    
    # Send batch completion notifications
    completed_count = notify_batch_scrape_completed(scrape_jobs, venue)
    print(f"Completed {completed_count} scrape jobs")


def example_advanced_usage():
    """
    Example of advanced usage with the full-featured sender
    """
    scrape_job_id = str(uuid.uuid4())
    venue = "Advanced Venue"
    
    # Using the full-featured sender for complex scenarios
    scrape_status_sender.send_scrape_progress(
        scrape_job_id=scrape_job_id,
        venue=venue,
        progress_type="validation",
        progress_percentage=75,
        current_step="Validating seat data integrity",
        event_title="Complex Event",
        performance_date=datetime.now() + timedelta(days=30),
        total_steps=4,
        completed_steps=3,
        items_processed=750,
        estimated_time_remaining="2 minutes",
        user_id=999
    )
    
    # Error with detailed information
    scrape_status_sender.send_scrape_error(
        scrape_job_id=scrape_job_id,
        venue=venue,
        error_message="Seat structure validation failed: Invalid section mapping",
        event_title="Complex Event",
        performance_date=datetime.now() + timedelta(days=30),
        error_type="validation_error",
        error_code="SEAT_STRUCTURE_INVALID",
        severity="high",
        processing_time_ms=120000,
        retryable=False,
        suggested_action="Check venue seat structure configuration",
        user_id=999
    )


def example_scraper_integration():
    """
    Example of how to integrate notifications into a scraper class
    """
    class ExampleScraper:
        def __init__(self, venue_name: str):
            self.venue_name = venue_name
        
        def scrape_performance(self, scrape_job_id: str, performance_data: Dict[str, Any]):
            """Example scraper method with full notification integration"""
            event_title = performance_data.get('event_title')
            performance_date = performance_data.get('performance_date')
            user_id = performance_data.get('user_id')
            
            # Send acknowledged notification
            notify_scrape_acknowledged(
                scrape_job_id=scrape_job_id,
                venue=self.venue_name,
                event_title=event_title,
                performance_date=performance_date,
                user_id=user_id
            )
            
            try:
                # Send started notification
                notify_scrape_started(
                    scrape_job_id=scrape_job_id,
                    venue=self.venue_name,
                    event_title=event_title,
                    performance_date=performance_date,
                    user_id=user_id
                )
                
                # Use progress tracker for the main scraping process
                with ProgressTracker(
                    scrape_job_id=scrape_job_id,
                    venue=self.venue_name,
                    process_name="Seat Data Extraction",
                    total_items=100,
                    event_title=event_title,
                    performance_date=performance_date,
                    user_id=user_id
                ) as tracker:
                    
                    # Simulate scraping work
                    for i in range(0, 101, 20):
                        # Do actual scraping work here
                        tracker.update(i, f"Extracting seats: {i}%")
                        # time.sleep(0.5)  # Simulate work
                
                # Success - send completion notification
                scraped_data = {
                    "url": f"https://{self.venue_name.lower().replace(' ', '')}.com",
                    "seat_count": 100,
                    "available_seats": 85,
                    "event_title": event_title
                }
                
                notify_scrape_success(
                    scrape_job_id=scrape_job_id,
                    venue=self.venue_name,
                    scraped_data=scraped_data,
                    event_title=event_title,
                    performance_date=performance_date,
                    processing_time_ms=5000,
                    user_id=user_id
                )
                
                return scraped_data
                
            except Exception as e:
                # Error - send error notification
                notify_scrape_error(
                    scrape_job_id=scrape_job_id,
                    venue=self.venue_name,
                    error_message=str(e),
                    event_title=event_title,
                    performance_date=performance_date,
                    user_id=user_id
                )
                raise
    
    # Usage example
    scraper = ExampleScraper("Example Venue")
    job_id = str(uuid.uuid4())
    performance_data = {
        "event_title": "Test Event",
        "performance_date": datetime.now() + timedelta(days=1),
        "user_id": 123
    }
    
    try:
        result = scraper.scrape_performance(job_id, performance_data)
        print(f"Scraping completed: {result}")
    except Exception as e:
        print(f"Scraping failed: {e}")


if __name__ == "__main__":
    # Run examples
    print("Running notification examples...")
    
    print("\n1. Complete scrape lifecycle:")
    example_scrape_lifecycle()
    
    print("\n2. Scrape with retry:")
    example_scrape_with_retry()
    
    print("\n3. POS sync notifications:")
    example_pos_sync_notifications()
    
    print("\n4. Progress tracker:")
    example_progress_tracker()
    
    print("\n5. Batch notifications:")
    example_batch_notifications()
    
    print("\n6. Advanced usage:")
    example_advanced_usage()
    
    print("\n7. Scraper integration:")
    example_scraper_integration()
    
    print("\nAll examples completed!")