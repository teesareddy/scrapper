"""
Plug-and-Play Notification Helper Functions

This module provides simple wrapper functions for the notification system.
These functions are designed to be easy to use with minimal parameters while
still providing comprehensive notification capabilities.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Union
from .scrape_status_sender import scrape_status_sender

logger = logging.getLogger(__name__)


def notify_scrape_acknowledged(
    scrape_job_id: str,
    venue: str,
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send scrape acknowledged notification.
    
    Args:
        scrape_job_id: Unique identifier for the scrape job
        venue: Venue name
        event_title: Optional event title
        performance_date: Optional performance date
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        return scrape_status_sender.send_scrape_acknowledged(
            scrape_job_id=scrape_job_id,
            venue=venue,
            event_title=event_title,
            performance_date=performance_date,
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_scrape_acknowledged: {e}")
        return False


def notify_scrape_queued(
    scrape_job_id: str,
    venue: str,
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    queue_position: Optional[int] = None,
    estimated_delay_minutes: Optional[int] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send scrape queued notification.
    
    Args:
        scrape_job_id: Unique identifier for the scrape job
        venue: Venue name
        event_title: Optional event title
        performance_date: Optional performance date
        queue_position: Optional position in queue
        estimated_delay_minutes: Optional estimated delay in minutes
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        estimated_start_time = None
        if estimated_delay_minutes:
            estimated_start_time = datetime.now() + timedelta(minutes=estimated_delay_minutes)
        
        return scrape_status_sender.send_scrape_queued(
            scrape_job_id=scrape_job_id,
            venue=venue,
            event_title=event_title,
            performance_date=performance_date,
            queue_position=queue_position,
            estimated_start_time=estimated_start_time,
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_scrape_queued: {e}")
        return False


def notify_scrape_started(
    scrape_job_id: str,
    venue: str,
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    task_id: Optional[str] = None,
    worker_id: Optional[str] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send scrape started notification.
    
    Args:
        scrape_job_id: Unique identifier for the scrape job
        venue: Venue name
        event_title: Optional event title
        performance_date: Optional performance date
        task_id: Optional task ID
        worker_id: Optional worker ID
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        return scrape_status_sender.send_scrape_started(
            scrape_job_id=scrape_job_id,
            venue=venue,
            event_title=event_title,
            performance_date=performance_date,
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_scrape_started: {e}")
        return False


def notify_scrape_progress(
    scrape_job_id: str,
    venue: str,
    step: str,
    completed_items: Optional[int] = None,
    total_items: Optional[int] = None,
    progress_percentage: Optional[int] = None,
    progress_type: str = "processing",
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    estimated_time_remaining: Optional[str] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send scrape progress notification with auto-calculation.
    
    Args:
        scrape_job_id: Unique identifier for the scrape job
        venue: Venue name
        step: Current step description
        completed_items: Optional number of completed items
        total_items: Optional total number of items
        progress_percentage: Optional progress percentage (0-100)
        progress_type: Type of progress (data_extraction, processing, validation)
        event_title: Optional event title
        performance_date: Optional performance date
        estimated_time_remaining: Optional estimated time remaining
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Auto-calculate percentage if not provided
        if progress_percentage is None and completed_items is not None and total_items is not None:
            progress_percentage = min(100, max(0, int((completed_items / total_items) * 100)))
        
        # Default percentage if still not available
        if progress_percentage is None:
            progress_percentage = 0
        
        return scrape_status_sender.send_scrape_progress(
            scrape_job_id=scrape_job_id,
            venue=venue,
            progress_type=progress_type,
            progress_percentage=progress_percentage,
            current_step=step,
            event_title=event_title,
            performance_date=performance_date,
            completed_steps=completed_items,
            total_steps=total_items,
            items_processed=completed_items,
            estimated_time_remaining=estimated_time_remaining,
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_scrape_progress: {e}")
        return False


def notify_scrape_retry(
    scrape_job_id: str,
    venue: str,
    error_message: str,
    retry_attempt: int,
    max_retries: int,
    retry_delay_minutes: int = 5,
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    retry_reason: Optional[str] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send scrape retry notification.
    
    Args:
        scrape_job_id: Unique identifier for the scrape job
        venue: Venue name
        error_message: Error message from previous attempt
        retry_attempt: Current retry attempt number
        max_retries: Maximum number of retries
        retry_delay_minutes: Delay before retry in minutes
        event_title: Optional event title
        performance_date: Optional performance date
        retry_reason: Optional reason for retry
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        retry_delay_seconds = retry_delay_minutes * 60
        next_retry_time = datetime.now() + timedelta(seconds=retry_delay_seconds)
        
        return scrape_status_sender.send_scrape_retry(
            scrape_job_id=scrape_job_id,
            venue=venue,
            error_message=error_message,
            retry_attempt=retry_attempt,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            event_title=event_title,
            performance_date=performance_date,
            retry_reason=retry_reason,
            next_retry_time=next_retry_time,
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_scrape_retry: {e}")
        return False


def notify_scrape_success(
    scrape_job_id: str,
    venue: str,
    scraped_data: Dict[str, Any],
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    processing_time_ms: Optional[int] = None,
    items_scraped: Optional[int] = None,
    seats_available: Optional[int] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send scrape success notification.
    
    Args:
        scrape_job_id: Unique identifier for the scrape job
        venue: Venue name
        scraped_data: Scraped data results
        event_title: Optional event title
        performance_date: Optional performance date
        processing_time_ms: Optional processing time in milliseconds
        items_scraped: Optional number of items scraped
        seats_available: Optional number of seats available
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Auto-extract common fields if not provided
        if event_title is None:
            event_title = scraped_data.get('event_title') or scraped_data.get('event_info', {}).get('title')
        
        if items_scraped is None:
            items_scraped = scraped_data.get('items_scraped') or scraped_data.get('seat_count')
        
        if seats_available is None:
            seats_available = scraped_data.get('seats_available') or scraped_data.get('available_seats')
        
        return scrape_status_sender.send_scrape_success(
            scrape_job_id=scrape_job_id,
            venue=venue,
            result_data=scraped_data,
            event_title=event_title,
            performance_date=performance_date,
            processing_time_ms=processing_time_ms,
            items_scraped=items_scraped,
            seats_available=seats_available,
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_scrape_success: {e}")
        return False


def notify_scrape_error(
    scrape_job_id: str,
    venue: str,
    error_message: str,
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    error_type: Optional[str] = None,
    error_code: Optional[str] = None,
    severity: str = "high",
    processing_time_ms: Optional[int] = None,
    retryable: bool = True,
    suggested_action: Optional[str] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send scrape error notification.
    
    Args:
        scrape_job_id: Unique identifier for the scrape job
        venue: Venue name
        error_message: Error message
        event_title: Optional event title
        performance_date: Optional performance date
        error_type: Optional error type
        error_code: Optional error code
        severity: Error severity (low, medium, high, critical)
        processing_time_ms: Optional processing time in milliseconds
        retryable: Whether the error is retryable
        suggested_action: Optional suggested action
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Auto-determine error type if not provided
        if error_type is None:
            error_message_lower = error_message.lower()
            if any(keyword in error_message_lower for keyword in ['timeout', 'connection', 'network']):
                error_type = "connection_error"
            elif any(keyword in error_message_lower for keyword in ['parse', 'parsing', 'invalid']):
                error_type = "parsing_error"
            elif any(keyword in error_message_lower for keyword in ['validation', 'validate']):
                error_type = "validation_error"
            else:
                error_type = "unknown_error"
        
        # Auto-generate error code if not provided
        if error_code is None:
            error_code = f"SCRAPE_{error_type.upper()}"
        
        return scrape_status_sender.send_scrape_error(
            scrape_job_id=scrape_job_id,
            venue=venue,
            error_message=error_message,
            event_title=event_title,
            performance_date=performance_date,
            error_type=error_type,
            error_code=error_code,
            severity=severity,
            processing_time_ms=processing_time_ms,
            retryable=retryable,
            suggested_action=suggested_action,
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_scrape_error: {e}")
        return False


def notify_pos_sync_success(
    operation_id: str,
    performance_id: str,
    venue: str,
    sync_results: Dict[str, Any],
    sync_type: str = "sync",
    event_title: Optional[str] = None,
    performance_date: Optional[datetime] = None,
    processing_time_ms: Optional[int] = None,
) -> bool:
    """
    Simple function to send POS sync success notification.
    
    Args:
        operation_id: Unique identifier for the sync operation
        performance_id: Performance ID
        venue: Venue name
        sync_results: Sync results dictionary
        sync_type: Type of sync (create, update, delete, sync)
        event_title: Optional event title
        performance_date: Optional performance date
        processing_time_ms: Optional processing time in milliseconds
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Auto-extract packs synced from results
        packs_synced = sync_results.get('processed', 0) or sync_results.get('total_packs', 0)
        
        return scrape_status_sender.send_pos_sync_success(
            operation_id=operation_id,
            performance_id=performance_id,
            venue=venue,
            sync_type=sync_type,
            packs_synced=packs_synced,
            sync_results=sync_results,
            event_title=event_title,
            performance_date=performance_date,
            processing_time_ms=processing_time_ms
        )
    except Exception as e:
        logger.error(f"Error in notify_pos_sync_success: {e}")
        return False


def notify_pos_sync_error(
    operation_id: str,
    performance_id: str,
    venue: str,
    error_message: str,
    sync_type: str = "sync",
    event_title: Optional[str] = None,
    error_type: Optional[str] = None,
    error_code: Optional[str] = None,
    severity: str = "high",
    packs_attempted: Optional[int] = None,
    packs_successful: Optional[int] = None,
    rollback_performed: bool = False,
    rollback_successful: bool = False,
    processing_time_ms: Optional[int] = None,
    retryable: bool = True,
    suggested_action: Optional[str] = None,
) -> bool:
    """
    Simple function to send POS sync error notification.
    
    Args:
        operation_id: Unique identifier for the sync operation
        performance_id: Performance ID
        venue: Venue name
        error_message: Error message
        sync_type: Type of sync (create, update, delete, sync)
        event_title: Optional event title
        performance_date: Optional performance date
        error_type: Optional error type
        error_code: Optional error code
        severity: Error severity (low, medium, high, critical)
        packs_attempted: Optional number of packs attempted
        packs_successful: Optional number of packs successful
        rollback_performed: Whether rollback was performed
        rollback_successful: Whether rollback was successful
        processing_time_ms: Optional processing time in milliseconds
        retryable: Whether the error is retryable
        suggested_action: Optional suggested action
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Auto-determine error type if not provided
        if error_type is None:
            error_message_lower = error_message.lower()
            if any(keyword in error_message_lower for keyword in ['api', 'stubhub', 'pos']):
                error_type = "api_error"
            elif any(keyword in error_message_lower for keyword in ['timeout', 'connection', 'network']):
                error_type = "network_error"
            elif any(keyword in error_message_lower for keyword in ['validation', 'validate']):
                error_type = "validation_error"
            elif any(keyword in error_message_lower for keyword in ['rollback']):
                error_type = "rollback_error"
            else:
                error_type = "unknown_error"
        
        # Auto-generate error code if not provided
        if error_code is None:
            error_code = f"POS_{error_type.upper()}"
        
        # Calculate failed packs if not provided
        packs_failed = None
        if packs_attempted is not None and packs_successful is not None:
            packs_failed = packs_attempted - packs_successful
        
        return scrape_status_sender.send_pos_sync_error(
            operation_id=operation_id,
            performance_id=performance_id,
            venue=venue,
            sync_type=sync_type,
            error_message=error_message,
            event_title=event_title,
            performance_date=performance_date,
            error_type=error_type,
            error_code=error_code,
            severity=severity,
            packs_attempted=packs_attempted,
            packs_successful=packs_successful,
            packs_failed=packs_failed,
            rollback_performed=rollback_performed,
            rollback_successful=rollback_successful,
            processing_time_ms=processing_time_ms,
            retryable=retryable,
            suggested_action=suggested_action
        )
    except Exception as e:
        logger.error(f"Error in notify_pos_sync_error: {e}")
        return False


# Context manager for automatic progress tracking
class ProgressTracker:
    """
    Context manager for automatic progress tracking during scraping operations.
    
    Usage:
        with ProgressTracker(scrape_job_id, venue, "Data Extraction", 100) as tracker:
            # Do work
            tracker.update(25, "Extracting seats")
            # More work
            tracker.update(50, "Processing data")
            # Etc.
    """
    
    def __init__(
        self,
        scrape_job_id: str,
        venue: str,
        process_name: str,
        total_items: int,
        progress_type: str = "processing",
        event_title: Optional[str] = None,

        user_id: Optional[int] = None,
    ):
        self.scrape_job_id = scrape_job_id
        self.venue = venue
        self.process_name = process_name
        self.total_items = total_items
        self.progress_type = progress_type
        self.event_title = event_title
        self.performance_date = performance_date
        self.user_id = user_id
        self.current_items = 0
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        notify_scrape_progress(
            scrape_job_id=self.scrape_job_id,
            venue=self.venue,
            step=f"Starting {self.process_name}",
            completed_items=0,
            total_items=self.total_items,
            progress_type=self.progress_type,
            event_title=self.event_title,
            performance_date=self.performance_date,
            user_id=self.user_id
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Successful completion
            notify_scrape_progress(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue,
                step=f"Completed {self.process_name}",
                completed_items=self.total_items,
                total_items=self.total_items,
                progress_type=self.progress_type,
                event_title=self.event_title,
                performance_date=self.performance_date,
                user_id=self.user_id
            )
        else:
            # Error occurred
            notify_scrape_error(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue,
                error_message=str(exc_val),
                event_title=self.event_title,
                performance_date=self.performance_date,
                processing_time_ms=int((datetime.now() - self.start_time).total_seconds() * 1000) if self.start_time else None,
                user_id=self.user_id
            )
    
    def update(self, completed_items: int, current_step: str):
        """Update progress with current status."""
        self.current_items = completed_items
        
        # Calculate estimated time remaining
        estimated_time_remaining = None
        if self.start_time and completed_items > 0:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            estimated_total_time = elapsed_time * (self.total_items / completed_items)
            remaining_time = estimated_total_time - elapsed_time
            if remaining_time > 0:
                estimated_time_remaining = f"{int(remaining_time // 60)}m {int(remaining_time % 60)}s"
        
        notify_scrape_progress(
            scrape_job_id=self.scrape_job_id,
            venue=self.venue,
            step=current_step,
            completed_items=completed_items,
            total_items=self.total_items,
            progress_type=self.progress_type,
            event_title=self.event_title,
            performance_date=self.performance_date,
            estimated_time_remaining=estimated_time_remaining,
            user_id=self.user_id
        )


# Batch notification functions for bulk operations
def notify_batch_scrape_started(scrape_jobs: list, venue: str) -> int:
    """
    Send started notifications for multiple scrape jobs.
    
    Args:
        scrape_jobs: List of scrape job dictionaries
        venue: Venue name
        
    Returns:
        int: Number of successful notifications sent
    """
    success_count = 0
    for job in scrape_jobs:
        if notify_scrape_started(
            scrape_job_id=job['scrape_job_id'],
            venue=venue,
            event_title=job.get('event_title'),
            
            user_id=job.get('user_id')
        ):
            success_count += 1
    return success_count


def notify_batch_scrape_completed(scrape_jobs: list, venue: str) -> int:
    """
    Send completion notifications for multiple scrape jobs.
    
    Args:
        scrape_jobs: List of scrape job dictionaries with results
        venue: Venue name
        
    Returns:
        int: Number of successful notifications sent
    """
    success_count = 0
    for job in scrape_jobs:
        if job.get('success', False):
            if notify_scrape_success(
                scrape_job_id=job['scrape_job_id'],
                venue=venue,
                scraped_data=job.get('scraped_data', {}),
                event_title=job.get('event_title'),
                
                processing_time_ms=job.get('processing_time_ms'),
                user_id=job.get('user_id')
            ):
                success_count += 1
        else:
            if notify_scrape_error(
                scrape_job_id=job['scrape_job_id'],
                venue=venue,
                error_message=job.get('error_message', 'Unknown error'),
                event_title=job.get('event_title'),
                
                processing_time_ms=job.get('processing_time_ms'),
                user_id=job.get('user_id')
            ):
                success_count += 1
    return success_count


def notify_pos_sync_completed(
    scrape_job_id: str,
    performance_id: str,
    operation_id: str,
    success: bool,
    venue: str,
    event_title: Optional[str] = None,
    performance_date: Optional[str] = None,
    sync_results: Optional[Dict[str, Any]] = None,
    workflow_summary: Optional[Dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> bool:
    """
    Simple function to send POS sync completion notification.
    
    Args:
        scrape_job_id: Scrape job identifier
        performance_id: Performance ID
        operation_id: Operation/workflow ID
        success: Whether the sync completed successfully
        venue: Venue name
        event_title: Optional event title
        performance_date: Optional performance date
        sync_results: Dictionary with sync operation results
        workflow_summary: Dictionary with workflow summary
        user_id: Optional user ID who initiated the job
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        return scrape_status_sender.send_pos_sync_completed(
            scrape_job_id=scrape_job_id,
            performance_id=performance_id,
            operation_id=operation_id,
            success=success,
            venue=venue,
            event_title=event_title,
            performance_date=performance_date,
            sync_results=sync_results or {},
            workflow_summary=workflow_summary or {},
            user_id=user_id
        )
    except Exception as e:
        logger.error(f"Error in notify_pos_sync_completed: {e}")
        return False