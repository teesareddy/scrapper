from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional

from django.utils import timezone


class MessagePattern(Enum):
    SCRAPE_REQUEST = "scrape.performance.request"
    SCRAPE_ACKNOWLEDGED = "scrape.performance.acknowledged"
    SCRAPE_QUEUED = "scrape.performance.queued"
    SCRAPE_STARTED = "scrape.performance.started"
    SCRAPE_SUCCESS = "scrape.performance.success"
    SCRAPE_ERROR = "scrape.performance.error"
    SCRAPE_RETRY = "scrape.performance.retry"
    SCRAPE_PROGRESS = "scrape.performance.progress"
    POS_SYNC_SUCCESS = "pos.sync.success"
    POS_SYNC_STARTED = "pos.sync.started"
    POS_SYNC_ERROR = "pos.sync.error"
    POS_SYNC_COMPLETED = "pos.sync.completed"


class MessageStatus(Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class MessageResponse:
    pattern: str
    status: str
    data: Dict[str, Any]
    error: Optional[str] = None
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "pattern": self.pattern,
            "status": self.status,
            "data": self.data
        }
        if self.error:
            result["error"] = self.error
        if self.timestamp:
            result["timestamp"] = self.timestamp
        return result


def create_acknowledgment_message(scrape_job_id: str, enriched_data: dict = None,
                                  user_id: str = None) -> MessageResponse:
    # Create user-friendly message based on enriched data
    if enriched_data:
        event_name = enriched_data.get('eventName', 'event')
        performance_id = enriched_data.get('performanceId')

        # Build contextual message with performance ID if available
        if enriched_data.get('posEnabled'):
            if performance_id:
                message = f"Manual scraping with POS sync initiated for {event_name}"
            else:
                message = f"Manual scraping with POS sync initiated for {event_name}"
        else:
            if performance_id:
                message = f"Manual scraping initiated for {event_name}"
            else:
                message = f"Manual scraping initiated for {event_name}"
    else:
        message = "Scrape request received and queued for processing"

    return MessageResponse(
        pattern=MessagePattern.SCRAPE_ACKNOWLEDGED.value,
        status=MessageStatus.QUEUED.value,
        data={
            "scrapeJobId": scrape_job_id,
            "userId": user_id,
            "message": message
        },
        timestamp=timezone.now().isoformat()
    )


def create_start_message(scrape_job_id: str, task_id: str, enriched_data: dict = None, user_id: str = None,
                         pattern: str = MessagePattern.SCRAPE_STARTED.value) -> MessageResponse:
    # Create user-friendly message based on enriched data
    if enriched_data:
        event_name = enriched_data.get('eventName', 'event')
        performance_id = enriched_data.get('performanceId')

        # Build contextual message
        if enriched_data.get('posEnabled'):
            if performance_id:
                message = f"Scraping with POS sync in progress for {event_name}"
            else:
                message = f"Scraping with POS sync in progress for {event_name}"
        else:
            if performance_id:
                message = f"Scraping in progress for {event_name}"
            else:
                message = f"Scraping in progress for {event_name}"
    else:
        message = "Scraping has started"

    return MessageResponse(
        pattern=pattern,
        status=MessageStatus.PROCESSING.value,
        data={
            "scrapeJobId": scrape_job_id,
            "taskId": task_id,
            "userId": user_id,
            "message": message
        },
        timestamp=timezone.now().isoformat()
    )


def create_success_message(scrape_job_id: str, result_data: Dict[str, Any], enriched_data: dict = None,
                           user_id: str = None) -> MessageResponse:
    # Create contextual success message based on enriched data and results
    message = "Scraping completed successfully"

    if enriched_data:
        event_name = enriched_data.get('eventName', 'event')

        # Extract meaningful context from result data
        seat_count = None
        venue_name = None

        if result_data:
            # Try to get seat count from various possible fields
            seats = result_data.get('seats', [])
            if isinstance(seats, list):
                seat_count = len(seats)
            elif isinstance(seats, dict):
                seat_count = len(seats.get('seats', []))

            # Try to get venue name
            venue_info = result_data.get('venue_info', {})
            if isinstance(venue_info, dict):
                venue_name = venue_info.get('name')
            elif isinstance(venue_info, str):
                venue_name = venue_info

        # Build contextual message
        if seat_count and venue_name:
            message = f"{event_name} scraping completed successfully - {seat_count:,} seats available at {venue_name}"
        elif seat_count:
            message = f"{event_name} scraping completed successfully - {seat_count:,} seats found"
        elif venue_name:
            message = f"{event_name} scraping completed successfully at {venue_name}"
        else:
            message = f"{event_name} scraping completed successfully"

    return MessageResponse(
        pattern=MessagePattern.SCRAPE_SUCCESS.value,
        status=MessageStatus.COMPLETED.value,
        data={
            "scrapeJobId": scrape_job_id,
            "userId": user_id,
            "result": result_data,
            "message": message
        },
        timestamp=timezone.now().isoformat()
    )


def create_error_message(scrape_job_id: str, error_msg: str, enriched_data: dict = None,
                         user_id: str = None) -> MessageResponse:
    # Create user-friendly message based on enriched data
    if enriched_data:
        event_name = enriched_data.get('eventName', 'event')
        performance_id = enriched_data.get('performanceId')

        # Determine error context based on error message
        error_context = ""
        error_lower = error_msg.lower()

        if "timeout" in error_lower or "network" in error_lower:
            error_context = " - Network timeout during data extraction"
        elif "parse" in error_lower or "extract" in error_lower:
            error_context = " - Failed to extract seat map data"
        elif "seat" in error_lower:
            error_context = " - Seat data processing failed"
        elif "browser" in error_lower or "playwright" in error_lower:
            error_context = " - Browser navigation failed"
        else:
            error_context = " - Data extraction failed"

        # Build contextual error message
        if performance_id:
            user_friendly_msg = f"Scraping failed for {event_name} (Performance ID: {performance_id}){error_context}"
        else:
            user_friendly_msg = f"Scraping failed for {event_name}{error_context}"
    else:
        user_friendly_msg = "Scraping failed"

    return MessageResponse(
        pattern=MessagePattern.SCRAPE_ERROR.value,
        status=MessageStatus.FAILED.value,
        data={
            "scrapeJobId": scrape_job_id,
            "userId": user_id,
            "message": user_friendly_msg
        },
        error=error_msg,
        timestamp=timezone.now().isoformat()
    )


def create_retry_message(scrape_job_id: str, error_msg: str, retry_attempt: int, max_retries: int, retry_delay: int,
                         enriched_data: dict = None, user_id: str = None) -> MessageResponse:
    # Create user-friendly message based on enriched data
    if enriched_data:
        event_name = enriched_data.get('eventName', 'event')
        performance_id = enriched_data.get('performanceId')

        # Determine retry reason based on error
        retry_reason = ""
        error_lower = error_msg.lower()

        if "timeout" in error_lower:
            retry_reason = " due to network timeout"
        elif "connection" in error_lower:
            retry_reason = " due to connection issues"
        elif "parse" in error_lower:
            retry_reason = " due to data parsing issues"
        else:
            retry_reason = " due to temporary issues"

        # Format retry time
        if retry_delay >= 60:
            time_str = f"{retry_delay // 60}m {retry_delay % 60}s"
        else:
            time_str = f"{retry_delay}s"

        # Build contextual retry message
        if performance_id:
            message = f"Retrying scrape for {event_name} (Performance ID: {performance_id}){retry_reason} - Attempt {retry_attempt}/{max_retries} in {time_str}"
        else:
            message = f"Retrying scrape for {event_name}{retry_reason} - Attempt {retry_attempt}/{max_retries} in {time_str}"
    else:
        message = f"Retrying scrape (attempt {retry_attempt}/{max_retries}) in {retry_delay} seconds"

    return MessageResponse(
        pattern=MessagePattern.SCRAPE_RETRY.value,
        status=MessageStatus.QUEUED.value,
        data={
            "scrapeJobId": scrape_job_id,
            "userId": user_id,
            "retryAttempt": retry_attempt,
            "maxRetries": max_retries,
            "retryDelaySeconds": retry_delay,
            "message": message
        },
        error=error_msg,
        timestamp=timezone.now().isoformat()
    )


def create_queued_message(scrape_job_id: str, venue: str, event_title: str = None, queue_position: int = None,
                          user_id: str = None) -> MessageResponse:
    return MessageResponse(
        pattern=MessagePattern.SCRAPE_QUEUED.value,
        status=MessageStatus.QUEUED.value,
        data={
            "scrapeJobId": scrape_job_id,
            "userId": user_id,
            "venue": venue,
            "eventTitle": event_title,
            "queuePosition": queue_position,
            "message": "Scrape job queued for processing"
        },
        timestamp=timezone.now().isoformat()
    )


def create_progress_message(scrape_job_id: str, venue: str, progress_type: str, progress_percentage: int,
                            current_step: str, user_id: str = None, **kwargs) -> MessageResponse:
    data = {
        "scrapeJobId": scrape_job_id,
        "userId": user_id,
        "venue": venue,
        "progressType": progress_type,
        "progressPercentage": progress_percentage,
        "currentStep": current_step,
        "message": f"{current_step} - {progress_percentage}% complete"
    }

    # Add optional fields
    for key, value in kwargs.items():
        if value is not None:
            data[key] = value

    return MessageResponse(
        pattern=MessagePattern.SCRAPE_PROGRESS.value,
        status=MessageStatus.PROCESSING.value,
        data=data,
        timestamp=timezone.now().isoformat()
    )


def create_pos_sync_success_message(operation_id: str, performance_id: str, venue: str, sync_type: str,
                                    packs_synced: int, sync_results: dict, **kwargs) -> MessageResponse:
    data = {
        "operationId": operation_id,
        "performanceId": performance_id,
        "venue": venue,
        "syncType": sync_type,
        "packsSynced": packs_synced,
        "syncResults": sync_results,
        "message": "POS sync completed successfully"
    }

    # Add optional fields
    for key, value in kwargs.items():
        if value is not None:
            data[key] = value

    return MessageResponse(
        pattern=MessagePattern.POS_SYNC_SUCCESS.value,
        status=MessageStatus.COMPLETED.value,
        data=data,
        timestamp=timezone.now().isoformat()
    )


def create_pos_sync_error_message(operation_id: str, performance_id: str, venue: str, sync_type: str, error_msg: str,
                                  **kwargs) -> MessageResponse:
    data = {
        "operationId": operation_id,
        "performanceId": performance_id,
        "venue": venue,
        "syncType": sync_type,
        "errorType": kwargs.get("error_type", "unknown"),
        "errorCode": kwargs.get("error_code", "POS_SYNC_ERROR"),
        "severity": kwargs.get("severity", "high"),
        "retryable": kwargs.get("retryable", True)
    }

    # Add optional fields
    for key, value in kwargs.items():
        if value is not None and key not in ["error_type", "error_code", "severity", "retryable"]:
            data[key] = value

    return MessageResponse(
        pattern=MessagePattern.POS_SYNC_ERROR.value,
        status=MessageStatus.FAILED.value,
        data=data,
        error=error_msg,
        timestamp=timezone.now().isoformat()
    )


def create_pos_sync_completed_message(
        scrapeJobId: str,
        performanceId: str,
        operationId: str,
        success: bool,
        venue: str,
        eventTitle: Optional[str] = None,
        performanceDate: Optional[str] = None,
        syncResults: Optional[Dict[str, Any]] = None,
        workflowSummary: Optional[Dict[str, Any]] = None,
        userId: Optional[int] = None,
) -> MessageResponse:
    """
    Create POS sync completion notification message for NestJS backend
    
    Args:
        scrapeJobId: Scrape job identifier
        performanceId: Performance ID
        operationId: Operation/workflow ID
        success: Whether the sync completed successfully
        venue: Venue name
        eventTitle: Optional event title
        performanceDate: Optional performance date
        syncResults: Dictionary with sync operation results
        workflowSummary: Dictionary with workflow summary
        userId: Optional user ID who initiated the job
        
    Returns:
        MessageResponse with POS sync completion data
    """
    data = {
        "scrapeJobId": scrapeJobId,
        "performanceId": performanceId,
        "operationId": operationId,
        "success": success,
        "venue": venue,
        "eventTitle": eventTitle,
        "performanceDate": performanceDate,
        "syncResults": syncResults or {},
        "workflowSummary": workflowSummary or {},
        "userId": userId
    }

    # Create user-friendly message based on success status
    if success:
        if eventTitle:
            message = f"POS sync completed successfully for {eventTitle} at {venue}"
        else:
            message = f"POS sync completed successfully at {venue}"
    else:
        if eventTitle:
            message = f"POS sync failed for {eventTitle} at {venue}"
        else:
            message = f"POS sync failed at {venue}"

    data["message"] = message

    return MessageResponse(
        pattern=MessagePattern.POS_SYNC_COMPLETED.value,
        status=MessageStatus.COMPLETED.value if success else MessageStatus.FAILED.value,
        data=data,
        timestamp=timezone.now().isoformat()
    )
