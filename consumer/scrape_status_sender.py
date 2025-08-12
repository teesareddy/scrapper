import logging
from datetime import datetime
from typing import Dict, Any, Optional

from .message_types import (
    create_acknowledgment_message,
    create_queued_message,
    create_progress_message,
    create_success_message,
    create_error_message,
    create_retry_message,
    create_pos_sync_success_message,
    create_pos_sync_error_message
)
from .rabbitmq_producer import RabbitMQProducer

logger = logging.getLogger(__name__)


class ScrapeStatusSender:
    """
    Service to send scrape status updates to NestJS backend via RabbitMQ
    """

    def __init__(self):
        self.producer = RabbitMQProducer()

    def send_status_update(
            self,
            scrape_job_id: str,
            status: str,
            venue: str,
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            data_extracted: Optional[Dict[str, Any]] = None,
            error_details: Optional[str] = None,
            processing_time_ms: Optional[int] = None,
            items_scraped: Optional[int] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """
        Send scrape status update to NestJS backend
        
        Args:
            scrape_job_id: Unique identifier for the scrape job
            status: Job status (queued, processing, completed, failed, cancelled)
            venue: Venue name being scraped
            event_title: Optional event title
            performance_date: Optional performance date
            data_extracted: Optional extracted data
            error_details: Optional error message if failed
            processing_time_ms: Optional processing time in milliseconds
            items_scraped: Optional number of items scraped
            user_id: Optional user ID who initiated the job
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = {
                'scrape_job_id': scrape_job_id,
                'status': status.lower(),
                'venue': venue,
                'timestamp': datetime.now().isoformat(),
            }

            # Add optional fields if provided
            if event_title:
                message['event_title'] = event_title
            if performance_date:
                message['performance_date'] = performance_date.isoformat()
            if data_extracted:
                message['data_extracted'] = data_extracted
            if error_details:
                message['error_details'] = error_details
            if processing_time_ms is not None:
                message['processing_time_ms'] = processing_time_ms
            if items_scraped is not None:
                message['items_scraped'] = items_scraped
            if user_id is not None:
                message['user_id'] = user_id

            # Send to scrape status updates queue
            success = self.producer.publish_message(
                queue_name='scrape_status_updates',
                message=message,
                routing_key='scrape.status.update'
            )

            if success:
                logger.info(f"Sent status update for job {scrape_job_id}: {status}")
            else:
                logger.error(f"Failed to send status update for job {scrape_job_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending status update for job {scrape_job_id}: {str(e)}")
            return False

    def send_performance_data(
            self,
            venue: str,
            event_title: str,
            performance_data: Dict[str, Any],
            scrape_job_id: Optional[str] = None,
    ) -> bool:
        """
        Send performance data update to NestJS backend
        
        Args:
            venue: Venue name
            event_title: Event title
            performance_data: Performance/seat availability data
            scrape_job_id: Optional scrape job ID
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = {
                'venue': venue,
                'event_title': event_title,
                'performance_data': performance_data,
                'timestamp': datetime.now().isoformat(),
            }

            if scrape_job_id:
                message['scrape_job_id'] = scrape_job_id

            success = self.producer.publish_message(
                queue_name='performance_data_updates',
                message=message,
                routing_key='performance.data.update'
            )

            if success:
                logger.info(f"Sent performance data for {event_title} at {venue}")
            else:
                logger.error(f"Failed to send performance data for {event_title} at {venue}")

            return success

        except Exception as e:
            logger.error(f"Error sending performance data: {str(e)}")
            return False

    def send_error_notification(
            self,
            error_type: str,
            error_message: str,
            venue: Optional[str] = None,
            scrape_job_id: Optional[str] = None,
            user_id: Optional[int] = None,
            severity: str = 'medium',
    ) -> bool:
        """
        Send error notification to NestJS backend
        
        Args:
            error_type: Type of error (e.g., 'scraping_error', 'connection_error')
            error_message: Detailed error message
            venue: Optional venue name
            scrape_job_id: Optional scrape job ID
            user_id: Optional user ID
            severity: Error severity (low, medium, high, critical)
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = {
                'error_type': error_type,
                'error_message': error_message,
                'severity': severity.lower(),
                'timestamp': datetime.now().isoformat(),
            }

            if venue:
                message['venue'] = venue
            if scrape_job_id:
                message['scrape_job_id'] = scrape_job_id
            if user_id is not None:
                message['user_id'] = user_id

            success = self.producer.publish_message(
                queue_name='error_notifications',
                message=message,
                routing_key='error.notification'
            )

            if success:
                logger.info(f"Sent error notification: {error_type}")
            else:
                logger.error(f"Failed to send error notification: {error_type}")

            return success

        except Exception as e:
            logger.error(f"Error sending error notification: {str(e)}")
            return False

    def send_scrape_started(
            self,
            scrape_job_id: str,
            venue: str,
            event_title: Optional[str] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """Convenience method to send scrape started notification"""
        return self.send_status_update(
            scrape_job_id=scrape_job_id,
            status='processing',
            venue=venue,
            event_title=event_title,
            user_id=user_id,
        )

    def send_scrape_completed(
            self,
            scrape_job_id: str,
            venue: str,
            event_title: Optional[str] = None,
            data_extracted: Optional[Dict[str, Any]] = None,
            processing_time_ms: Optional[int] = None,
            items_scraped: Optional[int] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """Convenience method to send scrape completed notification"""
        return self.send_status_update(
            scrape_job_id=scrape_job_id,
            status='completed',
            venue=venue,
            event_title=event_title,
            data_extracted=data_extracted,
            processing_time_ms=processing_time_ms,
            items_scraped=items_scraped,
            user_id=user_id,
        )

    def send_scrape_failed(
            self,
            scrape_job_id: str,
            venue: str,
            error_details: str,
            event_title: Optional[str] = None,
            processing_time_ms: Optional[int] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """Convenience method to send scrape failed notification"""
        success = self.send_status_update(
            scrape_job_id=scrape_job_id,
            status='failed',
            venue=venue,
            event_title=event_title,
            error_details=error_details,
            processing_time_ms=processing_time_ms,
            user_id=user_id,
        )

        # Also send error notification
        self.send_error_notification(
            error_type='scraping_error',
            error_message=error_details,
            venue=venue,
            scrape_job_id=scrape_job_id,
            user_id=user_id,
            severity='high',
        )

        return success

    def send_scrape_acknowledged(
            self,
            scrape_job_id: str,
            venue: str,
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """
        Send scrape acknowledged notification
        
        Args:
            scrape_job_id: Unique identifier for the scrape job
            venue: Venue name
            event_title: Optional event title
            performance_date: Optional performance date
            user_id: Optional user ID who initiated the job
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_acknowledgment_message(
                scrape_job_id=scrape_job_id,
                user_id=str(user_id) if user_id else None
            )

            # Add venue and event details
            message.data.update({
                "venue": venue,
                "eventTitle": event_title,
                "performanceDate": performance_date.isoformat() if performance_date else None
            })

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(f"Sent scrape acknowledged notification for job {scrape_job_id}")
            else:
                logger.error(f"Failed to send scrape acknowledged notification for job {scrape_job_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending scrape acknowledged notification for job {scrape_job_id}: {str(e)}")
            return False

    def send_scrape_queued(
            self,
            scrape_job_id: str,
            venue: str,
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            queue_position: Optional[int] = None,
            estimated_start_time: Optional[datetime] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """
        Send scrape queued notification
        
        Args:
            scrape_job_id: Unique identifier for the scrape job
            venue: Venue name
            event_title: Optional event title
            performance_date: Optional performance date
            queue_position: Optional position in queue
            estimated_start_time: Optional estimated start time
            user_id: Optional user ID who initiated the job
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_queued_message(
                scrape_job_id=scrape_job_id,
                venue=venue,
                event_title=event_title,
                queue_position=queue_position,
                user_id=str(user_id) if user_id else None
            )

            # Add additional fields
            if performance_date:
                message.data["performanceDate"] = performance_date.isoformat()
            if estimated_start_time:
                message.data["estimatedStartTime"] = estimated_start_time.isoformat()

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(f"Sent scrape queued notification for job {scrape_job_id}")
            else:
                logger.error(f"Failed to send scrape queued notification for job {scrape_job_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending scrape queued notification for job {scrape_job_id}: {str(e)}")
            return False

    def send_scrape_progress(
            self,
            scrape_job_id: str,
            venue: str,
            progress_type: str,
            progress_percentage: int,
            current_step: str,
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            total_steps: Optional[int] = None,
            completed_steps: Optional[int] = None,
            items_processed: Optional[int] = None,
            estimated_time_remaining: Optional[str] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """
        Send scrape progress notification
        
        Args:
            scrape_job_id: Unique identifier for the scrape job
            venue: Venue name
            progress_type: Type of progress (data_extraction, processing, validation)
            progress_percentage: Progress percentage (0-100)
            current_step: Current step description
            event_title: Optional event title
            performance_date: Optional performance date
            total_steps: Optional total number of steps
            completed_steps: Optional number of completed steps
            items_processed: Optional number of items processed
            estimated_time_remaining: Optional estimated time remaining
            user_id: Optional user ID who initiated the job
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_progress_message(
                scrape_job_id=scrape_job_id,
                venue=venue,
                progress_type=progress_type,
                progress_percentage=progress_percentage,
                current_step=current_step,
                user_id=str(user_id) if user_id else None,
                eventTitle=event_title,
                performanceDate=performance_date.isoformat() if performance_date else None,
                totalSteps=total_steps,
                completedSteps=completed_steps,
                itemsProcessed=items_processed,
                estimatedTimeRemaining=estimated_time_remaining
            )

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(f"Sent scrape progress notification for job {scrape_job_id}: {progress_percentage}%")
            else:
                logger.error(f"Failed to send scrape progress notification for job {scrape_job_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending scrape progress notification for job {scrape_job_id}: {str(e)}")
            return False

    def send_scrape_retry(
            self,
            scrape_job_id: str,
            venue: str,
            error_message: str,
            retry_attempt: int,
            max_retries: int,
            retry_delay_seconds: int,
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            retry_reason: Optional[str] = None,
            next_retry_time: Optional[datetime] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """
        Send scrape retry notification
        
        Args:
            scrape_job_id: Unique identifier for the scrape job
            venue: Venue name
            error_message: Error message from previous attempt
            retry_attempt: Current retry attempt number
            max_retries: Maximum number of retries
            retry_delay_seconds: Delay before retry in seconds
            event_title: Optional event title
            performance_date: Optional performance date
            retry_reason: Optional reason for retry
            next_retry_time: Optional next retry time
            user_id: Optional user ID who initiated the job
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_retry_message(
                scrape_job_id=scrape_job_id,
                error_msg=error_message,
                retry_attempt=retry_attempt,
                max_retries=max_retries,
                retry_delay=retry_delay_seconds,
                user_id=str(user_id) if user_id else None
            )

            # Add additional fields
            message.data.update({
                "venue": venue,
                "eventTitle": event_title,
                "performanceDate": performance_date.isoformat() if performance_date else None,
                "retryReason": retry_reason,
                "nextRetryTime": next_retry_time.isoformat() if next_retry_time else None,
                "previousError": error_message
            })

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(
                    f"Sent scrape retry notification for job {scrape_job_id}: attempt {retry_attempt}/{max_retries}")
            else:
                logger.error(f"Failed to send scrape retry notification for job {scrape_job_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending scrape retry notification for job {scrape_job_id}: {str(e)}")
            return False

    def send_scrape_success(
            self,
            scrape_job_id: str,
            venue: str,
            result_data: Dict[str, Any],
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            processing_time_ms: Optional[int] = None,
            items_scraped: Optional[int] = None,
            seats_available: Optional[int] = None,
            user_id: Optional[int] = None,
    ) -> bool:
        """
        Send scrape success notification
        
        Args:
            scrape_job_id: Unique identifier for the scrape job
            venue: Venue name
            result_data: Scraped data results
            event_title: Optional event title
            performance_date: Optional performance date
            processing_time_ms: Optional processing time in milliseconds
            items_scraped: Optional number of items scraped
            seats_available: Optional number of seats available
            user_id: Optional user ID who initiated the job
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_success_message(
                scrape_job_id=scrape_job_id,
                result_data=result_data,
                user_id=str(user_id) if user_id else None
            )

            # Add additional fields
            message.data.update({
                "venue": venue,
                "eventTitle": event_title,
                "performanceDate": performance_date.isoformat() if performance_date else None,
                "processingTimeMs": processing_time_ms,
                "itemsScraped": items_scraped,
                "seatsAvailable": seats_available
            })

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(f"Sent scrape success notification for job {scrape_job_id}")
            else:
                logger.error(f"Failed to send scrape success notification for job {scrape_job_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending scrape success notification for job {scrape_job_id}: {str(e)}")
            return False

    def send_scrape_error(
            self,
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
        Send scrape error notification
        
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
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_error_message(
                scrape_job_id=scrape_job_id,
                error_msg=error_message,
                user_id=str(user_id) if user_id else None
            )

            # Add additional fields
            message.data.update({
                "venue": venue,
                "eventTitle": event_title,
                "performanceDate": performance_date.isoformat() if performance_date else None,
                "errorType": error_type,
                "errorCode": error_code,
                "severity": severity,
                "processingTimeMs": processing_time_ms,
                "retryable": retryable,
                "suggestedAction": suggested_action
            })

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(f"Sent scrape error notification for job {scrape_job_id}: {error_type or 'unknown'}")
            else:
                logger.error(f"Failed to send scrape error notification for job {scrape_job_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending scrape error notification for job {scrape_job_id}: {str(e)}")
            return False

    def send_pos_sync_success(
            self,
            operation_id: str,
            performance_id: str,
            venue: str,
            sync_type: str,
            packs_synced: int,
            sync_results: Dict[str, Any],
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            processing_time_ms: Optional[int] = None,
    ) -> bool:
        """
        Send POS sync success notification
        
        Args:
            operation_id: Unique identifier for the sync operation
            performance_id: Performance ID
            venue: Venue name
            sync_type: Type of sync (create, update, delete)
            packs_synced: Number of packs synced
            sync_results: Sync results dictionary
            event_title: Optional event title
            performance_date: Optional performance date
            processing_time_ms: Optional processing time in milliseconds
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_pos_sync_success_message(
                operation_id=operation_id,
                performance_id=performance_id,
                venue=venue,
                sync_type=sync_type,
                packs_synced=packs_synced,
                sync_results=sync_results,
                eventTitle=event_title,
                performanceDate=performance_date.isoformat() if performance_date else None,
                processingTimeMs=processing_time_ms
            )

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(f"Sent POS sync success notification for operation {operation_id}")
            else:
                logger.error(f"Failed to send POS sync success notification for operation {operation_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending POS sync success notification for operation {operation_id}: {str(e)}")
            return False

    def send_pos_sync_error(
            self,
            operation_id: str,
            performance_id: str,
            venue: str,
            sync_type: str,
            error_message: str,
            event_title: Optional[str] = None,
            performance_date: Optional[datetime] = None,
            error_type: Optional[str] = None,
            error_code: Optional[str] = None,
            severity: str = "high",
            packs_attempted: Optional[int] = None,
            packs_successful: Optional[int] = None,
            packs_failed: Optional[int] = None,
            rollback_performed: bool = False,
            rollback_successful: bool = False,
            processing_time_ms: Optional[int] = None,
            retryable: bool = True,
            suggested_action: Optional[str] = None,
    ) -> bool:
        """
        Send POS sync error notification
        
        Args:
            operation_id: Unique identifier for the sync operation
            performance_id: Performance ID
            venue: Venue name
            sync_type: Type of sync (create, update, delete)
            error_message: Error message
            event_title: Optional event title
            performance_date: Optional performance date
            error_type: Optional error type
            error_code: Optional error code
            severity: Error severity (low, medium, high, critical)
            packs_attempted: Optional number of packs attempted
            packs_successful: Optional number of packs successful
            packs_failed: Optional number of packs failed
            rollback_performed: Whether rollback was performed
            rollback_successful: Whether rollback was successful
            processing_time_ms: Optional processing time in milliseconds
            retryable: Whether the error is retryable
            suggested_action: Optional suggested action
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            message = create_pos_sync_error_message(
                operation_id=operation_id,
                performance_id=performance_id,
                venue=venue,
                sync_type=sync_type,
                error_msg=error_message,
                error_type=error_type,
                error_code=error_code,
                severity=severity,
                eventTitle=event_title,
                performanceDate=performance_date.isoformat() if performance_date else None,
                packsAttempted=packs_attempted,
                packsSuccessful=packs_successful,
                packsFailed=packs_failed,
                rollbackPerformed=rollback_performed,
                rollbackSuccessful=rollback_successful,
                processingTimeMs=processing_time_ms,
                retryable=retryable,
                suggestedAction=suggested_action
            )

            success = self.producer.send_message(message.to_dict())

            if success:
                logger.info(f"Sent POS sync error notification for operation {operation_id}: {error_type or 'unknown'}")
            else:
                logger.error(f"Failed to send POS sync error notification for operation {operation_id}")

            return success

        except Exception as e:
            logger.error(f"Error sending POS sync error notification for operation {operation_id}: {str(e)}")
            return False

    def send_pos_sync_completed(
        self,
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
        Send POS sync completion notification to NestJS backend
        
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
            from .message_types import create_pos_sync_completed_message
            
            message = create_pos_sync_completed_message(
                scrapeJobId=scrape_job_id,
                performanceId=performance_id,
                operationId=operation_id,
                success=success,
                venue=venue,
                eventTitle=event_title,
                performanceDate=performance_date,
                syncResults=sync_results or {},
                workflowSummary=workflow_summary or {},
                userId=user_id
            )

            success_sent = self.producer.send_message(message.to_dict())

            if success_sent:
                logger.info(f"Sent POS sync completion notification for operation {operation_id}: {'success' if success else 'failed'}")
            else:
                logger.error(f"Failed to send POS sync completion notification for operation {operation_id}")

            return success_sent

        except Exception as e:
            logger.error(f"Error sending POS sync completion notification for operation {operation_id}: {str(e)}")
            return False

    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.close()


# Global instance for easy import
scrape_status_sender = ScrapeStatusSender()
