import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from django.utils import timezone

from .core.result_structures import (
    ScrapingResult, ResultBuilder, ErrorCategory,
    ValidationResult
)
from .exceptions.scraping_exceptions import (
    ScrapingException, NetworkException, ParseException,
    TimeoutException
)
from .services.event_tracker import ScrapingEventTracker, track_scraping_exception

# Import notification helpers
try:
    from consumer.notification_helpers import (
        notify_scrape_acknowledged,
        notify_scrape_started,
        notify_scrape_progress,
        notify_scrape_success,
        notify_scrape_error,
        ProgressTracker
    )
except ImportError:
    logging.getLogger(__name__).warning("Notification helpers not available - notifications will be disabled")
    notify_scrape_acknowledged = lambda *args, **kwargs: False
    notify_scrape_started = lambda *args, **kwargs: False
    notify_scrape_progress = lambda *args, **kwargs: False
    notify_scrape_success = lambda *args, **kwargs: False
    notify_scrape_error = lambda *args, **kwargs: False
    ProgressTracker = None

# Performance optimizer module not available
PerformanceOptimizer = None
OptimizationLevel = None
PerformanceOptimizerFactory = None


class BaseScraper(ABC):
    """Enhanced base class for all scrapers with comprehensive error handling and result management"""

    def __init__(self, url: Optional[str] = None, scrape_job_id: Optional[str] = None,
                 config: Optional[Dict[str, Any]] = None, scraper_definition=None):
        self.url = url
        self.scrape_job_id = scrape_job_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self._result_builder: Optional[ResultBuilder] = None
        self._event_tracker: Optional[ScrapingEventTracker] = None
        self._database_result: Optional[Dict[str, str]] = None
        self.config = config or {}
        self.scraper_definition = scraper_definition

        if scrape_job_id:
            self._event_tracker = ScrapingEventTracker(
                scrape_job_id=scrape_job_id,
                scraper_name=self.name if hasattr(self, 'name') else self.__class__.__name__,
                url=url
            )

        self._apply_scraper_definition_config()

    @property
    def venue_name(self) -> str:
        """Get venue name for notifications. Override in subclasses."""
        if hasattr(self, 'name'):
            return self.name
        return self.__class__.__name__.replace('Scraper', '')
    
    def _get_event_info_for_notifications(self) -> Dict[str, Any]:
        """Get event information for notifications. Override in subclasses."""
        return {
            'event_title': None,
            'performance_date': None,
            'user_id': None
        }

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the unique name of the scraper"""
        pass

    @abstractmethod
    async def extract_data(self) -> Dict[str, Any]:
        """Extract raw data from the website - scraper specific
        
        Returns:
            Dict containing raw-extracted data
            
        Raises:
            NetworkException: For network-related issues
            ParseException: For data parsing issues
            TimeoutException: For timeout issues
        """
        pass

    @abstractmethod
    async def process_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw data into unified structure - scraper specific
        
        Args:
            raw_data: Raw data from extract_data method
            
        Returns:
            Dict containing processed data in standardized format
            
        Raises:
            ParseException: For data processing issues
            ValidationException: For data validation issues
        """
        pass

    @abstractmethod
    async def store_in_database(self, processed_data: Dict[str, Any]) -> str:
        """Store processed data in database - scraper-specific mapping
        
        Args:
            processed_data: Processed data from process_data method
            
        Returns:
            String key/ID for the stored data
            
        Raises:
            DatabaseStorageException: For database storage issues
        """
        pass

    def validate_data(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate processed data - can be overridden by specific scrapers
        
        Args:
            data: Processed data to validate
            
        Returns:
            ValidationResult containing validation status and any errors/warnings
        """
        validation = ValidationResult(is_valid=True)

        if not data:
            validation.add_error("No data to validate")
            return validation

        required_fields = self.get_required_fields()
        for field in required_fields:
            if field not in data or data[field] is None:
                validation.add_error(f"Required field '{field}' is missing or None")

        return validation

    def get_required_fields(self) -> List[str]:
        """Return list of required fields - can be overridden by specific scrapers"""
        return []

    def _apply_scraper_definition_config(self):
        """Apply configuration from ScraperDefinition"""
        if not self.scraper_definition:
            return

        # Optimization settings (performance optimizer not available)
        if hasattr(self.scraper_definition, 'optimization_enabled'):
            self.optimization_enabled = self.scraper_definition.optimization_enabled
            
        if hasattr(self.scraper_definition, 'optimization_level'):
            self.optimization_level = self.scraper_definition.optimization_level

        # Apply timeout settings
        if hasattr(self.scraper_definition, 'timeout_seconds'):
            self.timeout_seconds = self.scraper_definition.timeout_seconds

        # Apply retry settings
        if hasattr(self.scraper_definition, 'retry_attempts'):
            self.retry_attempts = self.scraper_definition.retry_attempts

        if hasattr(self.scraper_definition, 'retry_delay_seconds'):
            self.retry_delay_seconds = self.scraper_definition.retry_delay_seconds

        # Apply browser settings
        if hasattr(self.scraper_definition, 'headless_mode'):
            self.headless_mode = self.scraper_definition.headless_mode

        if hasattr(self.scraper_definition, 'user_agent') and self.scraper_definition.user_agent:
            self.user_agent = self.scraper_definition.user_agent

        if hasattr(self.scraper_definition, 'viewport_width'):
            self.viewport_width = self.scraper_definition.viewport_width

        if hasattr(self.scraper_definition, 'viewport_height'):
            self.viewport_height = self.scraper_definition.viewport_height

        # Apply rate limiting
        if hasattr(self.scraper_definition, 'delay_between_requests_ms'):
            self.delay_between_requests_ms = self.scraper_definition.delay_between_requests_ms

        if hasattr(self.scraper_definition, 'max_concurrent_jobs'):
            self.max_concurrent_jobs = self.scraper_definition.max_concurrent_jobs

        # Apply debug settings
        if hasattr(self.scraper_definition, 'enable_screenshots'):
            self.enable_screenshots = self.scraper_definition.enable_screenshots

        if hasattr(self.scraper_definition, 'enable_detailed_logging'):
            self.enable_detailed_logging = self.scraper_definition.enable_detailed_logging

        if hasattr(self.scraper_definition, 'log_level'):
            self.log_level = self.scraper_definition.log_level

        # Apply custom settings
        if hasattr(self.scraper_definition, 'custom_settings') and self.scraper_definition.custom_settings:
            for key, value in self.scraper_definition.custom_settings.items():
                setattr(self, f"custom_{key}", value)

    async def scrape(self) -> ScrapingResult:
        """Main scraping method - enhanced with comprehensive error handling"""
        self._result_builder = ResultBuilder(self.name, self.url or "", self.scrape_job_id)

        # Send acknowledged notification
        if self.scrape_job_id:
            event_info = self._get_event_info_for_notifications()
            notify_scrape_acknowledged(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue_name,
                event_title=event_info.get('event_title'),
                performance_date=event_info.get('performance_date'),
                user_id=event_info.get('user_id')
            )

        # Track scraping start
        if self._event_tracker:
            self._event_tracker.track_scrape_started()

        if not self.url:
            if self._event_tracker:
                self._event_tracker.track_scrape_failed("No URL provided", "ValidationError")
            
            # Send error notification
            if self.scrape_job_id:
                event_info = self._get_event_info_for_notifications()
                notify_scrape_error(
                    scrape_job_id=self.scrape_job_id,
                    venue=self.venue_name,
                    error_message="No URL provided",
                    event_title=event_info.get('event_title'),
                    performance_date=event_info.get('performance_date'),
                    error_type="validation_error",
                    user_id=event_info.get('user_id')
                )
            
            return (self._result_builder
                    .with_error(ErrorCategory.VALIDATION, "No URL provided", fatal=True)
                    .build())

        try:
            async with self._scraping_context():

                # Send started notification
                if self.scrape_job_id:
                    event_info = self._get_event_info_for_notifications()
                    notify_scrape_started(
                        scrape_job_id=self.scrape_job_id,
                        venue=self.venue_name,
                        event_title=event_info.get('event_title'),
                        performance_date=event_info.get('performance_date'),
                        user_id=event_info.get('user_id')
                    )

                # Track extraction start
                if self._event_tracker:
                    self._event_tracker.track_extraction_started()

                # Send progress notification for extraction
                if self.scrape_job_id:
                    event_info = self._get_event_info_for_notifications()
                    notify_scrape_progress(
                        scrape_job_id=self.scrape_job_id,
                        venue=self.venue_name,
                        step="Starting data extraction",
                        progress_percentage=10,
                        progress_type="data_extraction",
                        event_title=event_info.get('event_title'),
                        performance_date=event_info.get('performance_date'),
                        user_id=event_info.get('user_id')
                    )

                raw_data = await self.extract_data()
                self._result_builder.record_page_scraped()

                # Track extraction completion
                if self._event_tracker:
                    items_count = len(raw_data) if isinstance(raw_data, (list, dict)) else None
                    self._event_tracker.track_extraction_completed(items_count)

                # Send progress notification for processing
                if self.scrape_job_id:
                    event_info = self._get_event_info_for_notifications()
                    notify_scrape_progress(
                        scrape_job_id=self.scrape_job_id,
                        venue=self.venue_name,
                        step="Processing extracted data",
                        progress_percentage=60,
                        progress_type="processing",
                        event_title=event_info.get('event_title'),
                        performance_date=event_info.get('performance_date'),
                        user_id=event_info.get('user_id')
                    )

                # Track processing start
                if self._event_tracker:
                    self._event_tracker.track_processing_started()

                processed_data = await self.process_data(raw_data)

                validation = self.validate_data(processed_data)
                self._result_builder.with_validation(validation)

                if not validation.is_valid:
                    self.logger.warning(f"Data validation failed: {validation.errors}")
                    if self._event_tracker:
                        self._event_tracker.track_error(
                            "ValidationError",
                            f"Data validation failed: {validation.errors}",
                            severity='warning'
                        )

                if processed_data:
                    self._result_builder.record_items_extracted(len(processed_data))

                    # Send progress notification for storage
                    if self.scrape_job_id:
                        event_info = self._get_event_info_for_notifications()
                        notify_scrape_progress(
                            scrape_job_id=self.scrape_job_id,
                            venue=self.venue_name,
                            step="Storing data in database",
                            progress_percentage=90,
                            progress_type="storage",
                            event_title=event_info.get('event_title'),
                            performance_date=event_info.get('performance_date'),
                            user_id=event_info.get('user_id')
                        )

                    # Track storage start
                    if self._event_tracker:
                        self._event_tracker.track_storage_started()

                    database_result = await self.store_in_database(processed_data)
                    if isinstance(database_result, dict):
                        # New format: dictionary with performance_id, event_id, venue_id
                        self._result_builder.with_database_key(database_result.get('performance_id'))
                        # Store additional IDs for use in success response
                        self._database_result = database_result
                    else:
                        # Legacy format: string performance_id
                        self._result_builder.with_database_key(database_result)
                        self._database_result = {'performance_id': database_result}

                result = self._result_builder.with_data(processed_data).build()

                await self._handle_success_response(result)
                return result

        except ScrapingException as e:
            self.logger.error(f"Scraping exception: {e.message if e.message else 'No error message'}")
            if self._event_tracker:
                track_scraping_exception(self._event_tracker, e, "ScrapingException")
                self._event_tracker.track_scrape_failed(str(e), e.__class__.__name__)
            return await self._handle_scraping_error(e)
        except Exception as e:
            error_msg = str(e).strip() if str(e).strip() else "Unknown error occurred"
            self.logger.exception(f"Unexpected error: {error_msg}")
            if self._event_tracker:
                track_scraping_exception(self._event_tracker, e, "UnexpectedError")
                self._event_tracker.track_scrape_failed(error_msg, e.__class__.__name__)
            return await self._handle_unexpected_error(e)

    @asynccontextmanager
    async def _scraping_context(self):
        """Context manager for scraping operations with cleanup"""
        try:
            yield
        finally:
            # Always ensure cleanup happens
            self.cleanup()

    def cleanup(self):
        """
        Comprehensive cleanup method for all scrapers.
        
        This method handles:
        - HTTP client cleanup
        - Browser instance cleanup
        - Cache clearing
        - Memory optimization
        - Resource tracking
        - Garbage collection
        
        Best practices implemented:
        - Safe cleanup with error handling
        - Resource tracking and logging
        - Memory optimization
        - Automatic cleanup in destructor
        """
        try:
            # Track cleanup start
            self.logger.info(f"Starting cleanup for {self.name}")
            
            # 1. Clean up HTTP clients and connections
            self._cleanup_http_resources()
            
            # 2. Clean up browser instances
            self._cleanup_browser_resources()
            
            # 3. Clean up extractor and processor resources
            self._cleanup_component_resources()
            
            # 4. Clear caches and temporary data
            self._cleanup_caches()
            
            # 5. Memory optimization
            self._optimize_memory()
            
            # 6. Force garbage collection
            self._force_garbage_collection()
            
            self.logger.info(f"Cleanup completed for {self.name}")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup for {self.name}: {e}")
        finally:
            # Always ensure basic cleanup happens
            self._force_garbage_collection()

    def _cleanup_http_resources(self):
        """Clean up HTTP clients and connections."""
        try:
            # Clean up HTTP clients
            if hasattr(self, '_http_client') and self._http_client:
                if hasattr(self._http_client, 'close'):
                    self._http_client.close()
                self._http_client = None
            
            # Clean up session objects
            if hasattr(self, '_session') and self._session:
                if hasattr(self._session, 'close'):
                    self._session.close()
                self._session = None
            
            # Clean up aiohttp sessions
            if hasattr(self, '_aiohttp_session') and self._aiohttp_session:
                if hasattr(self._aiohttp_session, 'close'):
                    self._aiohttp_session.close()
                self._aiohttp_session = None
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up HTTP resources: {e}")

    def _cleanup_browser_resources(self):
        """Clean up browser instances and pages."""
        try:
            # Clean up browser instances
            if hasattr(self, '_browser') and self._browser:
                if hasattr(self._browser, 'close'):
                    self._browser.close()
                self._browser = None
            
            # Clean up page objects
            if hasattr(self, '_page') and self._page:
                if hasattr(self._page, 'close'):
                    self._page.close()
                self._page = None
                
            # Clean up context objects
            if hasattr(self, '_context') and self._context:
                if hasattr(self._context, 'close'):
                    self._context.close()
                self._context = None
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up browser resources: {e}")

    def _cleanup_component_resources(self):
        """Clean up extractor and processor resources."""
        try:
            # Clean up extractor
            if hasattr(self, 'extractor') and self.extractor:
                if hasattr(self.extractor, 'cleanup'):
                    self.extractor.cleanup()
                elif hasattr(self.extractor, '_cleanup'):
                    self.extractor._cleanup()
            
            # Clean up processor
            if hasattr(self, 'processor') and self.processor:
                if hasattr(self.processor, 'cleanup'):
                    self.processor.cleanup()
                elif hasattr(self.processor, '_cleanup'):
                    self.processor._cleanup()
                    
        except Exception as e:
            self.logger.warning(f"Error cleaning up component resources: {e}")

    def _cleanup_caches(self):
        """Clear caches and temporary data."""
        try:
            # Clear response caches
            if hasattr(self, '_response_cache'):
                self._response_cache.clear()
            
            # Clear data caches
            if hasattr(self, '_data_cache'):
                self._data_cache.clear()
            
            # Clear temporary data
            if hasattr(self, '_temp_data'):
                self._temp_data.clear()
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up caches: {e}")

    def _optimize_memory(self):
        """Optimize memory usage."""
        try:
            # Clear large objects
            large_attrs = ['_raw_data', '_processed_data', '_extracted_data']
            for attr in large_attrs:
                if hasattr(self, attr):
                    delattr(self, attr)
                    
            # Clear result builder if not needed
            if hasattr(self, '_result_builder') and self._result_builder:
                self._result_builder = None
                
        except Exception as e:
            self.logger.warning(f"Error optimizing memory: {e}")

    def _force_garbage_collection(self):
        """Force garbage collection to free memory."""
        try:
            import gc
            # Collect garbage
            collected = gc.collect()
            if collected > 0:
                self.logger.debug(f"Garbage collection freed {collected} objects")
                
        except Exception as e:
            self.logger.warning(f"Error during garbage collection: {e}")

    def __del__(self):
        """Destructor to ensure cleanup happens."""
        try:
            self.cleanup()
        except:
            pass  # Ignore errors during cleanup in destructor

    async def _handle_success_response(self, result: ScrapingResult) -> None:
        """Handle successful response using new notification system"""
        if not self.scrape_job_id:
            return

        try:
            from asgiref.sync import sync_to_async
            from scrapers.services.performance_data_serializer import PerformanceDataSerializer

            # Get performance ID from result
            performance_id = result.database_key
            if not performance_id:
                self.logger.warning("Skipping scrape completion notification - no performance_id returned")
                return

            # Use the new serializer to get clean, structured data
            self.logger.info(f"Creating clean serialized data for performance {performance_id}")
            
            # Create async wrapper for the serializer
            serialize_func = sync_to_async(
                PerformanceDataSerializer.serialize_for_nestjs, 
                thread_sensitive=True
            )
            
            # Get the properly structured data
            serialized_data = await serialize_func(performance_id, self.scrape_job_id)
            
            # Check if serialization was successful
            if not serialized_data.get("success"):
                error_msg = serialized_data.get("error", {}).get("message", "Unknown serialization error")
                self.logger.error(f"Performance data serialization failed: {error_msg}")
                
                # Send error notification using new system
                event_info = self._get_event_info_for_notifications()
                notify_scrape_error(
                    scrape_job_id=self.scrape_job_id,
                    venue=self.venue_name,
                    error_message=error_msg,
                    event_title=event_info.get('event_title'),
                    performance_date=event_info.get('performance_date'),
                    error_type="serialization_error",
                    user_id=event_info.get('user_id')
                )
                return
            
            # Send success notification using new system with database-fetched data
            event_info = self._get_event_info_for_notifications()
            scraped_data = {
                'internal_performance_id': performance_id,
                'internal_event_id': serialized_data["data"]["event"]["id"],
                'internal_venue_id': serialized_data["data"]["venue"]["id"],
                'url': serialized_data["data"]["performance"]["seat_map_url"],
                'scraper_name': self.name,
                'status': 'success',
                'scraped_at': serialized_data["data"]["scrape_completion"]["scraped_at"],
                'venue_config': serialized_data["data"]["venue"].get("seating_config"),
                'event_info': serialized_data["data"]["event"],
                'venue_info': serialized_data["data"]["venue"],
                'performance_info': serialized_data["data"]["performance"]
            }
            
            notify_scrape_success(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue_name,
                scraped_data=scraped_data,
                event_title=serialized_data["data"]["event"]["name"],
                performance_date=timezone.datetime.fromisoformat(serialized_data["data"]["performance"]["datetime_utc"].replace('Z', '+00:00')),
                processing_time_ms=getattr(result, 'processing_time_ms', None),
                user_id=event_info.get('user_id')
            )
            
            self.logger.info(f"Successfully sent clean scrape completion notification for performance {performance_id}")

        except Exception as e:
            self.logger.error(f"Failed to send completion notification: {e}", exc_info=True)
            
            # Send a basic error notification as fallback
            event_info = self._get_event_info_for_notifications()
            notify_scrape_error(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue_name,
                error_message=f"Failed to process scrape completion: {str(e)}",
                event_title=event_info.get('event_title'),
                performance_date=event_info.get('performance_date'),
                error_type="notification_error",
                user_id=event_info.get('user_id')
            )

    async def _handle_scraping_error(self, error: ScrapingException) -> ScrapingResult:
        """Handle known scraping errors"""
        # Ensure result builder is initialized
        if not self._result_builder:
            self._result_builder = ResultBuilder(self.name, self.url or "", self.scrape_job_id)
            
        if isinstance(error, NetworkException):
            category = ErrorCategory.NETWORK
            error_type = "network_error"
        elif isinstance(error, ParseException):
            category = ErrorCategory.PARSING
            error_type = "parsing_error"
        elif isinstance(error, TimeoutException):
            category = ErrorCategory.TIMEOUT
            error_type = "timeout_error"
        else:
            category = ErrorCategory.UNKNOWN
            error_type = "unknown_error"

        result = (self._result_builder
                  .with_error(category, error.message, error.details,
                              error.retry_after, error.fatal)
                  .build())

        # Send error notification using new system
        if self.scrape_job_id:
            event_info = self._get_event_info_for_notifications()
            notify_scrape_error(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue_name,
                error_message=error.message,
                event_title=event_info.get('event_title'),
                performance_date=event_info.get('performance_date'),
                error_type=error_type,
                retryable=not error.fatal,
                user_id=event_info.get('user_id')
            )

        await self._send_error_notification(result)
        return result

    async def _handle_unexpected_error(self, error: Exception) -> ScrapingResult:
        """Handle unexpected errors"""
        # Ensure result builder is initialized
        if not self._result_builder:
            self._result_builder = ResultBuilder(self.name, self.url or "", self.scrape_job_id)
            
        error_msg = str(error).strip() if str(error).strip() else "Unknown error occurred"
        result = (self._result_builder
                  .with_error(ErrorCategory.UNKNOWN, error_msg,
                              details=f"Unexpected error in {self.name}", fatal=True)
                  .build())

        # Send error notification using new system
        if self.scrape_job_id:
            event_info = self._get_event_info_for_notifications()
            notify_scrape_error(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue_name,
                error_message=error_msg,
                event_title=event_info.get('event_title'),
                performance_date=event_info.get('performance_date'),
                error_type="unexpected_error",
                retryable=False,
                user_id=event_info.get('user_id')
            )

        await self._send_error_notification(result)
        return result

    async def _send_error_notification(self, result: ScrapingResult) -> None:
        """Send error notification to queue"""
        if not self.scrape_job_id:
            return

        try:
            from asgiref.sync import sync_to_async
            from consumer.rabbitmq_producer import producer
            from scrapers.storage.redis_handler import RedisStorageHandler

            redis_handler = RedisStorageHandler()

            store_error_func = sync_to_async(redis_handler.store_error, thread_sensitive=True)
            await store_error_func(self.scrape_job_id,
                                   '; '.join([e.message for e in result.errors]),
                                   result.url)

            send_func = sync_to_async(producer.send_scrape_completed, thread_sensitive=True)
            await send_func(self.scrape_job_id, result.to_dict())

        except Exception as e:
            self.logger.error(f"Failed to send error notification: {e}")

    def get_proxy_config(self) -> Optional[Dict[str, Any]]:
        """Get proxy configuration from ScraperDefinition"""
        if self.scraper_definition and hasattr(self.scraper_definition,
                                               'proxy_settings') and self.scraper_definition.proxy_settings:
            proxy = self.scraper_definition.proxy_settings
            if proxy.is_active and proxy.status == 'active':
                return {
                    "host": proxy.host,
                    "port": proxy.port,
                    "username": proxy.username,
                    "password": proxy.password,
                    "protocol": "http"
                }

        # Fallback to general proxy lookup
        try:
            from .models import ProxyConfiguration
            active_proxy = ProxyConfiguration.objects.filter(
                is_active=True,
                status='active'
            ).first()

            if active_proxy:
                return {
                    "host": active_proxy.host,
                    "port": active_proxy.port,
                    "username": active_proxy.username,
                    "password": active_proxy.password,
                    "protocol": "http"
                }
        except Exception:
            pass

        return None
