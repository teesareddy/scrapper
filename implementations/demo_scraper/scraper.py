from typing import Dict, Any

from .extractor import DemoScraperExtractor
from .processor import DemoScraperProcessor
from ...base import BaseScraper
from ...exceptions import NetworkException, ParseException, DatabaseStorageException


class DemoScraper(BaseScraper):
    def __init__(self, url: str = None, scrape_job_id: str = None,
                 optimization_enabled: bool = True, optimization_level: str = "balanced",
                 config: Dict[str, Any] = None, scraper_definition=None, enriched_data: Dict[str, Any] = None):
        super().__init__(url, scrape_job_id, config, scraper_definition)
        self.extractor = DemoScraperExtractor()
        self.processor = DemoScraperProcessor(config=self.config)
        
        # Store enriched data for context-aware notifications
        self.enriched_data = enriched_data or {}
        
        # Set scrape context on extractor for progress notifications
        if scrape_job_id:
            self.extractor.set_scrape_context(scrape_job_id, self.venue_name, enriched_data)

    @property
    def name(self) -> str:
        if self.scraper_definition and hasattr(self.scraper_definition, 'name'):
            return self.scraper_definition.name
        return self.config.get('scraper_name', "demo_scraper_v1")
    
    @property
    def venue_name(self) -> str:
        """Get venue name for notifications"""
        if hasattr(self, '_venue_name') and self._venue_name:
            return self._venue_name
        # Use enriched data if available
        if self.enriched_data and self.enriched_data.get('eventName'):
            return f"Demo Venue ({self.enriched_data.get('eventName')})"
        return "Demo Venue"
    
    def _get_event_info_for_notifications(self) -> Dict[str, Any]:
        """Get event information for notifications"""
        # Use enriched data when available, fallback to extracted or default values
        event_title = getattr(self, '_event_title', None)
        if not event_title and self.enriched_data:
            event_title = self.enriched_data.get('eventName', "Demo Event")
        if not event_title:
            event_title = "Demo Event"
            
        user_id = None
        if self.enriched_data:
            user_id = self.enriched_data.get('userId')
            
        return {
            'event_title': event_title,
            'performance_date': None,  # Will be extracted from data if available
            'user_id': user_id
        }

    async def extract_data(self) -> Dict[str, Any]:
        try:
            # Import notification helpers
            try:
                from consumer.notification_helpers import notify_scrape_progress
            except ImportError:
                notify_scrape_progress = lambda *args, **kwargs: False
            
            # Send progress notification for extraction start
            if self.scrape_job_id:
                event_info = self._get_event_info_for_notifications()
                event_title = event_info.get('event_title')
                
                # Create contextual step message
                step_message = f"Initializing browser to extract {event_title} performance data"
                
                notify_scrape_progress(
                    scrape_job_id=self.scrape_job_id,
                    venue=self.venue_name,
                    step=step_message,
                    progress_percentage=20,
                    progress_type="data_extraction",
                    event_title=event_title,
                    performance_date=event_info.get('performance_date'),
                    user_id=event_info.get('user_id')
                )

            # The extractor returns two dictionaries: performance_info and seats_info
            performance_info, seats_info = await self.extractor.extract(self.url)

            # Store dynamic event and venue names from extracted data
            if performance_info and 'performance' in performance_info:
                perf_data = performance_info['performance']
                self._venue_name = perf_data.get('venue', 'Demo Venue')
                self._event_title = perf_data.get('event', 'Demo Event')

            # Send progress notification for extraction completion
            if self.scrape_job_id:
                event_info = self._get_event_info_for_notifications()
                event_title = event_info.get('event_title')
                venue_name = self._venue_name if hasattr(self, '_venue_name') else self.venue_name
                
                # Create contextual completion message
                step_message = f"Successfully extracted performance data for {event_title}"
                if venue_name and venue_name != "Demo Venue":
                    step_message += f" at {venue_name}"
                
                notify_scrape_progress(
                    scrape_job_id=self.scrape_job_id,
                    venue=self.venue_name,
                    step=step_message,
                    progress_percentage=50,
                    progress_type="data_extraction",
                    event_title=event_title,
                    performance_date=event_info.get('performance_date'),
                    user_id=event_info.get('user_id')
                )

            if not performance_info or not seats_info:
                error_msg = "Failed to extract required Demo Scraper data"
                raise ParseException(error_msg)

            return {
                "performance_info": performance_info,
                "seats_info": seats_info
            }
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            error_msg = str(e).strip() if str(e).strip() else "Unknown extraction error"
            raise NetworkException(f"Extraction failed: {error_msg}")

    async def process_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Import notification helpers
            try:
                from consumer.notification_helpers import notify_scrape_progress
            except ImportError:
                notify_scrape_progress = lambda *args, **kwargs: False
            
            # Send progress notification for processing start
            if self.scrape_job_id:
                event_info = self._get_event_info_for_notifications()
                event_title = event_info.get('event_title')
                
                # Create contextual processing start message
                step_message = f"Processing extracted data for {event_title} - Organizing seat map and pricing information"
                
                notify_scrape_progress(
                    scrape_job_id=self.scrape_job_id,
                    venue=self.venue_name,
                    step=step_message,
                    progress_percentage=70,
                    progress_type="processing",
                    event_title=event_title,
                    user_id=event_info.get('user_id')
                )

            scraped_data = self.processor.process(raw_data, self.url, self.scrape_job_id)

            def to_serializable_dict(obj):
                from datetime import datetime
                from decimal import Decimal

                if hasattr(obj, '__dict__'):
                    result = {}
                    for key, value in obj.__dict__.items():
                        if hasattr(value, '__dict__'):
                            result[key] = to_serializable_dict(value)
                        elif isinstance(value, datetime):
                            result[key] = value.isoformat()
                        elif isinstance(value, Decimal):
                            result[key] = float(value)
                        elif isinstance(value, list):
                            result[key] = [to_serializable_dict(item) if hasattr(item, '__dict__') else item for item in
                                           value]
                        else:
                            result[key] = value
                    return result
                return obj

            prefix = self.config.get('venue_prefix', 'ds')
            if self.scraper_definition and hasattr(self.scraper_definition, 'prefix'):
                prefix = self.scraper_definition.prefix

            result = {
                "venue_info": to_serializable_dict(scraped_data.venue_info),
                "event_info": to_serializable_dict(scraped_data.event_info),
                "performance_info": to_serializable_dict(scraped_data.performance_info),
                "levels": [to_serializable_dict(level) for level in scraped_data.levels],
                "zones": [to_serializable_dict(zone) for zone in scraped_data.zones],
                "sections": [to_serializable_dict(section) for section in scraped_data.sections],
                "seats": [to_serializable_dict(seat) for seat in scraped_data.seats],
                "seat_packs": [to_serializable_dict(pack) for pack in scraped_data.seat_packs],
                "scraped_data": scraped_data,
                "scraped_data_serialized": to_serializable_dict(scraped_data),
                "source_website": scraped_data.source_website,
                "scraped_at": scraped_data.scraped_at.isoformat(),
                "url": scraped_data.url,
                "internal_event_id": f"{prefix}_event_{scraped_data.event_info.source_event_id}",
                "performance_key": f"{prefix}_perf_{scraped_data.performance_info.source_performance_id}",
                "venue_timezone": scraped_data.venue_info.venue_timezone or "America/New_York",
                "scraper_name": self.name,
                "status": "success"
            }
            
            # Send progress notification for processing completion
            if self.scrape_job_id:
                event_info = self._get_event_info_for_notifications()
                event_title = scraped_data.event_info.name if scraped_data.event_info else event_info.get('event_title')
                venue_name = scraped_data.venue_info.name if scraped_data.venue_info else self.venue_name
                
                # Get seat count for context
                seat_count = len(scraped_data.seats) if scraped_data.seats else 0
                
                # Create contextual completion message with seat count
                if seat_count > 0 and venue_name:
                    step_message = f"Successfully processed {seat_count:,} seats for {event_title} at {venue_name}"
                elif seat_count > 0:
                    step_message = f"Successfully processed {seat_count:,} seats for {event_title}"
                else:
                    step_message = f"Data processing completed for {event_title}"
                
                notify_scrape_progress(
                    scrape_job_id=self.scrape_job_id,
                    venue=self.venue_name,
                    step=step_message,
                    progress_percentage=85,
                    progress_type="processing",
                    event_title=event_title,
                    user_id=event_info.get('user_id')
                )
            
            return result
        except Exception as e:
            error_msg = str(e).strip() if str(e).strip() else "Unknown processing error"
            raise ParseException(f"Data processing failed: {error_msg}")

    async def store_in_database(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            scraped_data = processed_data.get("scraped_data")
            if not scraped_data:
                raise DatabaseStorageException("No scraped data found for storage")

            # Use universal database handler for storage
            from ...core.universal_database_handler import UniversalDatabaseHandler

            scraper_name = self.config.get('source_website', "demo_scraper")
            prefix = self.config.get('venue_prefix', "ds")

            if self.scraper_definition:
                if hasattr(self.scraper_definition, 'prefix') and self.scraper_definition.prefix:
                    prefix = self.scraper_definition.prefix
            elif self.config:
                prefix = self.config.get('prefix', prefix)

            from asgiref.sync import sync_to_async
            handler = UniversalDatabaseHandler(scraper_name, prefix)
            store_func = sync_to_async(handler.save_scraped_data, thread_sensitive=True)
            result = await store_func(scraped_data, self.scrape_job_id)

            if not result:
                raise DatabaseStorageException("Database storage returned no key")
            
            return result
        except Exception as e:
            if isinstance(e, DatabaseStorageException):
                raise
            error_msg = str(e).strip() if str(e).strip() else "Unknown database error"
            raise DatabaseStorageException(f"Database storage failed: {error_msg}")

    def get_required_fields(self) -> list[str]:
        return ["performance_info", "seats_info"]
