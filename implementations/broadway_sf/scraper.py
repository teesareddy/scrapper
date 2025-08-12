from typing import Dict, Any

from .extractor import BroadwaySFExtractor
from .processor import BroadwaySFProcessor
from ...base import BaseScraper
from ...exceptions import NetworkException, ParseException, DatabaseStorageException


class BroadwaySFScraper(BaseScraper):
    def __init__(self, url: str = None, scrape_job_id: str = None,
                 optimization_enabled: bool = True, optimization_level: str = "balanced",
                 config: Dict[str, Any] = None, scraper_definition=None, enriched_data: Dict[str, Any] = None):
        super().__init__(url, scrape_job_id, config, scraper_definition)
        self.extractor = BroadwaySFExtractor()
        self.processor = BroadwaySFProcessor()
        
        # Store enriched data for markup processing
        self.enriched_data = enriched_data or {}

        # Initialize event tracker with venue information
        if self._event_tracker:
            venue_name = 'Broadway SF'
            if self.scraper_definition and hasattr(self.scraper_definition, 'display_name'):
                venue_name = self.scraper_definition.display_name
            elif self.config:
                venue_name = self.config.get('venue_name', 'Broadway SF')
            self._event_tracker.venue = venue_name

    @property
    def name(self) -> str:
        return "broadway_sf_scraper_v5"

    async def extract_data(self) -> Dict[str, Any]:
        try:
            calendar_data, seating_data, scraper_instance = await self.extractor.extract(self.url)

            if not seating_data:
                error_msg = "Failed to extract required Broadway SF seating data"
                if self._event_tracker:
                    self._event_tracker.track_error("ExtractionError", error_msg, severity='error')
                raise ParseException(error_msg)

            # Track successful extraction with details
            if self._event_tracker:
                seating_count = len(seating_data.get('seats', []))
                calendar_events = len(calendar_data.get('events', []))
                self._event_tracker.track_status_update(
                    f"Extracted data: {calendar_events} calendar events, {seating_count} seats",
                    metadata={
                        'calendar_events_count': calendar_events,
                        'seating_count': seating_count,
                        'venue_title': seating_data.get('venue_name', 'Unknown')
                    }
                )

            return {
                "calendar_data": calendar_data,
                "seating_data": seating_data,
                "scraper_instance": scraper_instance
            }
        except Exception as e:
            if self._event_tracker:
                self._event_tracker.track_error("ExtractionError", str(e), severity='error')
            if isinstance(e, (NetworkException, ParseException)):
                raise
            error_msg = str(e).strip() if str(e).strip() else "Unknown extraction error"
            raise NetworkException(f"Extraction failed: {error_msg}")

    async def process_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            calendar_data = raw_data["calendar_data"]
            seating_data = raw_data["seating_data"]
            scraper_instance = raw_data.get("scraper_instance")

            scraped_data = self.processor.process(calendar_data, seating_data, self.url, self.scrape_job_id, scraper_instance, self.enriched_data)

            # Track processing success with event details
            if self._event_tracker:
                event_title = scraped_data.event_info.name if scraped_data.event_info else None
                if event_title:
                    self._event_tracker.event_title = event_title

                self._event_tracker.track_status_update(
                    f"Processed venue data: {len(scraped_data.zones)} zones, {len(scraped_data.levels)} levels, {len(scraped_data.seats)} seats",
                    metadata={
                        'zones_count': len(scraped_data.zones),
                        'levels_count': len(scraped_data.levels),
                        'seats_count': len(scraped_data.seats),
                        'seat_packs_count': len(scraped_data.seat_packs),
                        'event_title': event_title,
                        'venue_name': scraped_data.venue_info.name if scraped_data.venue_info else None
                    }
                )

            # Convert to dict for compatibility with base scraper, handling nested dataclasses
            def to_serializable_dict(obj, seen=None):
                """
                Convert dataclass objects to serializable dictionaries with circular reference detection.
                
                CRITICAL: This function handles circular references that occur when serializing SeatPackData objects.
                The database handler expects full objects (Performance, Event, Level) for creating Foreign Key relationships,
                but serialization needs only IDs to avoid infinite recursion.
                
                This function extracts IDs from performance/event/level objects during serialization while preserving
                the full objects for database storage.
                """
                from datetime import datetime
                from decimal import Decimal

                if seen is None:
                    seen = set()
                
                # Handle circular references
                obj_id = id(obj)
                if obj_id in seen:
                    return f"<circular_reference:{obj_id}>"
                
                seen.add(obj_id)

                if hasattr(obj, '__dict__'):
                    result = {}
                    for key, value in obj.__dict__.items():
                        # Special handling for SeatPackData objects to avoid circular references
                        # These fields contain full objects that create circular reference chains:
                        # SeatPackData -> Performance -> Event -> Venue -> Levels -> Sections -> Seats -> SeatPacks
                        if key in ['performance', 'event', 'level'] and hasattr(value, '__dict__'):
                            # Extract only the ID from these objects to prevent recursion
                            # The database handler will still receive the full objects for proper FK creation
                            if hasattr(value, 'source_performance_id'):
                                result[key] = value.source_performance_id
                            elif hasattr(value, 'source_event_id'):
                                result[key] = value.source_event_id
                            elif hasattr(value, 'level_id'):
                                result[key] = value.level_id
                            else:
                                result[key] = str(value)  # Fallback to string representation
                        elif hasattr(value, '__dict__'):
                            result[key] = to_serializable_dict(value, seen)
                        elif isinstance(value, datetime):
                            result[key] = value.isoformat()
                        elif isinstance(value, Decimal):
                            result[key] = float(value)
                        elif isinstance(value, list):
                            result[key] = [to_serializable_dict(item, seen) if hasattr(item, '__dict__') else item for item in
                                           value]
                        else:
                            result[key] = value
                    return result
                return obj
            
            prefix = "bsf"

            return {
                "venue_info": to_serializable_dict(scraped_data.venue_info),
                "event_info": to_serializable_dict(scraped_data.event_info),
                "performance_info": to_serializable_dict(scraped_data.performance_info),
                "levels": [to_serializable_dict(level) for level in scraped_data.levels],
                "zones": [to_serializable_dict(zone) for zone in scraped_data.zones],
                "sections": [to_serializable_dict(section) for section in scraped_data.sections],
                "seats": [to_serializable_dict(seat) for seat in scraped_data.seats],
                "seat_packs": [to_serializable_dict(pack) for pack in scraped_data.seat_packs],
                "scraped_data": scraped_data,  # Keep original object for database storage
                "source_website": scraped_data.source_website,
                "scraped_at": scraped_data.scraped_at.isoformat(),
                "url": scraped_data.url,
                "internal_event_id": f"{prefix}_event_{scraped_data.event_info.source_event_id}",
                "performance_key": f"{prefix}_perf_{scraped_data.performance_info.source_performance_id}",
                "venue_timezone": scraped_data.venue_info.venue_timezone or "America/Los_Angeles",
                "scraper_name": self.name,
                "status": "success"
            }
        except Exception as e:
            if self._event_tracker:
                self._event_tracker.track_error("ProcessingError", str(e), severity='error')
            error_msg = str(e).strip() if str(e).strip() else "Unknown processing error"
            raise ParseException(f"Data processing failed: {error_msg}")

    async def store_in_database(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Extract the ScrapedData object from processed_data for universal database handler
            scraped_data = processed_data.get("scraped_data")
            if not scraped_data:
                raise DatabaseStorageException("No scraped data found for storage")

            # Use universal database handler for storage
            from ...core.universal_database_handler import UniversalDatabaseHandler

            # Get prefix from ScraperDefinition or config
            # IMPORTANT: Keep scraper_name consistent with processor.py to avoid sync issues
            scraper_name = "broadway_sf"  # Must match processor.py source_website
            prefix = "bsf"

            # Only get prefix from scraper definition, NOT the name
            # The name "broadway_sf_scraper_v5" would break sync algorithm
            if self.scraper_definition:
                if hasattr(self.scraper_definition, 'prefix') and self.scraper_definition.prefix:
                    prefix = self.scraper_definition.prefix
            elif self.config:
                # Only allow override from config if explicitly provided, not from scraper definition
                prefix = self.config.get('prefix', prefix)

            try:
                from asgiref.sync import sync_to_async
                handler = UniversalDatabaseHandler(scraper_name, prefix)
                store_func = sync_to_async(handler.save_scraped_data, thread_sensitive=True)
                result = await store_func(scraped_data, self.scrape_job_id, self.enriched_data)
            except ImportError:
                handler = UniversalDatabaseHandler(scraper_name, prefix)
                result = handler.save_scraped_data(scraped_data, self.scrape_job_id, self.enriched_data)

            if not result:
                raise DatabaseStorageException("Database storage returned no key")
            return result
        except Exception as e:
            if isinstance(e, DatabaseStorageException):
                raise
            error_msg = str(e).strip() if str(e).strip() else "Unknown database error"
            raise DatabaseStorageException(f"Database storage failed: {error_msg}")

    def get_required_fields(self) -> list[str]:
        return ["venue_info", "zones", "levels"]
