from typing import Dict, Any

from .extractor import DavidHKochTheaterExtractor
from .processor import DavidHKochTheaterProcessor
from ...base import BaseScraper
from ...exceptions import NetworkException, ParseException, DatabaseStorageException


class DavidHKochTheaterScraper(BaseScraper):
    def __init__(self, url: str = None, scrape_job_id: str = None,
                 optimization_enabled: bool = True, optimization_level: str = "balanced",
                 config: Dict[str, Any] = None, scraper_definition=None):
        super().__init__(url, scrape_job_id, config, scraper_definition)
        self.extractor = DavidHKochTheaterExtractor()
        self.processor = DavidHKochTheaterProcessor()

        # Initialize event tracker with venue information
        if self._event_tracker:
            venue_name = 'David H. Koch Theater'
            if self.scraper_definition and hasattr(self.scraper_definition, 'display_name'):
                venue_name = self.scraper_definition.display_name
            elif self.config:
                venue_name = self.config.get('venue_name', 'David H. Koch Theater')
            self._event_tracker.venue = venue_name

    @property
    def name(self) -> str:
        return "david_h_koch_theater_scraper_v5"

    async def extract_data(self) -> Dict[str, Any]:
        try:
            proxy_config = self.get_proxy_config()
            performance_data, seats_data = await self.extractor.extract(self.url, proxy_config)

            if not performance_data:
                error_msg = "Failed to extract performance data from David H Koch Theater"
                if self._event_tracker:
                    self._event_tracker.track_error("ExtractionError", error_msg, severity='error')
                raise ParseException(error_msg)

            # Track successful extraction with details
            if self._event_tracker:
                seats_count = len(seats_data.get('GetSeatsBriefWithMOS', {}).get('body', {}).get('result', {}).get(
                    'GetSeatsBriefExResults', {}).get('S', []))
                perf_data = seats_data.get('GetPerformanceDetailWithDiscountingEx', {}).get('body', {}).get('result',
                                                                                                            {}).get(
                    'GetPerformanceDetailWithDiscountingExResult', {})
                zones_count = len(perf_data.get('AllPrice', []))

                self._event_tracker.track_status_update(
                    f"Extracted data: {zones_count} zones, {seats_count} seats",
                    metadata={
                        'zones_count': zones_count,
                        'seats_count': seats_count,
                        'venue_title': performance_data.get('raw_venue_name', 'David H. Koch Theater')
                    }
                )

            return {
                "performance_data": performance_data,
                "seats_data": seats_data
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
            performance_data = raw_data["performance_data"]
            seats_data = raw_data["seats_data"]

            scraped_data = self.processor.process(performance_data, seats_data, self.url, self.scrape_job_id)

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
            def to_serializable_dict(obj):
                """Convert dataclass objects to serializable dictionaries"""
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
                "scraped_data_serialized": to_serializable_dict(scraped_data),  # Serialized version for JSON
                "source_website": scraped_data.source_website,
                "scraped_at": scraped_data.scraped_at.isoformat(),
                "url": scraped_data.url
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
            scraper_name = "david_h_koch_theater"  # Must match processor.py source_website
            prefix = "dhkt"

            # Only get prefix from scraper definition, NOT the name
            # The name "david_h_koch_theater_scraper" would break sync algorithm
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
                result = await store_func(scraped_data, self.scrape_job_id)
            except ImportError:
                handler = UniversalDatabaseHandler(scraper_name, prefix)
                result = handler.save_scraped_data(scraped_data, self.scrape_job_id)

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
