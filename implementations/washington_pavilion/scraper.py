from typing import Dict, Any

from .extractor import WashingtonPavilionExtractor
from .processor import WashingtonPavilionProcessor
from ...base import BaseScraper
from ...exceptions import NetworkException, ParseException, DatabaseStorageException


class WashingtonPavilionScraper(BaseScraper):
    def __init__(self, url: str = None, scrape_job_id: str = None,
                 optimization_enabled: bool = True, optimization_level: str = "balanced",
                 config: Dict[str, Any] = None, scraper_definition=None, enriched_data: Dict[str, Any] = None):
        super().__init__(url, scrape_job_id, config, scraper_definition)
        self.extractor = WashingtonPavilionExtractor()
        self.processor = WashingtonPavilionProcessor(config=self.config)
        
        # Store enriched data for markup processing
        self.enriched_data = enriched_data or {}

        if self._event_tracker:
            venue_name = self.config.get('default_venue_name', 'Washington Pavilion')
            if self.scraper_definition and hasattr(self.scraper_definition, 'display_name'):
                venue_name = self.scraper_definition.display_name
            elif self.config:
                venue_name = self.config.get('venue_name', venue_name)
            self._event_tracker.venue = venue_name

    @property
    def name(self) -> str:
        if self.scraper_definition and hasattr(self.scraper_definition, 'name'):
            return self.scraper_definition.name
        return self.config.get('scraper_name', "washington_pavilion_scraper_v5")

    async def extract_data(self) -> Dict[str, Any]:
        try:
            pricing_info, seats_info = await self.extractor.extract(self.url)

            if not pricing_info or not seats_info:
                error_msg = "Failed to extract required Washington Pavilion data"
                if self._event_tracker:
                    self._event_tracker.track_error("ExtractionError", error_msg, severity='error')
                raise ParseException(error_msg)

            # Track successful extraction with details
            if self._event_tracker:
                categories_count = len(pricing_info.get('priceRangeCategories', []) if pricing_info else [])
                seats_count = len(seats_info.get('features', []) if seats_info else [])
                self._event_tracker.track_status_update(
                    f"Extracted data: {categories_count} price categories, {seats_count} seats",
                    metadata={
                        'categories_count': categories_count,
                        'seats_count': seats_count,
                        'venue_title': pricing_info.get('title', 'Unknown') if pricing_info else 'Unknown'
                    }
                )

            return {
                "pricing_info": pricing_info,
                "seats_info": seats_info
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
            pricing_info = raw_data["pricing_info"]
            seats_info = raw_data["seats_info"]

            scraped_data = self.processor.process(pricing_info, seats_info, self.url, self.scrape_job_id, self.enriched_data)

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

            # Get prefix from config or scraper definition
            prefix = self.config.get('venue_prefix', 'wp')
            if self.scraper_definition and hasattr(self.scraper_definition, 'prefix'):
                prefix = self.scraper_definition.prefix

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
                "url": scraped_data.url,
                "internal_event_id": f"{prefix}_event_{scraped_data.event_info.source_event_id}",
                "performance_key": f"{prefix}_perf_{scraped_data.performance_info.source_performance_id}",
                "venue_timezone": scraped_data.venue_info.venue_timezone or "America/Chicago",
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
            scraped_data = processed_data.get("scraped_data")
            if not scraped_data:
                raise DatabaseStorageException("No scraped data found for storage")

            # Use universal database handler for storage
            from ...core.universal_database_handler import UniversalDatabaseHandler

            # Get prefix from ScraperDefinition or config
            # IMPORTANT: Keep scraper_name consistent with processor.py to avoid sync issues
            scraper_name = self.config.get('source_website', "washington_pavilion")  # Must match processor.py
            prefix = self.config.get('venue_prefix', "wp")

            # Only get prefix from scraper definition, NOT the name
            # The name "washington_pavilion_scraper_v5" would break sync algorithm
            if self.scraper_definition:
                if hasattr(self.scraper_definition, 'prefix') and self.scraper_definition.prefix:
                    prefix = self.scraper_definition.prefix
            elif self.config:
                # Only allow prefix override from config, maintain source_website consistency
                prefix = self.config.get('prefix', prefix)

            # Use async-safe approach for database operations
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
        return ["venue_info", "zones", "levels"]
