from typing import Dict, Any

from .extractor import ColoradoBalletExtractor
from .processor import ColoradoBalletProcessor
from ...base import BaseScraper
from ...exceptions import NetworkException, ParseException, DatabaseStorageException


class ColoradoBalletScraper(BaseScraper):
    def __init__(self, url: str = None, scrape_job_id: str = None,
                 optimization_enabled: bool = True, optimization_level: str = "balanced",
                 config: Dict[str, Any] = None, scraper_definition=None, enriched_data: Dict[str, Any] = None):
        super().__init__(url, scrape_job_id, config, scraper_definition)
        self.extractor = ColoradoBalletExtractor()
        self.processor = ColoradoBalletProcessor(config=self.config)
        
        self.enriched_data = enriched_data or {}

        if self._event_tracker:
            venue_name = self.config.get('default_venue_name', 'Colorado Ballet')
            if self.scraper_definition and hasattr(self.scraper_definition, 'display_name'):
                venue_name = self.scraper_definition.display_name
            elif self.config:
                venue_name = self.config.get('venue_name', venue_name)
            self._event_tracker.venue = venue_name

    @property
    def name(self) -> str:
        if self.scraper_definition and hasattr(self.scraper_definition, 'name'):
            return self.scraper_definition.name
        return self.config.get('scraper_name', "colorado_ballet_scraper_v1")

    async def extract_data(self) -> Dict[str, Any]:
        try:
            combined_data, _ = await self.extractor.extract(self.url)

            if not combined_data:
                error_msg = "Failed to extract required Colorado Ballet data"
                if self._event_tracker:
                    self._event_tracker.track_error("ExtractionError", error_msg, severity='error')
                raise ParseException(error_msg)

            if self._event_tracker:
                seats_count = len(combined_data.get('seats', []))
                event_title = combined_data.get('event_details', {}).get('title', 'Unknown')
                self._event_tracker.track_status_update(
                    f"Extracted data: {seats_count} seats for {event_title}",
                    metadata={
                        'seats_count': seats_count,
                        'event_title': event_title,
                        'venue_name': combined_data.get('event_details', {}).get('venue', 'Unknown')
                    }
                )

            return {
                "combined_data": combined_data
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
            combined_data = raw_data["combined_data"]

            scraped_data = self.processor.process(combined_data, self.url, self.scrape_job_id, self.enriched_data)

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

            prefix = self.config.get('venue_prefix', 'cb')
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
                "scraped_data": scraped_data,
                "scraped_data_serialized": to_serializable_dict(scraped_data),
                "source_website": scraped_data.source_website,
                "scraped_at": scraped_data.scraped_at.isoformat(),
                "url": scraped_data.url,
                "internal_event_id": f"{prefix}_event_{scraped_data.event_info.source_event_id}",
                "performance_key": f"{prefix}_perf_{scraped_data.performance_info.source_performance_id}",
                "venue_timezone": scraped_data.venue_info.venue_timezone or "America/Denver",
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

            from ...core.universal_database_handler import UniversalDatabaseHandler

            scraper_name = self.config.get('source_website', "colorado_ballet")
            prefix = self.config.get('venue_prefix', "cb")

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
        return ["venue_info", "zones", "levels"]