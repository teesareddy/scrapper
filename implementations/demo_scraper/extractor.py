import asyncio
import os
from typing import Dict, Any, Tuple, Optional
import aiohttp
from ...exceptions.scraping_exceptions import NetworkException, ParseException

class DemoScraperExtractor:

    def __init__(self):
        try:
            from consumer.notification_helpers import notify_scrape_progress
            self.notify_scrape_progress = notify_scrape_progress
        except ImportError:
            self.notify_scrape_progress = lambda *args, **kwargs: False
        
        self.scrape_job_id = None
        self.venue_name = "Demo Venue"
        self.enriched_data = {}
        # Make base URL configurable via environment variable
        self.base_url = os.getenv('DEMO_SCRAPER_BASE_URL', 'http://backend-demo-scrapper:5174')

    def set_scrape_context(self, scrape_job_id: str, venue_name: str = "Demo Venue", enriched_data: dict = None):
        """Set scrape context for progress notifications"""
        self.scrape_job_id = scrape_job_id
        self.venue_name = venue_name
        self.enriched_data = enriched_data or {}

    def _send_progress(self, current_step: str, percentage: int, progress_type: str = "data_extraction"):
        """Send progress notification"""
        if self.scrape_job_id:
            event_title = self.enriched_data.get('eventName', "Demo Event")
            user_id = self.enriched_data.get('userId')
            
            self.notify_scrape_progress(
                scrape_job_id=self.scrape_job_id,
                venue=self.venue_name,
                step=current_step,
                progress_percentage=percentage,
                progress_type=progress_type,
                event_title=event_title,
                user_id=user_id
            )

    async def _fetch_api_data(self, session, url: str) -> Optional[Dict[str, Any]]:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            raise NetworkException(f"API request to {url} failed: {e}")
        except Exception as e:
            raise ParseException(f"Failed to parse JSON from {url}: {e}")

    async def extract(self, url: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        try:
            performance_id = url.split('/')[-1]
            if not performance_id.isdigit():
                raise ParseException(f"Invalid performance ID in URL: {url}")

            event_name = self.enriched_data.get('eventName', 'Demo Event')
            self._send_progress(f"Fetching data for {event_name} from API", 25)

            async with aiohttp.ClientSession() as session:
                performance_task = self._fetch_api_data(session, f"{self.base_url}/api/performance/{performance_id}")
                meta_task = self._fetch_api_data(session, f"{self.base_url}/api/meta/{performance_id}")
                seats_task = self._fetch_api_data(session, f"{self.base_url}/api/seats/{performance_id}")

                results = await asyncio.gather(performance_task, meta_task, seats_task, return_exceptions=True)

            performance_data, meta_data, seats_data = results

            if isinstance(performance_data, Exception):
                raise performance_data
            if isinstance(meta_data, Exception):
                raise meta_data
            if isinstance(seats_data, Exception):
                raise seats_data

            self._send_progress(f"Parsing extracted data for {event_name}", 35)

            parsed_performance = self._parse_performance_data(performance_data)
            parsed_meta = self._parse_meta_data(meta_data)
            parsed_seats = self._parse_seats_data(seats_data)

            if not parsed_performance:
                raise ParseException("Failed to parse performance data from API")
            if not parsed_meta:
                raise ParseException("Failed to parse meta data from API")
            if not parsed_seats:
                raise ParseException("Failed to parse seats data from API")

            seat_count = len(parsed_seats)
            self._send_progress(f"Data extraction completed successfully - Found {seat_count:,} seats for {event_name}", 45)

            return {"performance": parsed_performance, "meta": parsed_meta}, {"seats": parsed_seats}

        except (NetworkException, ParseException) as e:
            raise e
        except Exception as e:
            raise NetworkException(f"Demo Scraper extraction failed: {e}")

    def _parse_performance_data(self, extracted_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not extracted_info:
            return None
        return {
            "event": extracted_info.get("event", "Demo Event"),
            "venue": extracted_info.get("venue", "Demo Venue"),
            "location": extracted_info.get("location", "Demo City, DS"),
            "address": extracted_info.get("address", "123 Demo St"),
            "datetime": extracted_info.get("datetime", "2024-01-01T19:00:00"),
            "description": f"Event at {extracted_info.get('venue', 'Demo Venue')}",
            "image": None
        }

    def _parse_meta_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not raw_data or 'zones' not in raw_data or 'levels' not in raw_data or 'sections' not in raw_data:
            return None
        return raw_data

    def _parse_seats_data(self, raw_data: list) -> Optional[list]:
        if not raw_data or not isinstance(raw_data, list):
            return None
        return raw_data