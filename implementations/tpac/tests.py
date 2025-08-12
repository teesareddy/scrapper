"""
Unit tests for TPAC scraper implementation
"""
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import aiohttp
from decimal import Decimal
from datetime import datetime

from .extractor import TPACExtractor
from .processor import TPACProcessor
from .scraper import TPACScraper
from ...exceptions import NetworkException, ParseException


class TestTPACExtractor(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        self.extractor = TPACExtractor()
        self.test_url = "https://cart.tpac.org/14390/14430"

    async def test_parse_tpac_url(self):
        """Test URL parsing to extract base URL and performance ID"""
        base_url, performance_id = self.extractor._parse_tpac_url(self.test_url)
        self.assertEqual(base_url, "https://cart.tpac.org/14390")
        self.assertEqual(performance_id, "14430")

    async def test_parse_tpac_url_invalid(self):
        """Test URL parsing with invalid URL"""
        with self.assertRaises(ParseException):
            self.extractor._parse_tpac_url("https://cart.tpac.org/invalid")

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_performance_details_success(self, mock_get):
        """Test successful performance details fetch"""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = {
            "facility_no": "123",
            "description": "Test Event",
            "perf_dt": "2023-12-01T19:00:00",
            "facility_desc": "Test Venue"
        }
        mock_get.return_value.__aenter__.return_value = mock_response

        async with self.extractor:
            result = await self.extractor._fetch_performance_details("https://cart.tpac.org/14390", "14430")
        
        self.assertEqual(result["facility_no"], "123")
        self.assertEqual(result["description"], "Test Event")

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_performance_details_http_error(self, mock_get):
        """Test performance details fetch with HTTP error"""
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_get.return_value.__aenter__.return_value = mock_response

        async with self.extractor:
            with self.assertRaises(NetworkException):
                await self.extractor._fetch_performance_details("https://cart.tpac.org/14390", "14430")

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_screens_success(self, mock_get):
        """Test successful screens fetch"""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = [
            {"screen_no": "1", "screen_desc": "Orchestra"},
            {"screen_no": "2", "screen_desc": "Balcony"}
        ]
        mock_get.return_value.__aenter__.return_value = mock_response

        async with self.extractor:
            result = await self.extractor._fetch_screens("https://cart.tpac.org/14390", "14430")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["screen_desc"], "Orchestra")


class TestTPACProcessor(unittest.TestCase):
    
    def setUp(self):
        self.processor = TPACProcessor({"source_website": "tpac"})
        self.sample_performance_data = {
            "performance_details": {
                "facility_no": "123",
                "description": "Test Event",
                "perf_dt": "2023-12-01T19:00:00",
                "facility_desc": "Test Venue"
            },
            "performance_id": "14430"
        }
        self.sample_seat_data = {
            "screens": [
                {"screen_no": "1", "screen_desc": "Orchestra"}
            ],
            "seat_lists": {
                "1": {
                    "AvailablePrices": [
                        {"ZoneNo": "1", "Price": 100.0, "ZoneDesc": "Premium"}
                    ],
                    "seats": [
                        {
                            "seat_status_desc": "Available",
                            "zone_no": "1",
                            "seat_row": "A",
                            "seat_num": "1",
                            "accessible_ind": False
                        }
                    ]
                }
            },
            "facility_id": "123"
        }

    def test_process_venue_info(self):
        """Test venue info processing"""
        venue_info = self.processor._process_venue_info(self.sample_performance_data["performance_details"])
        
        self.assertEqual(venue_info.name, "Test Venue")
        self.assertEqual(venue_info.source_venue_id, "tpac_123")
        self.assertEqual(venue_info.source_website, "tpac")
        self.assertEqual(venue_info.city, "Nashville")
        self.assertEqual(venue_info.state, "TN")
        self.assertEqual(venue_info.venue_timezone, "America/Chicago")

    def test_process_event_info(self):
        """Test event info processing"""
        event_info = self.processor._process_event_info(
            self.sample_performance_data["performance_details"], 
            "tpac_123", 
            "https://cart.tpac.org/14390/14430"
        )
        
        self.assertEqual(event_info.name, "Test Event")
        self.assertEqual(event_info.source_website, "tpac")
        self.assertEqual(event_info.currency, "USD")

    def test_process_zones(self):
        """Test zone processing"""
        zones = self.processor._process_zones(self.sample_seat_data["seat_lists"])
        
        self.assertEqual(len(zones), 1)
        zone = zones[0]
        self.assertEqual(zone.zone_id, "1")
        self.assertEqual(zone.name, "Premium")
        self.assertEqual(zone.min_price, Decimal('100.0'))
        self.assertEqual(zone.source_website, "tpac")

    def test_process_levels_and_sections(self):
        """Test levels and sections processing"""
        levels, sections = self.processor._process_levels_and_sections(
            self.sample_seat_data["screens"], 
            self.sample_seat_data["facility_id"]
        )
        
        self.assertEqual(len(levels), 1)
        self.assertEqual(levels[0].name, "Main Level")
        self.assertEqual(levels[0].source_website, "tpac")
        
        self.assertEqual(len(sections), 1)
        self.assertEqual(sections[0].name, "Orchestra")
        self.assertEqual(sections[0].source_website, "tpac")


class TestTPACScraper(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        self.scraper = TPACScraper(
            url="https://cart.tpac.org/14390/14430",
            config={"source_website": "tpac"}
        )

    def test_scraper_name(self):
        """Test scraper name property"""
        self.assertEqual(self.scraper.name, "tpac_scraper_v1")

    def test_get_required_fields(self):
        """Test required fields"""
        required_fields = self.scraper.get_required_fields()
        expected_fields = ["venue_info", "zones", "levels"]
        self.assertEqual(required_fields, expected_fields)

    @patch.object(TPACExtractor, 'extract')
    async def test_extract_data_success(self, mock_extract):
        """Test successful data extraction"""
        mock_extract.return_value = (
            {"performance_details": {"facility_desc": "Test Venue"}},
            {"screens": [{"screen_no": "1"}], "seat_lists": {"1": {}}}
        )
        
        result = await self.scraper.extract_data()
        
        self.assertIn("performance_data", result)
        self.assertIn("seat_data", result)

    @patch.object(TPACExtractor, 'extract')
    async def test_extract_data_no_seat_data(self, mock_extract):
        """Test extraction with no seat data"""
        mock_extract.return_value = ({"performance_details": {}}, None)
        
        with self.assertRaises(ParseException):
            await self.scraper.extract_data()

    @patch.object(TPACProcessor, 'process')
    async def test_process_data_success(self, mock_process):
        """Test successful data processing"""
        # Create mock scraped data
        mock_scraped_data = MagicMock()
        mock_scraped_data.venue_info.name = "Test Venue"
        mock_scraped_data.event_info.name = "Test Event"
        mock_scraped_data.zones = []
        mock_scraped_data.levels = []
        mock_scraped_data.seats = []
        mock_scraped_data.seat_packs = []
        mock_scraped_data.source_website = "tpac"
        mock_scraped_data.scraped_at = datetime.utcnow()
        mock_scraped_data.url = "https://cart.tpac.org/14390/14430"
        
        mock_process.return_value = mock_scraped_data
        
        raw_data = {
            "performance_data": {"performance_details": {}},
            "seat_data": {"screens": [], "seat_lists": {}}
        }
        
        result = await self.scraper.process_data(raw_data)
        
        self.assertIn("venue_info", result)
        self.assertIn("event_info", result)
        self.assertEqual(result["status"], "success")


if __name__ == '__main__':
    unittest.main()