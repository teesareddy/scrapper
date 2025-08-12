"""
Tests for VividSeats scraper implementation
"""
import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from decimal import Decimal
from datetime import datetime

from .extractor import VividSeatsExtractor
from .processor import VividSeatsProcessor
from .scraper import VividSeatsScraper
from ...exceptions import NetworkException, ParseException


class TestVividSeatsExtractor:
    
    @pytest.fixture
    def extractor(self):
        return VividSeatsExtractor()
    
    def test_extract_production_id_success(self, extractor):
        url = "https://www.vividseats.com/shucked-tickets-fort-worth-bass-performance-hall-7-31-2025--theater-musical/production/4855476"
        production_id = extractor._extract_production_id(url)
        assert production_id == "4855476"
    
    def test_extract_production_id_failure(self, extractor):
        url = "https://www.vividseats.com/invalid-url"
        with pytest.raises(ParseException):
            extractor._extract_production_id(url)
    
    @pytest.mark.asyncio
    async def test_fetch_listings_data_success(self, extractor):
        mock_response_data = {
            "global": [{"productionName": "Test Event"}],
            "tickets": [],
            "sections": [],
            "groups": []
        }
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_get.return_value.__aenter__.return_value = mock_response
            
            extractor.session = Mock()
            extractor.session.get = mock_get
            
            result = await extractor._fetch_listings_data("123456")
            assert result == mock_response_data
    
    @pytest.mark.asyncio
    async def test_fetch_listings_data_no_global_info(self, extractor):
        mock_response_data = {
            "global": [],
            "tickets": [],
            "sections": [],
            "groups": []
        }
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_get.return_value.__aenter__.return_value = mock_response
            
            extractor.session = Mock()
            extractor.session.get = mock_get
            
            with pytest.raises(ParseException):
                await extractor._fetch_listings_data("123456")


class TestVividSeatsProcessor:
    
    @pytest.fixture
    def processor(self):
        config = {
            'source_website': 'vividseats',
            'venue_prefix': 'vs',
            'scraper_name': 'vividseats_scraper_v1'
        }
        return VividSeatsProcessor(config)
    
    @pytest.fixture
    def sample_listings_data(self):
        return {
            "global": [{
                "productionName": "Test Event",
                "mapTitle": "Test Venue",
                "productionId": "123456",
                "eventId": "789",
                "venueTimeZone": "America/New_York"
            }],
            "tickets": [{
                "l": "Orchestra",
                "r": "A",
                "m": "1,2",
                "p": "100.00",
                "z": "zone1",
                "g": "level1",
                "di": False
            }],
            "groups": [{
                "i": "zone1",
                "n": "Premium Zone",
                "l": "75.00",
                "h": "125.00",
                "q": "50"
            }],
            "sections": [{
                "i": "section1",
                "g": "level1",
                "n": "Orchestra",
                "q": "100"
            }]
        }
    
    @pytest.fixture
    def sample_production_details(self):
        return {
            "id": 123456,
            "name": "Test Event",
            "utcDate": "2025-07-31T19:00:00Z",
            "venue": {
                "name": "Test Venue",
                "city": "Fort Worth",
                "state": "TX",
                "countryCode": "US",
                "timezone": "America/Chicago"
            }
        }
    
    def test_get_global_info(self, processor, sample_listings_data):
        global_info = processor._get_global_info(sample_listings_data)
        assert global_info["productionName"] == "Test Event"
        assert global_info["mapTitle"] == "Test Venue"
    
    def test_process_venue_info(self, processor, sample_listings_data, sample_production_details):
        global_info = processor._get_global_info(sample_listings_data)
        venue_info = processor._process_venue_info(global_info, sample_production_details)
        
        assert venue_info.name == "Test Venue"
        assert venue_info.city == "Fort Worth"
        assert venue_info.state == "TX"
        assert venue_info.country == "US"
        assert venue_info.source_website == "vividseats"
    
    def test_process_zones(self, processor, sample_listings_data):
        groups = sample_listings_data["groups"]
        zones = processor._process_zones(groups)
        
        assert len(zones) == 1
        zone = zones[0]
        assert zone.zone_id == "zone1"
        assert zone.name == "Premium Zone"
        assert zone.min_price == Decimal("75.00")
        assert zone.max_price == Decimal("125.00")
    
    def test_process_seats(self, processor, sample_listings_data):
        # Create mock zones, levels, and sections
        zones = processor._process_zones(sample_listings_data["groups"])
        levels = processor._process_levels(sample_listings_data["sections"])
        sections = processor._process_sections(levels)
        
        seats = processor._process_seats(sample_listings_data["tickets"], zones, levels, sections)
        
        assert len(seats) == 2  # Two seats from "1,2"
        seat1 = seats[0]
        assert seat1.row_label == "A"
        assert seat1.seat_number == "1"
        assert seat1.zone_id == "zone1"
        assert seat1.price == Decimal("100.00")


class TestVividSeatsScraper:
    
    @pytest.fixture
    def scraper(self):
        config = {
            'source_website': 'vividseats',
            'venue_prefix': 'vs'
        }
        return VividSeatsScraper(
            url="https://www.vividseats.com/test/production/123456",
            config=config
        )
    
    def test_scraper_name(self, scraper):
        assert scraper.name == "vividseats_scraper_v1"
    
    def test_get_required_fields(self, scraper):
        required_fields = scraper.get_required_fields()
        assert "venue_info" in required_fields
        assert "zones" in required_fields
        assert "levels" in required_fields
    
    @pytest.mark.asyncio
    async def test_extract_data_success(self, scraper):
        mock_listings = {"global": [{"productionName": "Test"}], "tickets": []}
        mock_details = {"id": 123456, "name": "Test Event"}
        
        with patch.object(scraper.extractor, 'extract', new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = (mock_listings, mock_details)
            
            result = await scraper.extract_data()
            
            assert "listings_data" in result
            assert "production_details" in result
            assert result["listings_data"] == mock_listings
            assert result["production_details"] == mock_details
    
    @pytest.mark.asyncio
    async def test_extract_data_no_listings(self, scraper):
        with patch.object(scraper.extractor, 'extract', new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = (None, {})
            
            with pytest.raises(ParseException):
                await scraper.extract_data()


if __name__ == "__main__":
    pytest.main([__file__])