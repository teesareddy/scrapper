"""
VividSeats extractor for API-based data extraction
Based on the reference implementation in scraperref/vividseats.py
"""
import re
import aiohttp
import asyncio
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlparse
from ...exceptions import NetworkException, ParseException


class VividSeatsExtractor:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.session = None
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.config.get('timeout_seconds', 60))
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def extract(self, url: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Extract data from VividSeats URL
        Returns tuple of (listings_data, production_details)
        """
        if not self.session:
            async with self:
                return await self._extract_data(url)
        else:
            return await self._extract_data(url)

    async def _extract_data(self, url: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        try:
            # Extract production ID from URL
            production_id = self._extract_production_id(url)
            
            # Fetch listings data and production details concurrently
            listings_task = self._fetch_listings_data(production_id)
            details_task = self._fetch_production_details(production_id)
            
            listings_data, production_details = await asyncio.gather(
                listings_task, details_task, return_exceptions=True
            )
            
            # Handle any exceptions from the concurrent requests
            if isinstance(listings_data, Exception):
                raise listings_data
            if isinstance(production_details, Exception):
                raise production_details
                
            return listings_data, production_details
            
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            raise NetworkException(f"VividSeats extraction failed: {str(e)}")

    def _extract_production_id(self, url: str) -> str:
        """Extract production ID from VividSeats URL"""
        match = re.search(r'/production/(\d+)', url)
        if not match:
            raise ParseException("Production ID not found in URL")
        return match.group(1)

    async def _fetch_listings_data(self, production_id: str) -> Dict[str, Any]:
        """Fetch listings data from VividSeats API"""
        listings_url = "https://www.vividseats.com/hermes/api/v1/listings"
        listings_params = {
            "productionId": production_id,
            "includeIpAddress": "true",
            "currency": "USD",
            "priceGroupId": "291",
            "localizeCurrency": "true"
        }
        
        try:
            async with self.session.get(listings_url, params=listings_params) as response:
                if response.status != 200:
                    raise NetworkException(f"Listings API returned status {response.status}")
                
                data = await response.json()
                
                # Validate response structure
                if not isinstance(data, dict):
                    raise ParseException("Invalid listings API response format")
                
                # Check for global info to validate the response
                global_list = data.get("global", [])
                if not global_list:
                    raise ParseException("No 'global' info found in response. Check if productionId is valid or API limit hit.")
                
                return data
                
        except aiohttp.ClientError as e:
            raise NetworkException(f"Failed to fetch listings data: {str(e)}")
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            raise NetworkException(f"Unexpected error fetching listings: {str(e)}")

    async def _fetch_production_details(self, production_id: str) -> Dict[str, Any]:
        """Fetch production details from VividSeats API"""
        details_url = f"https://www.vividseats.com/hermes/api/v1/productions/{production_id}/details?currency=USD"
        
        try:
            async with self.session.get(details_url) as response:
                if response.status != 200:
                    raise NetworkException(f"Details API returned status {response.status}")
                
                data = await response.json()
                
                # Validate response structure
                if not isinstance(data, dict):
                    raise ParseException("Invalid details API response format")
                
                return data
                
        except aiohttp.ClientError as e:
            raise NetworkException(f"Failed to fetch production details: {str(e)}")
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            raise NetworkException(f"Unexpected error fetching details: {str(e)}")

    def _validate_response_data(self, data: Dict[str, Any]) -> bool:
        """Validate that response data contains required fields"""
        required_fields = ["global", "tickets", "sections", "groups"]
        return all(field in data for field in required_fields)