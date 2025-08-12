"""
TPAC extractor for API-based data extraction
Based on the reference implementation in scraperref/tpac.py
Converted to async architecture following VividSeats pattern
"""
import re
import aiohttp
import asyncio
from typing import Dict, Any, Optional, Tuple, List
from ...exceptions import NetworkException, ParseException
from .types import TPACEventData, TPACPerformanceDetails, TPACScreen, TPACSeatListResponse


class TPACExtractor:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.session = None
        self.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 OPR/119.0.0.0",
            "x-requested-with": "XMLHttpRequest",
        }
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.config.get('timeout_seconds', 60))
        # Create connector with better SSL and connection handling
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            enable_cleanup_closed=True,
            use_dns_cache=True,
            ttl_dns_cache=300,  # Cache DNS for 5 minutes
            family=0,  # Allow both IPv4 and IPv6
            ssl=False  # Let aiohttp handle SSL verification automatically
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout, 
            headers=self.headers,
            connector=connector,
            cookie_jar=aiohttp.CookieJar()  # Enable cookie handling
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def extract(self, url: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Extract data from TPAC URL
        Returns tuple of (performance_data, seat_data) for compatibility with processor
        """
        if not self.session:
            async with self:
                return await self._extract_data(url)
        else:
            return await self._extract_data(url)

    async def _extract_data(self, url: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        try:
            # Extract base URL and performance ID
            base_url, performance_id = self._parse_tpac_url(url)
            
            # First, try to establish session by visiting the main page (like a browser would)
            try:
                async with self.session.get(url) as main_response:
                    # We don't need to process this response, just establish the session
                    pass
            except Exception:
                # If main page fails, continue anyway - the API calls will handle their own errors
                pass
            
            # Fetch performance details and screens concurrently
            performance_task = self._fetch_performance_details(base_url, performance_id)
            screens_task = self._fetch_screens(base_url, performance_id)
            
            performance_details, screens = await asyncio.gather(
                performance_task, screens_task, return_exceptions=True
            )
            
            # Handle any exceptions from the concurrent requests
            if isinstance(performance_details, Exception):
                raise performance_details
            if isinstance(screens, Exception):
                raise screens
            
            # Fetch seat lists for each screen concurrently
            facility_id = performance_details.get("facility_no")
            if not facility_id:
                raise ParseException("No facility_no found in performance details")
            
            seat_list_tasks = []
            for screen in screens:
                screen_id = screen.get("screen_no")
                if screen_id:
                    task = self._fetch_seat_list(base_url, performance_id, facility_id, screen_id)
                    seat_list_tasks.append((screen_id, task))
            
            # Execute all seat list requests concurrently
            seat_list_results = await asyncio.gather(
                *[task for _, task in seat_list_tasks], return_exceptions=True
            )
            
            # Process seat list results
            seat_lists = {}
            for i, (screen_id, _) in enumerate(seat_list_tasks):
                result = seat_list_results[i]
                if isinstance(result, Exception):
                    # Log warning but continue with other screens
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Failed to fetch seat list for screen {screen_id}: {result}")
                    continue
                seat_lists[screen_id] = result
            
            # Structure the data for the processor
            performance_data = {
                "performance_details": performance_details,
                "base_url": base_url,
                "performance_id": performance_id
            }
            
            seat_data = {
                "screens": screens,
                "seat_lists": seat_lists,
                "facility_id": facility_id
            }
            
            return performance_data, seat_data
            
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            raise NetworkException(f"TPAC extraction failed: {str(e)}")

    def _parse_tpac_url(self, url: str) -> Tuple[str, str]:
        """Parse TPAC URL to extract base URL and performance ID"""
        # Extract performance ID from URL (as in reference)
        match = re.search(r"/(\d+)$", url)
        if not match:
            raise ParseException("Performance ID not found in TPAC URL")
        
        performance_id = match.group(1)
        
        # Extract base URL (everything before the performance ID)
        base_url = url.rsplit('/', 1)[0]
        
        return base_url, performance_id

    async def _fetch_performance_details(self, base_url: str, performance_id: str) -> Dict[str, Any]:
        """Fetch performance details from TPAC API, handling queue system redirects"""
        url = f"{base_url}/GetPerformanceDetails?performanceId={performance_id}"
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise NetworkException(f"Performance details API returned status {response.status}")
                
                # Check if we got redirected to queue system
                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' in content_type:
                    response_text = await response.text()
                    if 'queue-it.net' in response_text or 'Pardon Our Interruption' in response_text:
                        raise NetworkException(f"TPAC event has queue system enabled. This is typically temporary during high-demand periods. Please try again later when queue is disabled.")
                    else:
                        raise ParseException(f"Expected JSON but got HTML from TPAC API: {url}")
                
                data = await response.json()
                
                # Validate response structure
                if not isinstance(data, dict):
                    raise ParseException("Invalid performance details API response format")
                
                # Check for required fields
                required_fields = ["facility_no", "description", "perf_dt", "facility_desc"]
                missing_fields = [field for field in required_fields if field not in data]
                if missing_fields:
                    raise ParseException(f"Missing required fields in performance details: {missing_fields}")
                
                return data
                
        except aiohttp.ClientError as e:
            error_msg = str(e)
            if "Temporary failure in name resolution" in error_msg or "Cannot connect to host" in error_msg:
                raise NetworkException(f"DNS resolution or network connectivity issue for TPAC. This may be temporary. Error: {error_msg}")
            raise NetworkException(f"Failed to fetch performance details: {error_msg}")
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            raise NetworkException(f"Unexpected error fetching performance details: {str(e)}")

    async def _fetch_screens(self, base_url: str, performance_id: str) -> List[Dict[str, Any]]:
        """Fetch screens from TPAC API, handling queue system redirects"""
        url = f"{base_url}/GetScreens?performanceId={performance_id}"
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise NetworkException(f"Screens API returned status {response.status}")
                
                # Check if we got redirected to queue system
                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' in content_type:
                    response_text = await response.text()
                    if 'queue-it.net' in response_text or 'Pardon Our Interruption' in response_text:
                        raise NetworkException(f"TPAC event has queue system enabled. This is typically temporary during high-demand periods. Please try again later when queue is disabled.")
                    else:
                        raise ParseException(f"Expected JSON but got HTML from TPAC API: {url}")
                
                data = await response.json()
                
                # Validate response structure
                if not isinstance(data, list):
                    raise ParseException("Invalid screens API response format - expected list")
                
                # Validate each screen has required fields
                for screen in data:
                    if not isinstance(screen, dict) or "screen_no" not in screen:
                        raise ParseException("Invalid screen data structure")
                
                return data
                
        except aiohttp.ClientError as e:
            error_msg = str(e)
            if "Temporary failure in name resolution" in error_msg or "Cannot connect to host" in error_msg:
                raise NetworkException(f"DNS resolution or network connectivity issue for TPAC. This may be temporary. Error: {error_msg}")
            raise NetworkException(f"Failed to fetch screens: {error_msg}")
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            raise NetworkException(f"Unexpected error fetching screens: {str(e)}")

    async def _fetch_seat_list(self, base_url: str, performance_id: str, facility_id: str, screen_id: str) -> Dict[str, Any]:
        """Fetch seat list for a specific screen from TPAC API, handling queue system redirects"""
        url = f"{base_url}/GetSeatList?performanceId={performance_id}&facilityId={facility_id}&screenId={screen_id}"
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise NetworkException(f"Seat list API returned status {response.status} for screen {screen_id}")
                
                # Check if we got redirected to queue system
                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' in content_type:
                    response_text = await response.text()
                    if 'queue-it.net' in response_text or 'Pardon Our Interruption' in response_text:
                        raise NetworkException(f"TPAC event has queue system enabled. This is typically temporary during high-demand periods. Please try again later when queue is disabled.")
                    else:
                        raise ParseException(f"Expected JSON but got HTML from TPAC API: {url}")
                
                data = await response.json()
                
                # Validate response structure
                if not isinstance(data, dict):
                    raise ParseException(f"Invalid seat list API response format for screen {screen_id}")
                
                # Check for expected fields (AvailablePrices and seats)
                if "AvailablePrices" not in data or "seats" not in data:
                    raise ParseException(f"Missing required fields in seat list for screen {screen_id}")
                
                return data
                
        except aiohttp.ClientError as e:
            error_msg = str(e)
            if "Temporary failure in name resolution" in error_msg or "Cannot connect to host" in error_msg:
                raise NetworkException(f"DNS resolution or network connectivity issue for TPAC. This may be temporary. Error: {error_msg}")
            raise NetworkException(f"Failed to fetch seat list for screen {screen_id}: {error_msg}")
        except Exception as e:
            if isinstance(e, (NetworkException, ParseException)):
                raise
            raise NetworkException(f"Unexpected error fetching seat list for screen {screen_id}: {str(e)}")

    async def _safe_request_with_retries(self, url: str, max_retries: int = 3) -> Dict[str, Any]:
        """Make a safe request with retries (similar to reference safe_get function)"""
        for attempt in range(1, max_retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        raise NetworkException(f"HTTP {response.status}")
            except Exception as e:
                if attempt == max_retries:
                    raise NetworkException(f"Failed to connect after {max_retries} attempts: {url}")
                # Wait before retry (exponential backoff)
                await asyncio.sleep(2 ** attempt)
        
        raise NetworkException(f"Failed to connect after {max_retries} attempts: {url}")