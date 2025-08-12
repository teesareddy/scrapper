"""
Abstract base class for API-based scrapers following the existing BaseScraper pattern.

This module extends the scraper architecture to support direct API calls while 
maintaining consistency with the existing Playwright-based scrapers.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from enum import Enum

from .request_client import HttpRequestClient, RequestConfig, ProxyConfig, RequestResult
from ..proxy.service import get_proxy_service
from ..exceptions.scraping_exceptions import (
    ScrapingException, NetworkException, ParseException, ValidationException
)


class ApiScrapingStrategy(Enum):
    """Different API scraping strategies."""
    REST_JSON = "rest_json"
    GRAPHQL = "graphql"
    XML_SOAP = "xml_soap"
    FORM_DATA = "form_data"


@dataclass
class ApiEndpoint:
    """Configuration for a specific API endpoint."""
    url: str
    method: str = "GET"
    headers: Optional[Dict[str, str]] = None
    required_params: Optional[List[str]] = None
    response_type: str = "json"
    timeout: Optional[int] = None
    
    def __post_init__(self):
        """Validate endpoint configuration."""
        if not self.url:
            raise ValueError("URL is required for API endpoint")
        
        valid_methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]
        if self.method.upper() not in valid_methods:
            raise ValueError(f"Invalid HTTP method: {self.method}")
        
        self.method = self.method.upper()
        
        if self.headers is None:
            self.headers = {}
        
        if self.required_params is None:
            self.required_params = []


@dataclass
class ApiScrapingConfig:
    """Configuration for API-based scraping."""
    strategy: ApiScrapingStrategy
    endpoints: Dict[str, ApiEndpoint]
    request_config: Optional[RequestConfig] = None
    proxy_required: bool = False
    rate_limit_delay: float = 0.0
    user_agent: Optional[str] = None
    
    def __post_init__(self):
        """Validate and set defaults for API scraping configuration."""
        if not self.endpoints:
            raise ValueError("At least one API endpoint must be configured")
        
        if self.request_config is None:
            self.request_config = RequestConfig()
        
        # Override user agent if provided
        if self.user_agent:
            self.request_config.user_agent = self.user_agent


class IApiExtractor(ABC):
    """Interface for API data extraction following Interface Segregation Principle."""
    
    @abstractmethod
    async def extract_from_endpoint(self, endpoint_name: str, **kwargs) -> RequestResult:
        """Extract data from a specific API endpoint."""
        pass
    
    @abstractmethod
    async def validate_response(self, result: RequestResult, endpoint_name: str) -> bool:
        """Validate API response for specific endpoint."""
        pass


class IApiProcessor(ABC):
    """Interface for API data processing."""
    
    @abstractmethod
    def process_api_response(self, response_data: Dict[str, Any], 
                           endpoint_name: str) -> Dict[str, Any]:
        """Process raw API response data."""
        pass
    
    @abstractmethod
    def combine_responses(self, responses: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Combine multiple API responses into unified data structure."""
        pass


class BaseApiScraper(IApiExtractor, IApiProcessor):
    """
    Abstract base class for API-based scrapers.
    
    This class provides the foundation for scrapers that interact with APIs
    instead of scraping web pages. It maintains consistency with the existing
    BaseScraper architecture while providing API-specific functionality.
    
    Key Features:
    - Automatic proxy integration
    - Request rate limiting
    - Response validation
    - Error handling and retry logic
    - Structured logging
    """
    
    def __init__(self, url: str, scrape_job_id: Optional[str] = None,
                 config: Optional[ApiScrapingConfig] = None,
                 scraper_definition=None):
        """
        Initialize API scraper with configuration.
        
        Args:
            url: Base URL or primary URL for the scraper
            scrape_job_id: Optional job ID for tracking
            config: API scraping configuration
            scraper_definition: Scraper definition from database
        """
        self.url = url
        self.scrape_job_id = scrape_job_id
        self.config = config or self._get_default_config()
        self.scraper_definition = scraper_definition
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize HTTP client
        self._http_client: Optional[HttpRequestClient] = None
        self._proxy_config: Optional[ProxyConfig] = None
        
        # Response cache for multiple endpoint calls
        self._response_cache: Dict[str, RequestResult] = {}
        
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the unique name of the scraper."""
        pass
    
    @abstractmethod
    def _get_default_config(self) -> ApiScrapingConfig:
        """Get default configuration for this scraper."""
        pass
    
    @abstractmethod
    async def parse_url_parameters(self, url: str) -> Dict[str, str]:
        """Parse URL to extract parameters needed for API calls."""
        pass
    
    async def _get_http_client(self) -> HttpRequestClient:
        """Get or create HTTP client with proxy configuration."""
        if self._http_client is None:
            # Get proxy configuration if required
            if self.config.proxy_required or self._should_use_proxy():
                self._proxy_config = await self._get_proxy_config()
            
            # Create HTTP client with configuration
            self._http_client = HttpRequestClient(
                config=self.config.request_config,
                proxy_config=self._proxy_config
            )
            
            self.logger.info(f"Initialized HTTP client for {self.name}")
        
        return self._http_client
    
    def _should_use_proxy(self) -> bool:
        """Check if proxy should be used based on environment."""
        import os
        return os.getenv('USE_PROXY', 'false').lower() == 'true'
    
    async def _get_proxy_config(self) -> Optional[ProxyConfig]:
        """Get proxy configuration using the existing proxy service."""
        try:
            from asgiref.sync import sync_to_async
            
            get_proxy_sync = sync_to_async(
                lambda: get_proxy_service().get_proxy_for_scraper(self.name),
                thread_sensitive=True
            )
            
            credentials = await get_proxy_sync()
            
            if credentials:
                return ProxyConfig(
                    host=credentials.host,
                    port=credentials.port,
                    username=credentials.username,
                    password=credentials.password,
                    protocol="http"
                )
            
            return None
            
        except Exception as e:
            # Check if this is a proxy requirement failure
            if "fail_without_proxy=True" in str(e):
                self.logger.error(f"Proxy requirement failure: {e}")
                raise NetworkException(f"Proxy required but not available: {e}")
            else:
                self.logger.warning(f"Failed to get proxy configuration: {e}")
                return None
    
    async def extract_from_endpoint(self, endpoint_name: str, **kwargs) -> RequestResult:
        """
        Extract data from a specific API endpoint.
        
        Args:
            endpoint_name: Name of the endpoint from configuration
            **kwargs: Additional parameters for the API call
            
        Returns:
            RequestResult with response data
            
        Raises:
            ScrapingException: If endpoint not found or request fails
        """
        if endpoint_name not in self.config.endpoints:
            raise ScrapingException(f"Endpoint '{endpoint_name}' not found in configuration")
        
        endpoint = self.config.endpoints[endpoint_name]
        http_client = await self._get_http_client()
        
        # Prepare request parameters
        headers = endpoint.headers.copy() if endpoint.headers else {}
        
        # Add any additional headers from kwargs
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        
        # Log the API call
        self.logger.info(f"Calling {endpoint_name} endpoint: {endpoint.method} {endpoint.url}")
        
        try:
            # Apply rate limiting if configured
            if self.config.rate_limit_delay > 0:
                import asyncio
                await asyncio.sleep(self.config.rate_limit_delay)
            
            # Make the API request
            if endpoint.method == "GET":
                result = await http_client.get(
                    url=endpoint.url,
                    headers=headers,
                    params=kwargs.get('params')
                )
            elif endpoint.method == "POST":
                result = await http_client.post(
                    url=endpoint.url,
                    headers=headers,
                    json_data=kwargs.get('json_data'),
                    data=kwargs.get('data')
                )
            else:
                raise ScrapingException(f"HTTP method {endpoint.method} not implemented")
            
            # Validate response
            if not await self.validate_response(result, endpoint_name):
                raise ParseException(f"Response validation failed for {endpoint_name}")
            
            # Cache successful response
            self._response_cache[endpoint_name] = result
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to extract from {endpoint_name}: {e}")
            if isinstance(e, (NetworkException, ParseException, ScrapingException)):
                raise
            else:
                raise ScrapingException(f"Unexpected error calling {endpoint_name}: {e}")
    
    async def validate_response(self, result: RequestResult, endpoint_name: str) -> bool:
        """
        Validate API response for specific endpoint.
        
        Default implementation checks for successful status code and non-empty data.
        Override in subclasses for endpoint-specific validation.
        
        Args:
            result: The API response result
            endpoint_name: Name of the endpoint
            
        Returns:
            True if response is valid, False otherwise
        """
        if not result.success:
            self.logger.warning(f"Request failed for {endpoint_name}: {result.error_message}")
            return False
        
        if not result.data:
            self.logger.warning(f"Empty response data for {endpoint_name}")
            return False
        
        return True
    
    def process_api_response(self, response_data: Dict[str, Any], 
                           endpoint_name: str) -> Dict[str, Any]:
        """
        Process raw API response data.
        
        Default implementation returns data as-is.
        Override in subclasses for endpoint-specific processing.
        
        Args:
            response_data: Raw response data from API
            endpoint_name: Name of the endpoint
            
        Returns:
            Processed data dictionary
        """
        return response_data
    
    def combine_responses(self, responses: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine multiple API responses into unified data structure.
        
        Default implementation merges all responses into a single dictionary.
        Override in subclasses for specific combination logic.
        
        Args:
            responses: Dictionary mapping endpoint names to response data
            
        Returns:
            Combined data dictionary
        """
        combined = {}
        for endpoint_name, response_data in responses.items():
            combined[endpoint_name] = response_data
        
        return combined
    
    async def extract_all_data(self) -> Dict[str, Any]:
        """
        Extract data from all configured endpoints.
        
        This method orchestrates calls to all configured endpoints and
        combines their responses into a unified data structure.
        
        Returns:
            Combined data from all endpoints
            
        Raises:
            ScrapingException: If critical endpoints fail
        """
        self.logger.info(f"Starting data extraction from {len(self.config.endpoints)} endpoints")
        
        # Parse URL to get parameters for API calls
        url_params = await self.parse_url_parameters(self.url)
        
        # Extract data from each endpoint
        responses = {}
        errors = {}
        
        for endpoint_name in self.config.endpoints.keys():
            try:
                result = await self.extract_from_endpoint(endpoint_name, **url_params)
                processed_data = self.process_api_response(result.data, endpoint_name)
                responses[endpoint_name] = processed_data
                
            except Exception as e:
                errors[endpoint_name] = str(e)
                self.logger.error(f"Failed to extract from {endpoint_name}: {e}")
        
        # Check if we have any successful responses
        if not responses:
            error_summary = "; ".join([f"{ep}: {err}" for ep, err in errors.items()])
            raise ScrapingException(f"All endpoints failed: {error_summary}")
        
        # Log any partial failures
        if errors:
            error_summary = "; ".join([f"{ep}: {err}" for ep, err in errors.items()])
            self.logger.warning(f"Some endpoints failed: {error_summary}")
        
        # Combine responses
        combined_data = self.combine_responses(responses)
        
        self.logger.info(f"Successfully extracted data from {len(responses)} endpoints")
        return combined_data
    
    def cleanup(self) -> None:
        """Clean up resources."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None
        
        self._response_cache.clear()
        self.logger.info(f"Cleaned up resources for {self.name}")
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.cleanup()
        except:
            pass  # Ignore errors during cleanup in destructor
    
    # ===============================================
    # SEATING ANALYSIS & SEAT PACK GENERATION METHODS
    # ===============================================
    
    @abstractmethod
    def analyze_seating_structure(self, seat_data: List[Dict[str, Any]]) -> str:
        """
        Analyze venue seating structure and return the detected pattern.
        
        Args:
            seat_data: List of seat dictionaries from API response
            
        Returns:
            String indicating detected structure: "consecutive", "odd_even", "mixed"
        """
        pass
    
    @abstractmethod
    def generate_seat_packs(self, seats: List[Any], sections: List[Any], performance: Any) -> List[Any]:
        """
        Generate seat packs using venue-specific strategy.
        
        Args:
            seats: List of SeatData objects
            sections: List of SectionData objects  
            performance: PerformanceData object
            
        Returns:
            List of SeatPackData objects
        """
        pass
    
    @abstractmethod
    def get_seat_pack_strategy(self) -> str:
        """
        Return the seat pack generation strategy for this scraper.
        
        Returns:
            Strategy string: "consecutive", "odd_even", "mixed", "custom"
        """
        pass
    
    # ===============================================
    # UNIVERSAL PATTERN DETECTION HELPER METHODS
    # ===============================================
    
    def _is_consecutive(self, numbers: List[int]) -> bool:
        """Check if a list of numbers is a consecutive sequence."""
        if not numbers or len(numbers) < 2:
            return True  # A single seat is technically consecutive
        sorted_numbers = sorted(numbers)
        return sorted_numbers == list(range(min(sorted_numbers), max(sorted_numbers) + 1))
    
    def _is_all_odd(self, numbers: List[int]) -> bool:
        """Check if all numbers in a list are odd."""
        return all(n % 2 != 0 for n in numbers)
    
    def _is_all_even(self, numbers: List[int]) -> bool:
        """Check if all numbers in a list are even."""
        return all(n % 2 == 0 for n in numbers)
    
    def _group_seats_by_aisle_gaps(self, row_seats: List[Dict[str, Any]], x_gap_threshold: int = 50) -> List[List[Dict[str, Any]]]:
        """
        Group seats by physical sections based on X-coordinate gaps (aisles).
        
        Args:
            row_seats: List of seat dictionaries with 'x' coordinates
            x_gap_threshold: Minimum X gap to consider an aisle
            
        Returns:
            List of seat groups (sections within the row)
        """
        if not row_seats:
            return []
        
        # Sort seats by their horizontal position
        sorted_seats = sorted(row_seats, key=lambda s: s.get('x', 0))
        
        # Group seats by gaps
        sections = []
        if sorted_seats:
            current_section = [sorted_seats[0]]
            
            for i in range(1, len(sorted_seats)):
                prev_seat = sorted_seats[i - 1]
                current_seat = sorted_seats[i]
                
                # Calculate gap between seats
                gap = current_seat.get('x', 0) - prev_seat.get('x', 0)
                
                if gap > x_gap_threshold:
                    # Large gap detected - start new section
                    sections.append(current_section)
                    current_section = []
                
                current_section.append(current_seat)
            
            # Add the final section
            sections.append(current_section)
        
        return sections
    
    def _analyze_row_structure(self, row_seats: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze numbering structure for a single row.
        
        Args:
            row_seats: List of seat dictionaries for one row
            
        Returns:
            Dictionary with analysis results:
            {
                'consecutive_seats': List[Dict],
                'odd_seats': List[Dict], 
                'even_seats': List[Dict],
                'dominant_pattern': str,
                'sections': List[List[Dict]]
            }
        """
        if not row_seats:
            return {
                'consecutive_seats': [],
                'odd_seats': [],
                'even_seats': [],
                'dominant_pattern': 'unknown',
                'sections': []
            }
        
        # Group seats by aisle gaps to identify physical sections
        sections = self._group_seats_by_aisle_gaps(row_seats)
        
        # Analyze each section's numbering pattern
        consecutive_seats = []
        odd_seats = []
        even_seats = []
        
        for section in sections:
            try:
                # Convert seat numbers to integers for analysis
                section_numbers = [int(s.get('number', 0)) for s in section]
                
                # Determine pattern for this section
                if self._is_consecutive(section_numbers):
                    consecutive_seats.extend(section)
                elif self._is_all_odd(section_numbers):
                    odd_seats.extend(section)
                elif self._is_all_even(section_numbers):
                    even_seats.extend(section)
                # Note: Mixed sections fall through and don't get categorized
                
            except (ValueError, TypeError):
                # Skip sections with non-integer seat numbers
                continue
        
        # Determine dominant pattern
        pattern_counts = {
            'consecutive': len(consecutive_seats),
            'odd': len(odd_seats),
            'even': len(even_seats)
        }
        
        dominant_pattern = max(pattern_counts.items(), key=lambda x: x[1])[0]
        if pattern_counts[dominant_pattern] == 0:
            dominant_pattern = 'unknown'
        elif len(odd_seats) > 0 and len(even_seats) > 0:
            dominant_pattern = 'odd_even'
        
        return {
            'consecutive_seats': consecutive_seats,
            'odd_seats': odd_seats,
            'even_seats': even_seats,
            'dominant_pattern': dominant_pattern,
            'sections': sections
        }
    
    def _detect_venue_wide_pattern(self, all_seat_data: List[Dict[str, Any]]) -> str:
        """
        Analyze all seat data to determine venue-wide seating pattern.
        
        Args:
            all_seat_data: Complete list of seat dictionaries
            
        Returns:
            Venue pattern: "consecutive", "odd_even", "mixed"
        """
        if not all_seat_data:
            return "unknown"
        
        # Group seats by level and row
        seating_map = {}
        for seat in all_seat_data:
            level = seat.get('level')
            row = seat.get('row')
            
            # Skip seats with missing info or invalid numbers
            if not all([level, row, seat.get('number'), seat.get('x') is not None]):
                continue
            
            try:
                # Validate number is convertible to int
                int(seat.get('number'))
            except (ValueError, TypeError):
                continue
            
            if level not in seating_map:
                seating_map[level] = {}
            if row not in seating_map[level]:
                seating_map[level][row] = []
            seating_map[level][row].append(seat)
        
        # Analyze patterns across all rows
        pattern_votes = {
            'consecutive': 0,
            'odd_even': 0,
            'mixed': 0
        }
        
        total_rows = 0
        for level_rows in seating_map.values():
            for row_seats in level_rows.values():
                if len(row_seats) < 2:  # Skip single-seat rows
                    continue
                
                row_analysis = self._analyze_row_structure(row_seats)
                pattern = row_analysis['dominant_pattern']
                
                if pattern == 'consecutive':
                    pattern_votes['consecutive'] += 1
                elif pattern in ['odd', 'even', 'odd_even']:
                    pattern_votes['odd_even'] += 1
                else:
                    pattern_votes['mixed'] += 1
                
                total_rows += 1
        
        if total_rows == 0:
            return "unknown"
        
        # Determine venue-wide pattern based on majority vote
        dominant_pattern = max(pattern_votes.items(), key=lambda x: x[1])[0]
        
        # If more than 20% of rows have different patterns, classify as mixed
        dominant_count = pattern_votes[dominant_pattern]
        if dominant_count / total_rows < 0.8:
            return "mixed"
        
        return dominant_pattern
    
    def _validate_seat_pack_integrity(self, seat_packs: List[Any]) -> bool:
        """
        Validate that generated seat packs maintain integrity.
        
        Args:
            seat_packs: List of generated seat pack objects
            
        Returns:
            True if all packs are valid, False otherwise
        """
        if not seat_packs:
            return True
        
        try:
            for pack in seat_packs:
                # Validate pack has required attributes
                required_attrs = ['pack_id', 'seat_ids', 'pack_size', 'row_label']
                for attr in required_attrs:
                    if not hasattr(pack, attr) or getattr(pack, attr) is None:
                        self.logger.error(f"Invalid seat pack: missing {attr}")
                        return False
                
                # Validate pack size matches seat count
                seat_count = len(getattr(pack, 'seat_ids', []))
                pack_size = getattr(pack, 'pack_size', 0)
                if seat_count != pack_size:
                    self.logger.error(f"Pack size mismatch: {pack_size} vs {seat_count} seats")
                    return False
                
                # Validate pack has reasonable size (1-10 seats)
                if not (1 <= pack_size <= 10):
                    self.logger.error(f"Invalid pack size: {pack_size}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating seat pack integrity: {e}")
            return False
    
    def _format_seats_with_status(self, seat_list: List[Dict[str, Any]]) -> str:
        """Helper function to format seat numbers with their availability."""
        if not seat_list:
            return "[]"
        
        # Sort seats by their number before formatting
        sorted_seats = sorted(seat_list, key=lambda s: int(s.get('number', 0)))
        formatted_strings = []
        
        for seat in sorted_seats:
            status = "Available" if seat.get('available') else "Unavailable"
            formatted_strings.append(f"{seat['number']} ({status})")
        
        return ", ".join(formatted_strings)