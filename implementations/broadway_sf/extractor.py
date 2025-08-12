"""
Broadway SF extractor using the new API-based architecture.

This module now serves as a compatibility layer that uses the new
centralized API scraping infrastructure while maintaining the same
interface for the existing scraper implementation.
"""

import logging
from typing import Dict, Any, Tuple, Optional

from .api_scraper import BroadwaySFApiScraper


class BroadwaySFExtractor:
    """
    Broadway SF extractor using the new API-based architecture.
    
    This class serves as a compatibility layer that uses the new
    centralized API scraping infrastructure while maintaining the same
    interface for the existing scraper implementation.
    
    Key improvements:
    - Uses centralized HttpRequestClient with connection pooling
    - Integrated proxy management through ProxyService
    - Robust error handling and retry logic
    - Response validation with BroadwaySFResponseValidator
    - Clean separation of concerns following SOLID principles
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def extract(self, url: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Any]]:
        """
        Extract data using the new API-based scraper.
        
        This method now uses the centralized API scraping infrastructure
        instead of the previous manual implementation, providing:
        - Better error handling and retry logic
        - Automatic proxy integration
        - Response validation
        - Connection pooling and performance optimization
        
        Args:
            url: Broadway SF event URL
            
        Returns:
            Tuple of (calendar_data, seating_data, scraper_instance) for compatibility
            
        Raises:
            Exception: If extraction fails
        """
        try:
            self.logger.info(f"Starting Broadway SF extraction using API scraper for: {url}")
            
            # Use the new API-based scraper with centralized architecture
            api_scraper = BroadwaySFApiScraper(url=url)
            
            try:
                # Extract all data using the new architecture
                combined_data = await api_scraper.extract_all_data()
                
                # Extract the components for compatibility with existing processor
                calendar_data = combined_data.get('calendar_data')
                seating_data = combined_data.get('seating_data')
                
                self.logger.info("Successfully extracted data using API-based scraper")
                return calendar_data, seating_data, api_scraper
                
            except Exception as e:
                # Clean up on error
                api_scraper.cleanup()
                raise

        except Exception as e:
            self.logger.error(f"API-based extraction failed: {e}")
            raise Exception(f"Broadway SF extraction failed: {e}")

    def _parse_calendar_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse Broadway SF calendar data - return dict instead of dataclass"""
        if not raw_data:
            return None

        try:
            # Validate that this looks like calendar service data
            if 'data' in raw_data and 'getShow' in raw_data.get('data', {}):
                return raw_data
            return None
        except Exception:
            return None

    def _parse_seating_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse Broadway SF seating data - return dict instead of dataclass"""
        if not raw_data:
            return None

        try:
            # Validate that this looks like bolt seating data
            if 'seats' in raw_data and 'zones' in raw_data:
                return raw_data
            return None
        except Exception:
            return None