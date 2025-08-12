import logging
import re
from typing import Dict, Type, Optional, Tuple, Union, Any
from urllib.parse import urlparse

from .base import BaseScraper
from .core import RetryConfig, BackoffStrategy
from .exceptions import ConfigurationException
from .core.data_schemas import ScrapedData

# Performance optimizer not available


class ScraperFactory:
    """Enhanced factory for creating scrapers with configuration and validation support"""

    logger = logging.getLogger(__name__ + '.ScraperFactory')

    # Separate storage for different pattern types for faster lookup
    _exact_patterns: Dict[str, Type[BaseScraper]] = {}
    _domain_patterns: Dict[str, Type[BaseScraper]] = {}
    _regex_patterns: Dict[str, Tuple[re.Pattern, Type[BaseScraper]]] = {}

    # Cache for compiled regex patterns and recent lookups
    _compiled_regex_cache: Dict[str, re.Pattern] = {}
    _url_cache: Dict[str, Optional[Type[BaseScraper]]] = {}
    _cache_size_limit = 100

    @classmethod
    def register_scraper(cls, pattern: str, scraper_class: Type[BaseScraper]):
        """Register a scraper for a URL pattern with optimized categorization"""
        pattern_lower = pattern.lower()

        # Clear URL cache when new scrapers are registered
        cls._url_cache.clear()

        # Categorize patterns for optimal lookup
        if cls._is_regex_pattern(pattern):
            # Compile and cache regex patterns
            try:
                compiled_pattern = re.compile(pattern_lower)
                cls._regex_patterns[pattern_lower] = (compiled_pattern, scraper_class)
            except re.error:
                # Fallback to an exact pattern if regex compilation fails
                cls._exact_patterns[pattern_lower] = scraper_class
        elif cls._is_domain_pattern(pattern):
            # Domain patterns (e.g., "example.com")
            cls._domain_patterns[pattern_lower] = scraper_class
        else:
            # Exact substring patterns
            cls._exact_patterns[pattern_lower] = scraper_class

    @classmethod
    def create_scraper(cls, url: str, scrape_job_id: str = None,
                       optimization_enabled: bool = False,
                       optimization_level: str = "balanced",
                       retry_config: Optional[RetryConfig] = None) -> Optional[BaseScraper]:
        """Create the appropriate scraper with enhanced configuration options"""
        if not url:
            cls.logger.warning("No URL provided to create_scraper")
            return None

        # Extract domain for database lookup
        domain = cls.extract_base_domain(url)

        # 1. Check database for scraper definition first
        scraper_definition = cls.get_scraper_definition_by_domain(domain)
        if scraper_definition:
            cls.logger.debug(f"Found database scraper definition for domain {domain}: {scraper_definition.name}")
            return cls._create_scraper_from_definition(
                scraper_definition, url, scrape_job_id,
                optimization_enabled, optimization_level, retry_config
            )

        # 2. Fallback to existing hardcoded patterns for other scrapers
        # Check cache first
        cache_key = url.lower()
        if cache_key in cls._url_cache:
            scraper_class = cls._url_cache[cache_key]
            if scraper_class:
                return cls._instantiate_scraper(
                    scraper_class, url, scrape_job_id,
                    optimization_enabled, optimization_level, retry_config
                )
            return None

        url_lower = cache_key
        parsed_url = urlparse(url_lower)
        domain_fallback = parsed_url.netloc.lower()

        scraper_class = None

        # Try domain-specific lookup (for remaining hardcoded scrapers)
        if domain_fallback in cls._domain_patterns:
            scraper_class = cls._domain_patterns[domain_fallback]
            cls.logger.debug(f"Found scraper for domain {domain_fallback}: {scraper_class.__name__}")

        # Try exact pattern matching (fast)
        if not scraper_class:
            for pattern, sc in cls._exact_patterns.items():
                if pattern in url_lower:
                    scraper_class = sc
                    cls.logger.debug(f"Found scraper for pattern {pattern}: {sc.__name__}")
                    break

        # Try regex patterns (slower, only if needed)
        if not scraper_class:
            for pattern_str, (compiled_pattern, sc) in cls._regex_patterns.items():
                if compiled_pattern.search(url_lower):
                    scraper_class = sc
                    cls.logger.debug(f"Found scraper for regex {pattern_str}: {sc.__name__}")
                    break

        # Cache the result (with size limit)
        if len(cls._url_cache) >= cls._cache_size_limit:
            # Remove oldest entries (simple FIFO)
            oldest_keys = list(cls._url_cache.keys())[:10]
            for key in oldest_keys:
                del cls._url_cache[key]

        cls._url_cache[cache_key] = scraper_class

        if scraper_class:
            return cls._instantiate_scraper(
                scraper_class, url, scrape_job_id,
                optimization_enabled, optimization_level, retry_config
            )

        cls.logger.warning(f"No scraper found for URL: {url}")
        return None

    @classmethod
    def _create_scraper_from_definition(cls, scraper_definition, url: str,
                                        scrape_job_id: Optional[str] = None,
                                        optimization_enabled: bool = False,
                                        optimization_level: str = "balanced",
                                        retry_config: Optional[RetryConfig] = None) -> Optional[BaseScraper]:
        """Create scraper instance from ScraperDefinition"""
        try:
            scraper_name = scraper_definition.name

            # Map scraper names to classes
            if scraper_name == 'washington_pavilion_scraper_v5' or 'washington' in scraper_name.lower():
                from .implementations.washington_pavilion.scraper import WashingtonPavilionScraper
                scraper = WashingtonPavilionScraper(
                    url=url,
                    scrape_job_id=scrape_job_id,
                    optimization_enabled=optimization_enabled,
                    optimization_level=optimization_level,
                    scraper_definition=scraper_definition
                )

                if retry_config and hasattr(scraper, 'set_retry_config'):
                    scraper.set_retry_config(retry_config)

                return scraper
            elif scraper_name == 'broadway_sf_scraper_v5' or 'broadway' in scraper_name.lower():
                from .implementations.broadway_sf.scraper import BroadwaySFScraper
                scraper = BroadwaySFScraper(
                    url=url,
                    scrape_job_id=scrape_job_id,
                    optimization_enabled=optimization_enabled,
                    optimization_level=optimization_level,
                    scraper_definition=scraper_definition
                )

                if retry_config and hasattr(scraper, 'set_retry_config'):
                    scraper.set_retry_config(retry_config)

                return scraper
            elif scraper_name == 'david_h_koch_theater_scraper_v5' or 'david_h_koch' in scraper_name.lower() or 'koch' in scraper_name.lower():
                from .implementations.david_h_koch_theater.scraper import DavidHKochTheaterScraper
                scraper = DavidHKochTheaterScraper(
                    url=url,
                    scrape_job_id=scrape_job_id,
                    optimization_enabled=optimization_enabled,
                    optimization_level=optimization_level,
                    scraper_definition=scraper_definition
                )

                if retry_config and hasattr(scraper, 'set_retry_config'):
                    scraper.set_retry_config(retry_config)

                return scraper
            elif scraper_name == 'demo_scraper_v1' or 'demo_scraper' in scraper_name.lower():
                from .implementations.demo_scraper.scraper import DemoScraper
                scraper = DemoScraper(
                    url=url,
                    scrape_job_id=scrape_job_id,
                    optimization_enabled=optimization_enabled,
                    optimization_level=optimization_level,
                    scraper_definition=scraper_definition
                )

                if retry_config and hasattr(scraper, 'set_retry_config'):
                    scraper.set_retry_config(retry_config)

                return scraper
            elif scraper_name == 'vividseats_scraper_v1' or 'vividseats' in scraper_name.lower():
                from .implementations.vividseats.scraper import VividSeatsScraper
                scraper = VividSeatsScraper(
                    url=url,
                    scrape_job_id=scrape_job_id,
                    optimization_enabled=optimization_enabled,
                    optimization_level=optimization_level,
                    scraper_definition=scraper_definition
                )

                if retry_config and hasattr(scraper, 'set_retry_config'):
                    scraper.set_retry_config(retry_config)

                return scraper
            elif scraper_name == 'tpac_scraper_v1' or 'tpac' in scraper_name.lower():
                from .implementations.tpac.scraper import TPACScraper
                scraper = TPACScraper(
                    url=url,
                    scrape_job_id=scrape_job_id,
                    optimization_enabled=optimization_enabled,
                    optimization_level=optimization_level,
                    scraper_definition=scraper_definition
                )

                if retry_config and hasattr(scraper, 'set_retry_config'):
                    scraper.set_retry_config(retry_config)

                return scraper
            elif scraper_name == 'colorado_ballet_scraper_v1' or 'colorado_ballet' in scraper_name.lower():
                from .implementations.colorado_ballet.scraper import ColoradoBalletScraper
                scraper = ColoradoBalletScraper(
                    url=url,
                    scrape_job_id=scrape_job_id,
                    optimization_enabled=optimization_enabled,
                    optimization_level=optimization_level,
                    scraper_definition=scraper_definition
                )

                if retry_config and hasattr(scraper, 'set_retry_config'):
                    scraper.set_retry_config(retry_config)

                return scraper
            else:
                cls.logger.warning(f"Unknown scraper name from database: {scraper_name}")
                return None

        except Exception as e:
            cls.logger.error(f"Failed to create scraper from definition: {e}")
            return None

    @classmethod
    def _instantiate_scraper(cls, scraper_class: Type[BaseScraper], url: str,
                             scrape_job_id: Optional[str] = None,
                             optimization_enabled: bool = False,
                             optimization_level: str = "balanced",
                             retry_config: Optional[RetryConfig] = None) -> BaseScraper:
        """Instantiate scraper with configuration"""
        try:
            scraper = scraper_class(
                url=url,
                scrape_job_id=scrape_job_id,
                optimization_enabled=optimization_enabled,
                optimization_level=optimization_level
            )

            # Set retry configuration if provided
            if retry_config and hasattr(scraper, 'set_retry_config'):
                scraper.set_retry_config(retry_config)

            return scraper

        except Exception as e:
            cls.logger.error(f"Failed to instantiate scraper {scraper_class.__name__}: {e}")
            raise ConfigurationException(f"Scraper instantiation failed: {e}")

    @classmethod
    def get_available_scrapers(cls) -> Dict[str, Type[BaseScraper]]:
        """Get all registered scrapers"""
        all_scrapers = {}
        all_scrapers.update(cls._exact_patterns)
        all_scrapers.update(cls._domain_patterns)
        # For regex patterns, just include the pattern string
        for pattern_str, (_, scraper_class) in cls._regex_patterns.items():
            all_scrapers[pattern_str] = scraper_class
        return all_scrapers

    @classmethod
    def get_scraper_for_url(cls, url: str) -> Optional[str]:
        """Get the scraper name that would handle this URL"""
        try:
            scraper = cls.create_scraper(url)
            return scraper.name if scraper else None
        except Exception as e:
            cls.logger.error(f"Error getting scraper for URL {url}: {e}")
            return None

    @classmethod
    def create_scraper_with_defaults(cls, url: str, scrape_job_id: str = None) -> Optional[BaseScraper]:
        """Create scraper with optimized default settings"""
        default_retry_config = RetryConfig(
            max_retries=3,
            base_delay=2.0,
            max_delay=60.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER
        )

        return cls.create_scraper(
            url=url,
            scrape_job_id=scrape_job_id,
            optimization_enabled=True,
            optimization_level="balanced",
            retry_config=default_retry_config
        )

    @classmethod
    def validate_scraper_config(cls, scraper_class: Type[BaseScraper]) -> bool:
        """Validate that a scraper class is properly configured"""
        try:
            # Check if scraper has required methods
            required_methods = ['extract_data', 'process_data', 'store_in_database']
            for method in required_methods:
                if not hasattr(scraper_class, method):
                    cls.logger.error(f"Scraper {scraper_class.__name__} missing required method: {method}")
                    return False

            # Check if scraper has name property
            if not hasattr(scraper_class, 'name'):
                cls.logger.error(f"Scraper {scraper_class.__name__} missing name property")
                return False

            return True

        except Exception as e:
            cls.logger.error(f"Error validating scraper config: {e}")
            return False

    @classmethod
    def clear_cache(cls):
        """Clear all caches - useful for testing or memory management"""
        cls._url_cache.clear()
        cls._compiled_regex_cache.clear()

    @classmethod
    def get_cache_stats(cls) -> Dict[str, int]:
        """Get cache statistics for monitoring"""
        return {
            'url_cache_size': len(cls._url_cache),
            'regex_cache_size': len(cls._compiled_regex_cache),
            'exact_patterns': len(cls._exact_patterns),
            'domain_patterns': len(cls._domain_patterns),
            'regex_patterns': len(cls._regex_patterns)
        }

    @staticmethod
    def _is_regex_pattern(pattern: str) -> bool:
        """Check if a pattern contains regex metacharacters (excluding simple dots)"""
        # Advanced regex chars that definitely indicate regex
        advanced_regex_chars = set('*+?^${}[]|()')
        if any(char in pattern for char in advanced_regex_chars):
            return True

        # Check for escaped characters (like \.)
        if '\\' in pattern:
            return True

        # If it just contains dots but looks like a domain, it's probably not regex
        return False

    @staticmethod
    def _is_domain_pattern(pattern: str) -> bool:
        """Check if the pattern looks like a domain (contains . but no / and no regex chars)"""
        if ScraperFactory._is_regex_pattern(pattern):
            return False
        # Simple domain check: contains dots, no slashes, no spaces, looks like domain
        return ('.' in pattern and
                '/' not in pattern and
                ' ' not in pattern and
                not pattern.startswith('http') and
                len(pattern.split('.')) >= 2)

    @staticmethod
    def extract_base_domain(url: str) -> str:
        """Extract base domain from URL for database lookup"""
        from urllib.parse import urlparse
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return ""

    @classmethod
    def get_scraper_definition_by_domain(cls, domain: str):
        """Get full ScraperDefinition from database/cache by domain"""
        try:
            from django.core.cache import cache
            from .models.scraper_config import ScraperDefinition

            # Check cache first
            cache_key = f"scraper_definition:{domain}"
            cached_definition = cache.get(cache_key)
            if cached_definition:
                cls.logger.debug(f"Found cached scraper definition for domain {domain}")
                return cached_definition

            # Query all active ScraperDefinitions
            all_scrapers = ScraperDefinition.objects.filter(
                is_enabled=True,
                status='active'
            ).select_related('proxy_settings', 'captcha_type').prefetch_related('optimization_rules')

            # Check each scraper's target domains
            for scraper_def in all_scrapers:
                target_domains = scraper_def.target_domains or []
                if domain in target_domains or any(domain.endswith(td) for td in target_domains):
                    # Cache for 1 hour
                    cache.set(cache_key, scraper_def, 3600)
                    return scraper_def

            # Cache negative result for 5 minutes to avoid repeated DB queries
            cache.set(cache_key, None, 300)
            return None

        except Exception as e:
            cls.logger.error(f"Failed to get scraper definition by domain {domain}: {e}")
            return None
