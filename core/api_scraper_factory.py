"""
Factory for creating API-based scrapers following the existing factory pattern.

This module provides a centralized factory for creating API scrapers with proper
dependency injection and configuration management, following the Factory Pattern
established in the existing codebase.
"""

import logging
from typing import Dict, Any, Optional, Type, Union
from dataclasses import dataclass
from enum import Enum

from .api_scraper_base import BaseApiScraper, ApiScrapingConfig, ApiScrapingStrategy
from .response_validators import IResponseValidator, ValidatorFactory, ValidationLevel
from .request_client import RequestConfig
from ..exceptions.scraping_exceptions import ScrapingException


class ScraperType(Enum):
    """Supported scraper types for API-based scraping."""
    BROADWAY_SF = "broadway_sf"
    DAVID_H_KOCH = "david_h_koch_theater"
    WASHINGTON_PAVILION = "washington_pavilion"
    GENERIC_REST = "generic_rest"
    GENERIC_GRAPHQL = "generic_graphql"


@dataclass
class ScraperDefinition:
    """Definition for creating a scraper with all necessary configuration."""
    scraper_type: ScraperType
    name: str
    base_url: str
    api_config: ApiScrapingConfig
    validator_type: str = "base"
    validation_level: ValidationLevel = ValidationLevel.STRICT
    proxy_required: bool = False
    rate_limit_delay: float = 0.0
    description: Optional[str] = None
    
    def __post_init__(self):
        """Validate scraper definition after initialization."""
        if not self.name:
            raise ValueError("Scraper name is required")
        if not self.base_url:
            raise ValueError("Base URL is required")
        if not isinstance(self.api_config, ApiScrapingConfig):
            raise ValueError("Valid ApiScrapingConfig is required")


class ApiScraperRegistry:
    """Registry for managing scraper definitions and configurations."""
    
    def __init__(self):
        self._scrapers: Dict[str, ScraperDefinition] = {}
        self._scraper_classes: Dict[ScraperType, Type[BaseApiScraper]] = {}
        self.logger = logging.getLogger(__name__)
        
        # Register built-in scraper configurations
        self._register_builtin_scrapers()
    
    def _register_builtin_scrapers(self) -> None:
        """Register built-in scraper configurations."""
        # Broadway SF configuration will be added after we create the specific scraper
        pass
    
    def register_scraper(self, definition: ScraperDefinition,
                        scraper_class: Type[BaseApiScraper]) -> None:
        """
        Register a scraper definition with its implementation class.
        
        Args:
            definition: Complete scraper definition
            scraper_class: Implementation class for the scraper
        """
        self._scrapers[definition.name] = definition
        self._scraper_classes[definition.scraper_type] = scraper_class
        
        self.logger.info(f"Registered scraper: {definition.name} ({definition.scraper_type.value})")
    
    def get_scraper_definition(self, name: str) -> Optional[ScraperDefinition]:
        """Get scraper definition by name."""
        return self._scrapers.get(name)
    
    def get_scraper_class(self, scraper_type: ScraperType) -> Optional[Type[BaseApiScraper]]:
        """Get scraper implementation class by type."""
        return self._scraper_classes.get(scraper_type)
    
    def list_available_scrapers(self) -> Dict[str, str]:
        """List all available scrapers with their descriptions."""
        return {
            name: definition.description or f"{definition.scraper_type.value} scraper"
            for name, definition in self._scrapers.items()
        }
    
    def is_registered(self, name: str) -> bool:
        """Check if a scraper is registered."""
        return name in self._scrapers


class ApiScraperFactory:
    """
    Factory for creating API-based scrapers with proper configuration.
    
    This factory follows the existing pattern established in the codebase
    and provides dependency injection for all scraper components.
    """
    
    def __init__(self, registry: Optional[ApiScraperRegistry] = None):
        """
        Initialize factory with scraper registry.
        
        Args:
            registry: Optional custom registry, uses default if not provided
        """
        self.registry = registry or ApiScraperRegistry()
        self.logger = logging.getLogger(__name__)
        self._validator_cache: Dict[str, IResponseValidator] = {}
    
    def create_scraper(self, scraper_name: str, url: str,
                      scrape_job_id: Optional[str] = None,
                      custom_config: Optional[Dict[str, Any]] = None,
                      scraper_definition_db=None) -> BaseApiScraper:
        """
        Create a scraper instance with full configuration.
        
        Args:
            scraper_name: Name of the registered scraper
            url: Target URL for scraping
            scrape_job_id: Optional job ID for tracking
            custom_config: Optional configuration overrides
            scraper_definition_db: Database scraper definition
            
        Returns:
            Configured scraper instance
            
        Raises:
            ScrapingException: If scraper cannot be created
        """
        # Get scraper definition
        definition = self.registry.get_scraper_definition(scraper_name)
        if not definition:
            available = list(self.registry.list_available_scrapers().keys())
            raise ScrapingException(
                f"Scraper '{scraper_name}' not found. Available scrapers: {available}"
            )
        
        # Get scraper implementation class
        scraper_class = self.registry.get_scraper_class(definition.scraper_type)
        if not scraper_class:
            raise ScrapingException(
                f"No implementation class found for scraper type: {definition.scraper_type.value}"
            )
        
        # Apply custom configuration overrides
        api_config = self._apply_config_overrides(definition.api_config, custom_config)
        
        # Create validator
        validator = self._get_or_create_validator(
            definition.validator_type,
            definition.validation_level
        )
        
        try:
            # Create scraper instance with dependency injection
            scraper = scraper_class(
                url=url,
                scrape_job_id=scrape_job_id,
                config=api_config,
                scraper_definition=scraper_definition_db
            )
            
            # Inject validator if the scraper supports it
            if hasattr(scraper, 'set_validator'):
                scraper.set_validator(validator)
            
            self.logger.info(f"Created scraper instance: {scraper_name} for URL: {url}")
            return scraper
            
        except Exception as e:
            raise ScrapingException(f"Failed to create scraper '{scraper_name}': {str(e)}")
    
    def create_generic_scraper(self, scraper_type: ScraperType, url: str,
                             api_config: ApiScrapingConfig,
                             scrape_job_id: Optional[str] = None) -> BaseApiScraper:
        """
        Create a generic scraper without registration.
        
        Args:
            scraper_type: Type of scraper to create
            url: Target URL
            api_config: API configuration
            scrape_job_id: Optional job ID
            
        Returns:
            Configured generic scraper instance
        """
        # Get base scraper class
        if scraper_type in [ScraperType.GENERIC_REST, ScraperType.GENERIC_GRAPHQL]:
            from .generic_api_scraper import GenericApiScraper
            scraper_class = GenericApiScraper
        else:
            scraper_class = self.registry.get_scraper_class(scraper_type)
            if not scraper_class:
                raise ScrapingException(f"No implementation for scraper type: {scraper_type.value}")
        
        # Create appropriate validator
        validator_type = "graphql" if scraper_type == ScraperType.GENERIC_GRAPHQL else "rest"
        validator = ValidatorFactory.create_validator(validator_type)
        
        # Create scraper
        scraper = scraper_class(
            url=url,
            scrape_job_id=scrape_job_id,
            config=api_config
        )
        
        if hasattr(scraper, 'set_validator'):
            scraper.set_validator(validator)
        
        return scraper
    
    def _apply_config_overrides(self, base_config: ApiScrapingConfig,
                               custom_config: Optional[Dict[str, Any]]) -> ApiScrapingConfig:
        """Apply custom configuration overrides to base configuration."""
        if not custom_config:
            return base_config
        
        # Create a copy of the base config
        import copy
        config_copy = copy.deepcopy(base_config)
        
        # Apply overrides
        if 'rate_limit_delay' in custom_config:
            config_copy.rate_limit_delay = custom_config['rate_limit_delay']
        
        if 'proxy_required' in custom_config:
            config_copy.proxy_required = custom_config['proxy_required']
        
        if 'user_agent' in custom_config:
            config_copy.user_agent = custom_config['user_agent']
            if config_copy.request_config:
                config_copy.request_config.user_agent = custom_config['user_agent']
        
        if 'timeout' in custom_config and config_copy.request_config:
            config_copy.request_config.timeout = custom_config['timeout']
        
        if 'max_retries' in custom_config and config_copy.request_config:
            config_copy.request_config.max_retries = custom_config['max_retries']
        
        return config_copy
    
    def _get_or_create_validator(self, validator_type: str,
                               validation_level: ValidationLevel) -> IResponseValidator:
        """Get validator from cache or create new one."""
        cache_key = f"{validator_type}_{validation_level.value}"
        
        if cache_key not in self._validator_cache:
            self._validator_cache[cache_key] = ValidatorFactory.create_validator(
                validator_type, validation_level
            )
        
        return self._validator_cache[cache_key]
    
    def register_custom_scraper(self, definition: ScraperDefinition,
                              scraper_class: Type[BaseApiScraper]) -> None:
        """
        Register a custom scraper with the factory.
        
        Args:
            definition: Complete scraper definition
            scraper_class: Implementation class
        """
        self.registry.register_scraper(definition, scraper_class)
    
    def list_available_scrapers(self) -> Dict[str, str]:
        """List all available scrapers."""
        return self.registry.list_available_scrapers()
    
    def get_scraper_info(self, scraper_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a scraper.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            Dictionary with scraper information or None if not found
        """
        definition = self.registry.get_scraper_definition(scraper_name)
        if not definition:
            return None
        
        return {
            'name': definition.name,
            'type': definition.scraper_type.value,
            'base_url': definition.base_url,
            'description': definition.description,
            'proxy_required': definition.proxy_required,
            'rate_limit_delay': definition.rate_limit_delay,
            'validation_level': definition.validation_level.value,
            'validator_type': definition.validator_type,
            'endpoints': list(definition.api_config.endpoints.keys()),
            'strategy': definition.api_config.strategy.value
        }


# Global factory instance following the singleton pattern used in the codebase
_api_scraper_factory: Optional[ApiScraperFactory] = None


def get_api_scraper_factory() -> ApiScraperFactory:
    """
    Get the global API scraper factory instance.
    
    Returns:
        Global ApiScraperFactory instance
    """
    global _api_scraper_factory
    if _api_scraper_factory is None:
        _api_scraper_factory = ApiScraperFactory()
    return _api_scraper_factory


def create_api_scraper(scraper_name: str, url: str,
                      scrape_job_id: Optional[str] = None,
                      custom_config: Optional[Dict[str, Any]] = None) -> BaseApiScraper:
    """
    Convenience function to create an API scraper.
    
    Args:
        scraper_name: Name of the registered scraper
        url: Target URL for scraping
        scrape_job_id: Optional job ID for tracking
        custom_config: Optional configuration overrides
        
    Returns:
        Configured scraper instance
    """
    factory = get_api_scraper_factory()
    return factory.create_scraper(scraper_name, url, scrape_job_id, custom_config)