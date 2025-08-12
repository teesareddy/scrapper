"""
Database Integrated Scraper
Enhanced base scraper that automatically uses database configuration
"""

import logging
from typing import Dict, Any, Optional, Union
from ..base import BaseScraper
from .scraper_config_service import ScraperConfigurationService

try:
    from ..utils.performance_optimizer import OptimizationLevel
except ImportError:
    OptimizationLevel = None

logger = logging.getLogger(__name__)


class DatabaseIntegratedScraper(BaseScraper):
    """
    Enhanced base scraper that automatically loads configuration from database
    and integrates with proxy management system.
    """

    def __init__(self, url: Optional[str] = None, scrape_job_id: Optional[str] = None):
        """
        Initialize scraper with database configuration.
        
        Args:
            url: URL to scrape (optional, can be set later)
            scrape_job_id: Scrape job ID for tracking
        """
        # Get configuration from database
        self.db_config = ScraperConfigurationService.get_scraper_config(self.name)
        
        if not self.db_config:
            logger.warning(f"No database configuration found for {self.name}. Using defaults.")
            # Initialize with defaults
            super().__init__(url, scrape_job_id, False, "balanced")
            return
        
        # Extract configuration values
        enable_optimization = self.db_config.get('optimization_enabled', True)
        optimization_level = self.db_config.get('optimization_level', 'balanced')
        
        # Convert string optimization level to enum if available
        if isinstance(optimization_level, str) and OptimizationLevel:
            try:
                optimization_level = OptimizationLevel(optimization_level)
            except ValueError:
                optimization_level = OptimizationLevel.BALANCED if OptimizationLevel else "balanced"
        
        # Initialize parent with database configuration
        super().__init__(url, scrape_job_id, enable_optimization, optimization_level)
        
        # Set up proxy configuration
        self.proxy_config = self.db_config.get('proxy_config')
        self.fallback_proxies = ScraperConfigurationService.get_fallback_proxies(self.name)
        
        # Store execution ID for tracking
        self.execution_id = None
        
        # Apply database configuration
        self._apply_database_config()

    def _apply_database_config(self):
        """Apply database configuration to scraper instance."""
        if not self.db_config:
            return
        
        try:
            # Apply timeout settings
            self.timeout_seconds = self.db_config.get('timeout_seconds', 60)
            self.retry_attempts = self.db_config.get('retry_attempts', 3)
            self.retry_delay_seconds = self.db_config.get('retry_delay_seconds', 5)
            
            # Apply rate limiting
            self.delay_between_requests_ms = self.db_config.get('delay_between_requests_ms', 1000)
            
            # Apply browser settings
            self.headless_mode = self.db_config.get('headless_mode', True)
            self.viewport_width = self.db_config.get('viewport_width', 1920)
            self.viewport_height = self.db_config.get('viewport_height', 1080)
            self.user_agent = self.db_config.get('user_agent', '')
            
            # Apply debug settings
            self.enable_screenshots = self.db_config.get('enable_screenshots', False)
            self.enable_detailed_logging = self.db_config.get('enable_detailed_logging', False)
            self.log_level = self.db_config.get('log_level', 'INFO')
            
            # Set logger level
            log_level_map = {
                'DEBUG': logging.DEBUG,
                'INFO': logging.INFO,
                'WARNING': logging.WARNING,
                'ERROR': logging.ERROR
            }
            self.logger.setLevel(log_level_map.get(self.log_level, logging.INFO))
            
            logger.info(f"Applied database configuration for {self.name}")
            
        except Exception as e:
            logger.error(f"Failed to apply database configuration for {self.name}: {e}")

    def get_proxy_configuration(self) -> Optional[Dict[str, Any]]:
        """
        Get the current proxy configuration for this scraper.
        
        Returns:
            Proxy configuration dictionary or None
        """
        return self.proxy_config

    def get_fallback_proxy_configurations(self) -> list:
        """
        Get fallback proxy configurations for this scraper.
        
        Returns:
            List of fallback proxy configurations
        """
        return self.fallback_proxies

    def switch_to_fallback_proxy(self, fallback_index: int = 0) -> bool:
        """
        Switch to a fallback proxy configuration.
        
        Args:
            fallback_index: Index of fallback proxy to use
            
        Returns:
            True if switch successful, False otherwise
        """
        try:
            if fallback_index < len(self.fallback_proxies):
                old_proxy = self.proxy_config.get('proxy_name', 'Unknown') if self.proxy_config else 'None'
                self.proxy_config = self.fallback_proxies[fallback_index]
                new_proxy = self.proxy_config.get('proxy_name', 'Unknown')
                
                logger.info(f"Switched from {old_proxy} to fallback proxy {new_proxy}")
                return True
            else:
                logger.warning(f"No fallback proxy available at index {fallback_index}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to switch to fallback proxy: {e}")
            return False

    async def scrape(self) -> 'ScrapingResult':
        """Enhanced scrape method with database integration."""
        
        # Check if scraper is enabled (use sync_to_async for async context)
        try:
            from asgiref.sync import sync_to_async
            is_enabled = await sync_to_async(ScraperConfigurationService.is_scraper_enabled)(self.name)
        except ImportError:
            # Fallback to sync call if asgiref not available
            is_enabled = ScraperConfigurationService.is_scraper_enabled(self.name)
        
        if not is_enabled:
            from ..core.result_structures import ResultBuilder, ErrorCategory
            return (ResultBuilder(self.name, self.url or "", self.scrape_job_id)
                   .with_error(ErrorCategory.VALIDATION, f"Scraper {self.name} is disabled", fatal=True)
                   .build())
        
        # Record execution start
        self.execution_id = ScraperConfigurationService.record_scraper_execution(
            self.name, self.url, self.proxy_config
        )
        
        try:
            # Call parent scrape method
            result = await super().scrape()
            
            # Update execution result
            if self.execution_id:
                performance_metrics = {
                    'response_time_ms': getattr(result, 'response_time_ms', None),
                    'memory_usage_mb': getattr(result, 'memory_usage_mb', None),
                    'cpu_usage_percent': getattr(result, 'cpu_usage_percent', None),
                    'data_quality_score': getattr(result, 'data_quality_score', None)
                }
                
                ScraperConfigurationService.update_execution_result(
                    self.execution_id,
                    result.success,
                    '; '.join([e.message for e in result.errors]) if result.errors else None,
                    getattr(result, 'items_extracted', 0),
                    performance_metrics
                )
            
            # Log proxy usage
            if self.proxy_config:
                ScraperConfigurationService.log_proxy_usage(
                    self.name,
                    self.proxy_config,
                    self.url,
                    result.success,
                    getattr(result, 'response_time_ms', None),
                    result.errors[0].category.value if result.errors else None,
                    result.errors[0].message if result.errors else None
                )
            
            return result
            
        except Exception as e:
            # Update execution with error
            if self.execution_id:
                ScraperConfigurationService.update_execution_result(
                    self.execution_id,
                    False,
                    str(e),
                    0
                )
            
            # Log proxy usage failure
            if self.proxy_config:
                ScraperConfigurationService.log_proxy_usage(
                    self.name,
                    self.proxy_config,
                    self.url,
                    False,
                    error_type='exception',
                    error_message=str(e)
                )
            
            raise

    def should_use_proxy(self) -> bool:
        """
        Check if this scraper should use proxy based on database configuration.
        
        Returns:
            True if proxy should be used, False otherwise
        """
        if not self.db_config:
            return super()._should_use_proxy()
        
        use_proxy = self.db_config.get('use_proxy', True)
        fail_without_proxy = self.db_config.get('fail_without_proxy', True)
        has_proxy_config = self.proxy_config is not None
        
        if use_proxy and fail_without_proxy and not has_proxy_config:
            logger.error(f"Scraper {self.name} requires proxy but no proxy assigned")
            return False
        
        return use_proxy and has_proxy_config

    def get_browser_config(self) -> Dict[str, Any]:
        """
        Get browser configuration from database settings.
        
        Returns:
            Browser configuration dictionary
        """
        if not self.db_config:
            return {
                'headless': True,
                'viewport': {'width': 1920, 'height': 1080},
                'user_agent': ''
            }
        
        return {
            'headless': self.db_config.get('headless_mode', True),
            'viewport': {
                'width': self.db_config.get('viewport_width', 1920),
                'height': self.db_config.get('viewport_height', 1080)
            },
            'user_agent': self.db_config.get('user_agent', ''),
            'timeout_seconds': self.db_config.get('timeout_seconds', 60),
            'enable_screenshots': self.db_config.get('enable_screenshots', False)
        }

    def get_custom_settings(self) -> Dict[str, Any]:
        """
        Get custom settings specific to this scraper.
        
        Returns:
            Custom settings dictionary
        """
        if not self.db_config:
            return {}
        
        return self.db_config.get('custom_settings', {})

    def is_feature_enabled(self, feature_name: str) -> bool:
        """
        Check if a specific feature is enabled for this scraper.
        
        Args:
            feature_name: Name of the feature to check
            
        Returns:
            True if feature is enabled, False otherwise
        """
        custom_settings = self.get_custom_settings()
        return custom_settings.get(feature_name, False)

    def get_required_fields(self) -> list:
        """
        Get required fields from custom settings.
        
        Returns:
            List of required field names
        """
        custom_settings = self.get_custom_settings()
        return custom_settings.get('required_fields', [])

    def __str__(self):
        """String representation of the scraper."""
        status = "enabled" if self.db_config and self.db_config.get('is_enabled') else "disabled"
        proxy_status = "with proxy" if self.proxy_config else "no proxy"
        return f"{self.name} ({status}, {proxy_status})"

    def __repr__(self):
        """Detailed representation of the scraper."""
        return (f"DatabaseIntegratedScraper(name='{self.name}', "
                f"enabled={self.db_config.get('is_enabled') if self.db_config else False}, "
                f"proxy={bool(self.proxy_config)}, "
                f"url='{self.url}')")