"""
Proxy service layer for managing proxy providers and assignments.

This module provides a high-level interface for proxy management,
handling provider selection, configuration loading, and scraper assignments.
"""

import os
import logging
import threading
from typing import Dict, Any, Optional, List
from django.core.cache import cache

from .base import BaseProxyProvider, ProxyCredentials, ProxyType
from .providers import WebshareProxyProvider, BrightDataProxyProvider, DatabaseProxyProvider


logger = logging.getLogger(__name__)


class ProxyService:
    """
    Central service for managing proxy providers and assignments.
    
    This service follows the Single Responsibility Principle by handling only
    proxy-related operations and delegating provider-specific logic to individual providers.
    """
    
    def __init__(self):
        self.providers: Dict[str, BaseProxyProvider] = {}
        self._providers_initialized = False
    
    def _initialize_providers(self) -> None:
        """Initialize available proxy providers based on configuration."""
        if self._providers_initialized:
            return
        
        # Skip all proxy provider initialization by default
        # Providers will only be initialized when explicitly requested
        logger.info("Proxy providers initialization skipped - will initialize on demand only")
        self._providers_initialized = True
    
    def _initialize_database_provider(self) -> None:
        """Lazily initialize the database provider when needed."""
        if 'database' not in self.providers:
            try:
                provider = DatabaseProxyProvider()
                if provider.validate_configuration():
                    self.providers['database'] = provider
                    logger.info("Initialized database proxy provider")
                else:
                    logger.warning("Skipped database proxy provider due to invalid configuration")
            except Exception as e:
                logger.error(f"Failed to initialize database proxy provider: {e}")
    
    def _initialize_environment_providers(self) -> None:
        """Initialize environment-based proxy providers when needed."""
        providers_to_init = [
            ('webshare', WebshareProxyProvider),
            ('bright_data', BrightDataProxyProvider),
        ]
        
        for provider_name, provider_class in providers_to_init:
            if provider_name not in self.providers:
                try:
                    provider = provider_class()
                    if provider.validate_configuration():
                        self.providers[provider_name] = provider
                        logger.info(f"Initialized {provider_name} proxy provider")
                    else:
                        logger.debug(f"Skipped {provider_name} proxy provider due to invalid configuration")
                except Exception as e:
                    logger.debug(f"Failed to initialize {provider_name} proxy provider: {e}")
    
    def get_proxy_for_scraper(self, scraper_name: str, proxy_type: Optional[ProxyType] = None) -> Optional[ProxyCredentials]:
        """
        Get proxy credentials for a specific scraper.
        
        Args:
            scraper_name: Name of the scraper requesting proxy
            proxy_type: Preferred proxy type, if None will use configuration
            
        Returns:
            ProxyCredentials if available, None otherwise
            
        Raises:
            Exception: If scraper requires proxy but none is available
        """
        # Ensure providers are initialized (but this now does nothing by default)
        if not self._providers_initialized:
            self._initialize_providers()
            
        # Check scraper definition for proxy requirements
        logger.debug(f"🔍 Getting proxy for scraper: {scraper_name}")
        scraper_config = self._get_scraper_config(scraper_name)
        logger.debug(f"🔍 Scraper config result: {scraper_config}")
        
        # Check if this scraper is configured to use proxy
        scraper_uses_proxy = scraper_config and scraper_config.get('use_proxy', True)
        logger.debug(f"🔍 scraper_uses_proxy calculation: scraper_config={scraper_config is not None}, use_proxy={scraper_config.get('use_proxy', True) if scraper_config else 'N/A'}, final={scraper_uses_proxy}")
        
        if not scraper_uses_proxy:
            logger.debug(f"Scraper {scraper_name} is configured to not use proxy")
            return None
        
        # Check if proxy is enabled globally
        if not self._is_proxy_enabled():
            if scraper_config and scraper_config.get('fail_without_proxy', False):
                raise Exception(f"Scraper {scraper_name} requires proxy but USE_PROXY is disabled")
            logger.debug("Proxy is disabled globally")
            return None
        
        # Determine required proxy type from scraper config
        if scraper_config:
            required_type = scraper_config.get('proxy_type_required')
            if required_type and required_type != 'none' and required_type != 'auto':
                try:
                    proxy_type = ProxyType(required_type)
                except ValueError:
                    logger.warning(f"Invalid proxy type in scraper config: {required_type}")
        
        # Try to get scraper-specific assignment from database first
        # Only initialize database provider if needed
        self._initialize_database_provider()
        
        assigned_proxy = self._get_scraper_assignment(scraper_name)
        if assigned_proxy:
            return assigned_proxy
        
        # Initialize environment providers only when actually needed
        self._initialize_environment_providers()
        
        # Fall back to environment-based configuration
        proxy = self._get_proxy_from_environment(proxy_type)
        
        # Check if scraper requires proxy but none is available
        if not proxy and scraper_config and scraper_config.get('use_proxy', True) and scraper_config.get('fail_without_proxy', False):
            required_type_str = proxy_type.value if proxy_type else 'any'
            error_msg = (
                f"Scraper {scraper_name} is configured to require proxy (fail_without_proxy=True) "
                f"but no {required_type_str} proxy is available. "
                f"Configure proxy assignments in Django admin or set fail_without_proxy=False to allow direct connections."
            )
            logger.error(error_msg)
            raise Exception(error_msg)
        
        return proxy
    
    def _is_proxy_enabled(self) -> bool:
        """Check if proxy usage is enabled via environment variable."""
        return os.getenv('USE_PROXY', 'false').lower() == 'true'
    
    def _get_scraper_assignment(self, scraper_name: str) -> Optional[ProxyCredentials]:
        """Get scraper-specific proxy assignment from database."""
        try:
            from ..models import ScraperProxyAssignment
            
            # Use cache to avoid repeated database queries
            cache_key = f"proxy_assignment_{scraper_name}"
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Use raw SQL consistently to avoid all async/sync Django ORM issues
            # This is the most reliable approach in any context (Celery workers, async views, etc.)
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT sp.assignment_id, sp.scraper_name, sp.is_active,
                           pc.config_id, pc.name, pc.host, pc.port, pc.username, 
                           pc.password, pc.proxy_type, pc.is_active
                    FROM scraper_proxy_assignment sp
                    JOIN proxy_configuration pc ON sp.proxy_configuration_id = pc.config_id
                    WHERE sp.scraper_name = %s AND sp.is_active = true
                    LIMIT 1
                """, [scraper_name])
                row = cursor.fetchone()
                
                if row:
                    # Extract data directly from row
                    assignment_id, scraper_name_db, assignment_active = row[:3]
                    config_id, name, host, port, username, password, proxy_type, config_active = row[3:]
                    
                    # Check if both assignment and config are active
                    if assignment_active and config_active:
                        credentials = ProxyCredentials(
                            host=host,
                            port=port,
                            username=username,
                            password=password,
                            proxy_type=ProxyType(proxy_type)
                        )
                        
                        # Cache for 5 minutes to reduce database load
                        cache.set(cache_key, credentials, 300)
                        
                        # Log usage for monitoring (pass config_id for proper logging)
                        self._log_proxy_usage(scraper_name, config_id)
                        
                        return credentials
            
            # Cache negative result for 1 minute
            cache.set(cache_key, None, 60)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get scraper proxy assignment for {scraper_name}: {e}")
            return None
    
    def _get_proxy_from_environment(self, proxy_type: Optional[ProxyType] = None) -> Optional[ProxyCredentials]:
        """Get proxy from environment configuration using available providers."""
        # Determine proxy type preference
        if proxy_type is None:
            proxy_type_str = os.getenv('DEFAULT_PROXY_TYPE', 'residential')
            try:
                proxy_type = ProxyType(proxy_type_str)
            except ValueError:
                logger.warning(f"Invalid default proxy type: {proxy_type_str}, using residential")
                proxy_type = ProxyType.RESIDENTIAL
        
        # Try providers in order of preference
        provider_preference = os.getenv('PROXY_PROVIDER_PREFERENCE', 'database,bright_data,webshare').split(',')
        
        for provider_name in provider_preference:
            provider_name = provider_name.strip()
            provider = self.providers.get(provider_name)
            if provider:
                credentials = provider.get_proxy_credentials(proxy_type)
                if credentials:
                    logger.debug(f"Using {provider_name} provider for {proxy_type.value} proxy")
                    return credentials
        
        logger.warning(f"No {proxy_type.value} proxy available from any provider")
        return None
    
    def _get_scraper_config(self, scraper_name: str) -> Optional[Dict[str, Any]]:
        """Get scraper configuration from database."""
        try:
            from ..models import ScraperDefinition
            
            # Use cache to avoid repeated database queries
            cache_key = f"scraper_config_{scraper_name}"
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"🔍 Cache HIT for {scraper_name}: {cached_result}")
                return cached_result
            
            logger.debug(f"🔍 Cache MISS for {scraper_name}, querying database")
            
            # Use raw SQL consistently to avoid all async/sync context issues
            # This approach works reliably in any context: sync, async, Celery workers, etc.
            logger.debug("Using raw SQL for database access to avoid async/sync context issues")
            
            try:
                logger.debug(f"🔍 Querying ScraperDefinition for name='{scraper_name}'")
                
                # Use raw SQL consistently to avoid all async/sync Django ORM issues
                # This is the most reliable approach in any context
                from django.db import connection
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT use_proxy, fail_without_proxy, "
                        "optimization_enabled, timeout_seconds, retry_attempts "
                        "FROM scraper_definition WHERE name = %s", 
                        [scraper_name]
                    )
                    row = cursor.fetchone()
                    if not row:
                        raise ScraperDefinition.DoesNotExist()
                    
                    # Extract data directly from row
                    use_proxy, fail_without_proxy, optimization_enabled, \
                    timeout_seconds, retry_attempts = row
                
                logger.debug(f"🔍 Found scraper: {scraper_name}, use_proxy={use_proxy}")
                
                config = {
                    'use_proxy': use_proxy,
                    'fail_without_proxy': fail_without_proxy,
                    'optimization_enabled': optimization_enabled,
                    'timeout_seconds': timeout_seconds,
                    'retry_attempts': retry_attempts,
                }
                
                logger.debug(f"🔍 Returning config for {scraper_name}: {config}")
                # Cache for 10 minutes
                cache.set(cache_key, config, 600)
                return config
                
            except ScraperDefinition.DoesNotExist:
                logger.warning(f"🔍 ScraperDefinition not found for '{scraper_name}'")
                # Cache negative result for 2 minutes
                cache.set(cache_key, None, 120)
                return None
                
        except Exception as e:
            logger.error(f"Failed to get scraper config for {scraper_name}: {e}")
            return None

    def _log_proxy_usage(self, scraper_name: str, config_id: int) -> None:
        """Log proxy usage for monitoring and analytics."""
        try:
            from django.utils import timezone
            from ..models import ProxyUsageLog, ProxyConfiguration
            
            # Get the actual ProxyConfiguration instance from database using config_id
            actual_proxy_config = ProxyConfiguration.objects.filter(
                config_id=config_id
            ).first()
            
            if actual_proxy_config:
                now = timezone.now()
                ProxyUsageLog.objects.create(
                    scraper_name=scraper_name,
                    proxy_configuration=actual_proxy_config,
                    target_url="https://example.com",  # Placeholder URL
                    target_domain="example.com",
                    was_successful=True,
                    started_at=now,
                    completed_at=now
                )
            else:
                logger.warning(f"Skipping proxy usage logging - ProxyConfiguration with config_id {config_id} not found")
        except Exception as e:
            logger.error(f"Failed to log proxy usage: {e}")
    
    def get_available_providers(self) -> List[str]:
        """Get list of available proxy provider names."""
        return list(self.providers.keys())
    
    def get_provider_info(self, provider_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific proxy provider."""
        provider = self.providers.get(provider_name)
        if not provider:
            return None
        
        return {
            'name': provider.provider_name,
            'available_types': [pt.value for pt in provider.get_available_proxy_types()],
            'is_configured': provider.validate_configuration()
        }
    
    def reload_configuration(self) -> None:
        """Reload proxy configuration from providers."""
        logger.info("Reloading proxy configuration")
        
        # Clear cache
        cache_keys = [key for key in cache._cache.keys() if key.startswith('proxy_assignment_')]
        if cache_keys:
            cache.delete_many(cache_keys)
        
        # Reinitialize providers
        self.providers.clear()
        self._initialize_providers()
    
    def validate_all_providers(self) -> Dict[str, bool]:
        """Validate configuration for all providers."""
        results = {}
        for provider_name, provider in self.providers.items():
            results[provider_name] = provider.validate_configuration()
        return results


# Lazy proxy service instance - only initialized when needed
_proxy_service_instance = None

def get_proxy_service() -> ProxyService:
    """Get the global proxy service instance, creating it if needed."""
    global _proxy_service_instance
    if _proxy_service_instance is None:
        _proxy_service_instance = ProxyService()
    return _proxy_service_instance