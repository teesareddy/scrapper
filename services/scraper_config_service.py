"""
Scraper Configuration Service
Manages integration between database-stored configurations and scraper execution
"""

import logging
from typing import Dict, Any, Optional, List
from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone
from ..models import (
    ScraperDefinition, ProxyConfiguration, ScraperProxyAssignment,
    ScraperExecution, ProxyUsageLog
)

logger = logging.getLogger(__name__)


class ScraperConfigurationService:
    """Service for managing scraper configurations and execution"""

    @staticmethod
    def get_scraper_config(scraper_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a scraper from the database.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            Dictionary with scraper configuration or None if not found
        """
        try:
            scraper = ScraperDefinition.objects.get(name=scraper_name, is_enabled=True)
            
            # Get assigned proxy
            proxy_config = ScraperConfigurationService.get_assigned_proxy(scraper_name)
            
            config = {
                'scraper_id': scraper.scraper_id,
                'name': scraper.name,
                'display_name': scraper.display_name,
                'description': scraper.description,
                'target_website': scraper.target_website,
                'target_domains': scraper.target_domains,
                'status': scraper.status,
                'is_enabled': scraper.is_enabled,
                
                # Proxy settings
                'use_proxy': scraper.use_proxy,
                'fail_without_proxy': scraper.fail_without_proxy,
                'proxy_config': proxy_config,
                
                # Performance settings
                'optimization_enabled': scraper.optimization_enabled,
                'timeout_seconds': scraper.timeout_seconds,
                'retry_attempts': scraper.retry_attempts,
                'retry_delay_seconds': scraper.retry_delay_seconds,
                
                # Rate limiting
                'max_concurrent_jobs': scraper.max_concurrent_jobs,
                'delay_between_requests_ms': scraper.delay_between_requests_ms,
                
                # Browser configuration
                'browser_engine': scraper.browser_engine,
                'headless_mode': scraper.headless_mode,
                'user_agent': scraper.user_agent,
                'viewport_width': scraper.viewport_width,
                'viewport_height': scraper.viewport_height,
                
                # Debug and monitoring
                'enable_screenshots': scraper.enable_screenshots,
                'enable_detailed_logging': scraper.enable_detailed_logging,
                'log_level': scraper.log_level,
                
                # Scheduling
                'can_be_scheduled': scraper.can_be_scheduled,
                'schedule_interval_hours': scraper.schedule_interval_hours,
                
                # Custom settings
                'custom_settings': scraper.custom_settings,
            }
            
            return config
            
        except ScraperDefinition.DoesNotExist:
            logger.warning(f"Scraper configuration not found for: {scraper_name}")
            return None
        except Exception as e:
            logger.error(f"Failed to get scraper configuration for {scraper_name}: {e}")
            return None

    @staticmethod
    def get_assigned_proxy(scraper_name: str) -> Optional[Dict[str, Any]]:
        """
        Get assigned proxy configuration for a scraper.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            Dictionary with proxy configuration or None if no proxy assigned
        """
        try:
            # Get primary proxy assignment
            assignment = ScraperProxyAssignment.objects.filter(
                scraper_name=scraper_name,
                is_active=True,
                is_primary=True,
                proxy_configuration__is_active=True
            ).select_related('proxy_configuration__provider').first()
            
            if not assignment:
                # Try to get any active assignment
                assignment = ScraperProxyAssignment.objects.filter(
                    scraper_name=scraper_name,
                    is_active=True,
                    proxy_configuration__is_active=True
                ).select_related('proxy_configuration__provider').first()
            
            if assignment:
                proxy = assignment.proxy_configuration
                return {
                    'assignment_id': assignment.assignment_id,
                    'proxy_id': proxy.config_id,
                    'proxy_name': proxy.name,
                    'provider_name': proxy.provider.display_name,
                    'proxy_type': proxy.proxy_type,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'password': proxy.password,
                    'protocol': proxy.protocol,
                    'proxy_url': proxy.proxy_url,
                    'max_requests_per_hour': assignment.max_requests_per_hour,
                    'max_concurrent_requests': assignment.max_concurrent_requests,
                    'timeout_seconds': proxy.timeout_seconds,
                    'retry_attempts': proxy.retry_attempts,
                    'is_primary': assignment.is_primary,
                    'is_fallback': assignment.is_fallback
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get proxy configuration for {scraper_name}: {e}")
            return None

    @staticmethod
    def get_fallback_proxies(scraper_name: str) -> List[Dict[str, Any]]:
        """
        Get fallback proxy configurations for a scraper.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            List of proxy configurations ordered by fallback_order
        """
        try:
            assignments = ScraperProxyAssignment.objects.filter(
                scraper_name__startswith=scraper_name,  # Include fallback assignments
                is_active=True,
                is_fallback=True,
                proxy_configuration__is_active=True
            ).select_related('proxy_configuration__provider').order_by('fallback_order')
            
            fallback_proxies = []
            for assignment in assignments:
                proxy = assignment.proxy_configuration
                fallback_proxies.append({
                    'assignment_id': assignment.assignment_id,
                    'proxy_id': proxy.config_id,
                    'proxy_name': proxy.name,
                    'provider_name': proxy.provider.display_name,
                    'proxy_type': proxy.proxy_type,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'password': proxy.password,
                    'protocol': proxy.protocol,
                    'proxy_url': proxy.proxy_url,
                    'fallback_order': assignment.fallback_order,
                    'max_requests_per_hour': assignment.max_requests_per_hour,
                    'max_concurrent_requests': assignment.max_concurrent_requests,
                    'timeout_seconds': proxy.timeout_seconds,
                    'retry_attempts': proxy.retry_attempts
                })
            
            return fallback_proxies
            
        except Exception as e:
            logger.error(f"Failed to get fallback proxies for {scraper_name}: {e}")
            return []

    @staticmethod
    def record_scraper_execution(scraper_name: str, target_url: str, 
                               proxy_config: Optional[Dict[str, Any]] = None) -> Optional[int]:
        """
        Record the start of a scraper execution.
        
        Args:
            scraper_name: Name of the scraper
            target_url: URL being scraped
            proxy_config: Proxy configuration being used
            
        Returns:
            Execution ID or None if recording failed
        """
        try:
            scraper = ScraperDefinition.objects.get(name=scraper_name)
            
            # Get proxy configuration object if provided
            proxy_used = None
            if proxy_config and proxy_config.get('proxy_id'):
                try:
                    proxy_used = ProxyConfiguration.objects.get(config_id=proxy_config['proxy_id'])
                except ProxyConfiguration.DoesNotExist:
                    logger.warning(f"Proxy configuration not found: {proxy_config['proxy_id']}")
            
            execution = ScraperExecution.objects.create(
                scraper=scraper,
                status='running',
                target_url=target_url,
                proxy_used=proxy_used,
                started_at=timezone.now(),
                config_snapshot={
                    'scraper_config': ScraperConfigurationService.get_scraper_config(scraper_name),
                    'proxy_config': proxy_config
                }
            )
            
            return execution.execution_id
            
        except Exception as e:
            logger.error(f"Failed to record scraper execution for {scraper_name}: {e}")
            return None

    @staticmethod
    def update_execution_result(execution_id: int, success: bool, 
                              error_message: str = None, 
                              items_extracted: int = 0,
                              performance_metrics: Dict[str, Any] = None) -> bool:
        """
        Update scraper execution with results.
        
        Args:
            execution_id: Execution ID
            success: Whether execution was successful
            error_message: Error message if failed
            items_extracted: Number of items extracted
            performance_metrics: Performance metrics
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            execution = ScraperExecution.objects.get(execution_id=execution_id)
            
            execution.status = 'completed' if success else 'failed'
            execution.success = success
            execution.completed_at = timezone.now()
            execution.items_extracted = items_extracted
            
            if execution.started_at and execution.completed_at:
                duration = execution.completed_at - execution.started_at
                execution.duration_seconds = duration.total_seconds()
            
            if error_message:
                execution.error_message = error_message
            
            if performance_metrics:
                execution.response_time_ms = performance_metrics.get('response_time_ms')
                execution.memory_usage_mb = performance_metrics.get('memory_usage_mb')
                execution.cpu_usage_percent = performance_metrics.get('cpu_usage_percent')
                execution.data_quality_score = performance_metrics.get('data_quality_score')
            
            execution.save()
            
            # Update scraper statistics
            ScraperConfigurationService._update_scraper_statistics(execution.scraper, success)
            
            return True
            
        except ScraperExecution.DoesNotExist:
            logger.error(f"Execution not found: {execution_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to update execution result: {e}")
            return False

    @staticmethod
    def log_proxy_usage(scraper_name: str, proxy_config: Dict[str, Any], 
                       target_url: str, was_successful: bool,
                       response_time_ms: Optional[int] = None,
                       error_type: str = None, error_message: str = None) -> bool:
        """
        Log proxy usage for analytics.
        
        Args:
            scraper_name: Name of the scraper
            proxy_config: Proxy configuration used
            target_url: URL that was requested
            was_successful: Whether the request was successful
            response_time_ms: Response time in milliseconds
            error_type: Type of error if failed
            error_message: Error message if failed
            
        Returns:
            True if logging successful, False otherwise
        """
        try:
            if not proxy_config or not proxy_config.get('proxy_id'):
                return False
            
            proxy = ProxyConfiguration.objects.get(config_id=proxy_config['proxy_id'])
            
            # Parse domain from URL
            from urllib.parse import urlparse
            parsed_url = urlparse(target_url)
            target_domain = parsed_url.netloc
            
            ProxyUsageLog.objects.create(
                proxy_configuration=proxy,
                scraper_name=scraper_name,
                target_url=target_url,
                target_domain=target_domain,
                was_successful=was_successful,
                response_time_ms=response_time_ms,
                error_type=error_type,
                error_message=error_message,
                started_at=timezone.now(),
                completed_at=timezone.now()
            )
            
            # Update proxy statistics
            ScraperConfigurationService._update_proxy_statistics(proxy, was_successful)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to log proxy usage: {e}")
            return False

    @staticmethod
    def _update_scraper_statistics(scraper: ScraperDefinition, success: bool):
        """Update scraper statistics."""
        try:
            scraper.total_runs += 1
            if success:
                scraper.successful_runs += 1
                scraper.last_success_at = timezone.now()
            else:
                scraper.failed_runs += 1
            
            scraper.last_run_at = timezone.now()
            scraper.save()
            
        except Exception as e:
            logger.error(f"Failed to update scraper statistics: {e}")

    @staticmethod
    def _update_proxy_statistics(proxy: ProxyConfiguration, success: bool):
        """Update proxy statistics."""
        try:
            proxy.total_requests += 1
            if success:
                proxy.successful_requests += 1
                proxy.last_success = timezone.now()
                proxy.consecutive_failures = 0
            else:
                proxy.failed_requests += 1
                proxy.last_failure = timezone.now()
                proxy.consecutive_failures += 1
            
            # Calculate success rate
            proxy.calculate_success_rate()
            proxy.save()
            
        except Exception as e:
            logger.error(f"Failed to update proxy statistics: {e}")

    @staticmethod
    def is_scraper_enabled(scraper_name: str) -> bool:
        """
        Check if scraper is enabled. Works in both sync and async contexts.
        In async context, this should be awaited: await sync_to_async(is_scraper_enabled)(scraper_name)
        """
        try:
            return ScraperConfigurationService._sync_is_scraper_enabled(scraper_name)
        except Exception as e:
            logger.error(f"Failed to check scraper status: {e}")
            return False

    @staticmethod 
    def _sync_is_scraper_enabled(scraper_name: str) -> bool:
        """Synchronous version of scraper enabled check"""
        try:
            # Use raw SQL to avoid async/sync Django ORM issues
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT is_enabled, status FROM scraper_definition WHERE name = %s",
                    [scraper_name]
                )
                row = cursor.fetchone()
                if row:
                    is_enabled, status = row
                    return is_enabled and status in ['active', 'testing']
                return False
        except Exception as e:
            logger.error(f"Failed to check scraper status in sync method: {e}")
            return False

    @staticmethod
    def get_active_scrapers() -> List[Dict[str, Any]]:
        """
        Get all active scrapers with their configurations.
        
        Returns:
            List of scraper configurations
        """
        try:
            scrapers = ScraperDefinition.objects.filter(
                is_enabled=True,
                status__in=['active', 'testing']
            )
            
            active_scrapers = []
            for scraper in scrapers:
                config = ScraperConfigurationService.get_scraper_config(scraper.name)
                if config:
                    active_scrapers.append(config)
            
            return active_scrapers
            
        except Exception as e:
            logger.error(f"Failed to get active scrapers: {e}")
            return []