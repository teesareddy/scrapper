"""
Concrete proxy provider implementations.

This module contains specific implementations for different proxy services,
following the interface defined in base.py.
"""

import os
import logging
from typing import Dict, Any, Optional, List

from .base import BaseProxyProvider, ProxyCredentials, ProxyType


logger = logging.getLogger(__name__)


class WebshareProxyProvider(BaseProxyProvider):
    """
    Webshare proxy provider implementation.
    
    Configuration is loaded from environment variables:
    - WEBSHARE_RESIDENTIAL_HOST: Host for residential proxies
    - WEBSHARE_RESIDENTIAL_PORT: Port for residential proxies
    - WEBSHARE_RESIDENTIAL_USERNAME: Username for residential proxies
    - WEBSHARE_RESIDENTIAL_PASSWORD: Password for residential proxies
    - WEBSHARE_DATACENTER_HOST: Host for datacenter proxies
    - WEBSHARE_DATACENTER_PORT: Port for datacenter proxies
    - WEBSHARE_DATACENTER_USERNAME: Username for datacenter proxies
    - WEBSHARE_DATACENTER_PASSWORD: Password for datacenter proxies
    """
    
    def __init__(self):
        super().__init__("webshare")
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load configuration from environment variables."""
        self.config = {
            ProxyType.RESIDENTIAL: {
                'host': os.getenv('WEBSHARE_RESIDENTIAL_HOST'),
                'port': self._get_int_env('WEBSHARE_RESIDENTIAL_PORT'),
                'username': os.getenv('WEBSHARE_RESIDENTIAL_USERNAME'),
                'password': os.getenv('WEBSHARE_RESIDENTIAL_PASSWORD'),
            },
            ProxyType.DATACENTER: {
                'host': os.getenv('WEBSHARE_DATACENTER_HOST'),
                'port': self._get_int_env('WEBSHARE_DATACENTER_PORT'),
                'username': os.getenv('WEBSHARE_DATACENTER_USERNAME'),
                'password': os.getenv('WEBSHARE_DATACENTER_PASSWORD'),
            }
        }
    
    def _get_int_env(self, key: str) -> Optional[int]:
        """Safely convert environment variable to integer."""
        value = os.getenv(key)
        if value:
            try:
                return int(value)
            except ValueError:
                logger.warning(f"Invalid integer value for {key}: {value}")
        return None
    
    def get_proxy_credentials(self, proxy_type: ProxyType) -> Optional[ProxyCredentials]:
        """Get Webshare proxy credentials for the specified type."""
        config = self.config.get(proxy_type)
        if not config:
            return None
        
        # Check if all required fields are present
        required_fields = ['host', 'port', 'username', 'password']
        if not all(config.get(field) for field in required_fields):
            logger.warning(f"Incomplete {proxy_type.value} proxy configuration for {self.provider_name}")
            return None
        
        return ProxyCredentials(
            host=config['host'],
            port=config['port'],
            username=config['username'],
            password=config['password'],
            proxy_type=proxy_type
        )
    
    def validate_configuration(self) -> bool:
        """Validate Webshare configuration."""
        # Check if at least one proxy type is configured
        for proxy_type in ProxyType:
            if self.get_proxy_credentials(proxy_type):
                return True
        
        logger.error("No valid proxy configuration found for Webshare provider")
        return False
    
    def get_available_proxy_types(self) -> List[ProxyType]:
        """Get available proxy types for Webshare."""
        available_types = []
        for proxy_type in ProxyType:
            if self.get_proxy_credentials(proxy_type):
                available_types.append(proxy_type)
        return available_types


class BrightDataProxyProvider(BaseProxyProvider):
    """
    Bright Data proxy provider implementation.
    
    Configuration is loaded from environment variables:
    - BRIGHT_DATA_RESIDENTIAL_HOST: Host for residential proxies
    - BRIGHT_DATA_RESIDENTIAL_PORT: Port for residential proxies
    - BRIGHT_DATA_RESIDENTIAL_USERNAME: Username for residential proxies
    - BRIGHT_DATA_RESIDENTIAL_PASSWORD: Password for residential proxies
    - BRIGHT_DATA_DATACENTER_HOST: Host for datacenter proxies
    - BRIGHT_DATA_DATACENTER_PORT: Port for datacenter proxies
    - BRIGHT_DATA_DATACENTER_USERNAME: Username for datacenter proxies
    - BRIGHT_DATA_DATACENTER_PASSWORD: Password for datacenter proxies
    """
    
    def __init__(self):
        super().__init__("bright_data")
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load configuration from environment variables."""
        self.config = {
            ProxyType.RESIDENTIAL: {
                'host': os.getenv('BRIGHT_DATA_RESIDENTIAL_HOST'),
                'port': self._get_int_env('BRIGHT_DATA_RESIDENTIAL_PORT'),
                'username': os.getenv('BRIGHT_DATA_RESIDENTIAL_USERNAME'),
                'password': os.getenv('BRIGHT_DATA_RESIDENTIAL_PASSWORD'),
            },
            ProxyType.DATACENTER: {
                'host': os.getenv('BRIGHT_DATA_DATACENTER_HOST'),
                'port': self._get_int_env('BRIGHT_DATA_DATACENTER_PORT'),
                'username': os.getenv('BRIGHT_DATA_DATACENTER_USERNAME'),
                'password': os.getenv('BRIGHT_DATA_DATACENTER_PASSWORD'),
            }
        }
    
    def _get_int_env(self, key: str) -> Optional[int]:
        """Safely convert environment variable to integer."""
        value = os.getenv(key)
        if value:
            try:
                return int(value)
            except ValueError:
                logger.warning(f"Invalid integer value for {key}: {value}")
        return None
    
    def get_proxy_credentials(self, proxy_type: ProxyType) -> Optional[ProxyCredentials]:
        """Get Bright Data proxy credentials for the specified type."""
        config = self.config.get(proxy_type)
        if not config:
            return None
        
        # Check if all required fields are present
        required_fields = ['host', 'port', 'username', 'password']
        if not all(config.get(field) for field in required_fields):
            logger.warning(f"Incomplete {proxy_type.value} proxy configuration for {self.provider_name}")
            return None
        
        return ProxyCredentials(
            host=config['host'],
            port=config['port'],
            username=config['username'],
            password=config['password'],
            proxy_type=proxy_type
        )
    
    def validate_configuration(self) -> bool:
        """Validate Bright Data configuration."""
        # Check if at least one proxy type is configured
        for proxy_type in ProxyType:
            if self.get_proxy_credentials(proxy_type):
                return True
        
        logger.error("No valid proxy configuration found for Bright Data provider")
        return False
    
    def get_available_proxy_types(self) -> List[ProxyType]:
        """Get available proxy types for Bright Data."""
        available_types = []
        for proxy_type in ProxyType:
            if self.get_proxy_credentials(proxy_type):
                available_types.append(proxy_type)
        return available_types


class DatabaseProxyProvider(BaseProxyProvider):
    """
    Database-backed proxy provider implementation.
    
    This provider loads proxy configurations from the Django database models,
    allowing for dynamic configuration without code changes.
    """
    
    def __init__(self):
        super().__init__("database")
    
    def get_proxy_credentials(self, proxy_type: ProxyType) -> Optional[ProxyCredentials]:
        """Get proxy credentials from database for the specified type."""
        try:
            # Use raw SQL to avoid all async/sync Django ORM issues
            # This is the most reliable approach in any context
            from django.db import connection
            
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT host, port, username, password, proxy_type, is_active
                    FROM proxy_configuration
                    WHERE proxy_type = %s AND is_active = true
                    LIMIT 1
                """, [proxy_type.value])
                row = cursor.fetchone()
                
                if not row:
                    logger.warning(f"No active {proxy_type.value} proxy configuration found in database")
                    return None
                
                host, port, username, password, db_proxy_type, is_active = row
                
                return ProxyCredentials(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    proxy_type=proxy_type
                )
            
        except Exception as e:
            logger.error(f"Failed to load proxy configuration from database: {e}")
            return None
    
    def validate_configuration(self) -> bool:
        """Validate database proxy configuration."""
        # During initialization, assume configuration is valid
        # The actual validation will happen when the provider is used
        # This avoids database access during app initialization
        try:
            # Check if the table exists without accessing data
            from django.db import connection
            
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'proxy_configuration'
                    )
                """)
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    logger.warning("Proxy configuration table does not exist")
                    return False
                
                return True
            
        except Exception as e:
            logger.error(f"Failed to validate database proxy configuration: {e}")
            return False
    
    def get_available_proxy_types(self) -> List[ProxyType]:
        """Get available proxy types from database."""
        try:
            # Use raw SQL to avoid async/sync Django ORM issues
            from django.db import connection
            
            with connection.cursor() as cursor:
                cursor.execute("SELECT DISTINCT proxy_type FROM proxy_configuration WHERE is_active = true")
                proxy_types_in_db = [row[0] for row in cursor.fetchall()]
            
            available_types = []
            for proxy_type_str in proxy_types_in_db:
                try:
                    proxy_type = ProxyType(proxy_type_str)
                    available_types.append(proxy_type)
                except ValueError:
                    logger.warning(f"Unknown proxy type in database: {proxy_type_str}")
            
            return available_types
            
        except Exception as e:
            logger.error(f"Failed to get available proxy types from database: {e}")
            return []