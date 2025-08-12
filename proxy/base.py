"""
Abstract base classes for proxy providers following SOLID principles.

This module defines the interface that all proxy providers must implement,
ensuring consistency and extensibility across different proxy services.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum


class ProxyType(Enum):
    """Types of proxy configurations available."""
    RESIDENTIAL = "residential"
    DATACENTER = "datacenter"


@dataclass
class ProxyCredentials:
    """Configuration for a specific proxy endpoint."""
    host: str
    port: int
    username: str
    password: str
    proxy_type: ProxyType
    
    def to_playwright_config(self) -> Dict[str, Any]:
        """Convert to Playwright proxy configuration format."""
        return {
            'server': f'http://{self.host}:{self.port}',
            'username': self.username,
            'password': self.password
        }
    
    def to_selenium_config(self) -> Dict[str, Any]:
        """Convert to Selenium proxy configuration format."""
        return {
            'http': f'http://{self.username}:{self.password}@{self.host}:{self.port}',
            'https': f'https://{self.username}:{self.password}@{self.host}:{self.port}'
        }


class BaseProxyProvider(ABC):
    """
    Abstract base class for proxy providers.
    
    All proxy providers must inherit from this class and implement the required methods.
    This ensures consistent interface across different providers (Webshare, Bright Data, etc.)
    """
    
    def __init__(self, provider_name: str):
        self.provider_name = provider_name
    
    @abstractmethod
    def get_proxy_credentials(self, proxy_type: ProxyType) -> Optional[ProxyCredentials]:
        """
        Get proxy credentials for the specified type.
        
        Args:
            proxy_type: The type of proxy needed (residential or datacenter)
            
        Returns:
            ProxyCredentials object if available, None otherwise
        """
        pass
    
    @abstractmethod
    def validate_configuration(self) -> bool:
        """
        Validate that the provider is properly configured.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def get_available_proxy_types(self) -> List[ProxyType]:
        """
        Get list of proxy types available from this provider.
        
        Returns:
            List of available ProxyType enums
        """
        pass
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(provider_name='{self.provider_name}')"