"""
Type-safe configuration classes for API-based scraping.

This module provides strongly-typed configuration classes that ensure
proper API configuration with validation and sensible defaults.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Union
from urllib.parse import urljoin

from .api_scraper_base import ApiEndpoint, ApiScrapingConfig, ApiScrapingStrategy
from .request_client import RequestConfig
from .response_validators import ValidationLevel


@dataclass
class GraphQLEndpointConfig(ApiEndpoint):
    """Configuration for GraphQL endpoints with query support."""
    query: Optional[str] = None
    variables: Optional[Dict[str, Any]] = None
    operation_name: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        # GraphQL endpoints are typically POST
        if self.method == "GET":
            self.method = "POST"
        
        # Set GraphQL-specific headers
        if not self.headers:
            self.headers = {}
        
        self.headers.setdefault('Content-Type', 'application/json')
        self.headers.setdefault('Accept', 'application/json')
    
    def create_request_payload(self, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create GraphQL request payload."""
        payload = {}
        
        if self.query:
            payload['query'] = self.query
        
        if self.operation_name:
            payload['operationName'] = self.operation_name
        
        # Merge provided variables with endpoint variables
        merged_variables = {}
        if self.variables:
            merged_variables.update(self.variables)
        if variables:
            merged_variables.update(variables)
        
        if merged_variables:
            payload['variables'] = merged_variables
        
        return payload


@dataclass
class RESTEndpointConfig(ApiEndpoint):
    """Configuration for REST endpoints with parameter support."""
    path_params: Optional[List[str]] = None
    query_params: Optional[Dict[str, Any]] = None
    body_template: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        super().__post_init__()
        
        if self.path_params is None:
            self.path_params = []
        
        if self.query_params is None:
            self.query_params = {}
        
        # Set REST-specific headers
        if not self.headers:
            self.headers = {}
        
        self.headers.setdefault('Accept', 'application/json')
        if self.method in ['POST', 'PUT', 'PATCH']:
            self.headers.setdefault('Content-Type', 'application/json')
    
    def build_url(self, base_url: str, path_values: Optional[Dict[str, str]] = None) -> str:
        """Build complete URL with path parameters."""
        url = self.url
        
        # Replace path parameters
        if path_values and self.path_params:
            for param in self.path_params:
                if param in path_values:
                    placeholder = f"{{{param}}}"
                    url = url.replace(placeholder, str(path_values[param]))
        
        # Join with base URL if needed
        if not url.startswith('http'):
            url = urljoin(base_url.rstrip('/') + '/', url.lstrip('/'))
        
        return url


class IApiConfigurationBuilder(ABC):
    """Interface for building API configurations."""
    
    @abstractmethod
    def build_config(self) -> ApiScrapingConfig:
        """Build the complete API scraping configuration."""
        pass
    
    @abstractmethod
    def add_endpoint(self, name: str, endpoint: ApiEndpoint) -> 'IApiConfigurationBuilder':
        """Add an endpoint to the configuration."""
        pass


class BaseApiConfigurationBuilder(IApiConfigurationBuilder):
    """Base builder for API configurations."""
    
    def __init__(self, strategy: ApiScrapingStrategy):
        self.strategy = strategy
        self.endpoints: Dict[str, ApiEndpoint] = {}
        self.request_config = RequestConfig()
        self.proxy_required = False
        self.rate_limit_delay = 0.0
        self.user_agent = None
    
    def add_endpoint(self, name: str, endpoint: ApiEndpoint) -> 'BaseApiConfigurationBuilder':
        """Add an endpoint to the configuration."""
        self.endpoints[name] = endpoint
        return self
    
    def set_request_config(self, config: RequestConfig) -> 'BaseApiConfigurationBuilder':
        """Set request configuration."""
        self.request_config = config
        return self
    
    def set_proxy_required(self, required: bool) -> 'BaseApiConfigurationBuilder':
        """Set proxy requirement."""
        self.proxy_required = required
        return self
    
    def set_rate_limit(self, delay: float) -> 'BaseApiConfigurationBuilder':
        """Set rate limiting delay."""
        self.rate_limit_delay = delay
        return self
    
    def set_user_agent(self, user_agent: str) -> 'BaseApiConfigurationBuilder':
        """Set custom user agent."""
        self.user_agent = user_agent
        return self
    
    def build_config(self) -> ApiScrapingConfig:
        """Build the complete API scraping configuration."""
        return ApiScrapingConfig(
            strategy=self.strategy,
            endpoints=self.endpoints,
            request_config=self.request_config,
            proxy_required=self.proxy_required,
            rate_limit_delay=self.rate_limit_delay,
            user_agent=self.user_agent
        )


class BroadwaySFConfigurationBuilder(BaseApiConfigurationBuilder):
    """Configuration builder specifically for Broadway SF APIs."""
    
    def __init__(self, domain: str = "broadwaysf.com"):
        super().__init__(ApiScrapingStrategy.GRAPHQL)
        self.domain = domain
        self._setup_broadway_sf_defaults()
    
    def _get_source_id_mapping(self, domain: str) -> str:
        """
        Get source ID for different domains.
        
        Args:
            domain: Base domain (e.g., 'broadwaysf.com')
            
        Returns:
            Source ID for the domain
        """
        source_id_mapping = {
            'broadwaysf.com': 'AV_US_WEST',
            'broadwayindetroit.com': 'AV_US_EAST',
            'saengernola.com': 'AV_US_CENTRAL',
            'kingstheatre.com': 'AV_US_EAST'
        }
        return source_id_mapping.get(domain, 'AV_US_WEST')  # Default to Broadway SF
    
    def _setup_broadway_sf_defaults(self) -> None:
        """Setup Broadway SF specific defaults."""
        # Configure request settings for Broadway SF
        self.request_config = RequestConfig(
            timeout=30,
            max_retries=3,
            backoff_factor=0.3,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        # Broadway SF may require proxy depending on configuration
        self.proxy_required = False
        
        # Add Broadway SF specific endpoints
        self._add_calendar_service_endpoint()
        self._add_bolt_api_endpoint(self.domain)
    
    def _add_calendar_service_endpoint(self) -> None:
        """Add Broadway SF calendar service GraphQL endpoint."""
        calendar_endpoint = GraphQLEndpointConfig(
            url="https://calendar-service.core.platform.atgtickets.com/",
            method="POST",
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            query="""
            query getShow($promoCode: String, $titleSlug: String, $venueSlug: String, $venueId: String, $combined: Boolean, $ruleSetting: RuleSetting, $sourceId: String, $ruleKey: String) {
              getShow(
                promoCode: $promoCode
                titleSlug: $titleSlug
                venueSlug: $venueSlug
                venueId: $venueId
                combined: $combined
                ruleSetting: $ruleSetting
                sourceId: $sourceId
                ruleKey: $ruleKey
              ) {
                show {
                  images {
                    vertical
                    horizontal
                  }
                  isEmbargoed
                  status
                  images {
                    vertical
                    horizontal
                  }
                  performanceMode
                  externalPurchaseLink
                  dates {
                    nextPerformanceDate
                    lastPerformanceDate
                    timeZone
                  }
                  performances {
                    id
                    dates {
                      performanceDate
                      onSaleDate
                      memberOnSaleDate
                    }
                    performanceTimeDescription
                    availabilityStatus
                    isAccessiblePerformance
                    accessibilityType
                    purchaseFlowType
                    ruleKey
                    promoCode {
                      ids
                      promoType
                      promoLocked
                      accessCodes
                    }
                    price {
                      minPrice
                      minPriceFee
                    }
                    sectionAvailability {
                      sectionName
                      priceZoneId
                      totalFaceValue
                      contiguousSeats
                      promoCodeIds
                    }
                  }
                }
                promoCode {
                  ids
                  promoType
                  accessCodes
                  singleUseCode
                }
                status {
                  code
                  message
                }
                singleUseError {
                  status
                  body {
                    error
                  }
                }
                isIsolatedCalendarRule
              }
            }
            """,
            operation_name="getShow",
            variables={
                "combined": False,
                "ruleSetting": {},
                "sourceId": self._get_source_id_mapping(self.domain)
            },
            timeout=30,
            response_type="json"
        )
        
        self.add_endpoint("calendar_service", calendar_endpoint)
    
    def _add_bolt_api_endpoint(self, domain: str = "broadwaysf.com") -> None:
        """Add Broadway SF Bolt API REST endpoint with dynamic domain support."""
        bolt_endpoint = RESTEndpointConfig(
            url=f"https://boltapi.{domain}/admissions/{{title_slug}}/{{venue_slug}}/{{performance_id}}",
            method="GET",
            headers={
                'Accept': 'application/json, text/plain, */*',
                'Referer': f'https://www.{domain}/'
            },
            path_params=["title_slug", "venue_slug", "performance_id"],
            timeout=30,
            response_type="json"
        )
        
        self.add_endpoint("bolt_api", bolt_endpoint)


class GenericGraphQLConfigurationBuilder(BaseApiConfigurationBuilder):
    """Configuration builder for generic GraphQL APIs."""
    
    def __init__(self, base_url: str):
        super().__init__(ApiScrapingStrategy.GRAPHQL)
        self.base_url = base_url
        self._setup_graphql_defaults()
    
    def _setup_graphql_defaults(self) -> None:
        """Setup GraphQL specific defaults."""
        self.request_config = RequestConfig(
            timeout=30,
            max_retries=2,
            user_agent="Mozilla/5.0 (compatible; API-Scraper/1.0)"
        )
    
    def add_graphql_endpoint(self, name: str, query: str,
                           operation_name: Optional[str] = None,
                           variables: Optional[Dict[str, Any]] = None) -> 'GenericGraphQLConfigurationBuilder':
        """Add a GraphQL endpoint with query."""
        endpoint = GraphQLEndpointConfig(
            url=self.base_url,
            query=query,
            operation_name=operation_name,
            variables=variables or {}
        )
        
        self.add_endpoint(name, endpoint)
        return self


class GenericRESTConfigurationBuilder(BaseApiConfigurationBuilder):
    """Configuration builder for generic REST APIs."""
    
    def __init__(self, base_url: str):
        super().__init__(ApiScrapingStrategy.REST_JSON)
        self.base_url = base_url
        self._setup_rest_defaults()
    
    def _setup_rest_defaults(self) -> None:
        """Setup REST specific defaults."""
        self.request_config = RequestConfig(
            timeout=30,
            max_retries=2,
            user_agent="Mozilla/5.0 (compatible; API-Scraper/1.0)"
        )
    
    def add_rest_endpoint(self, name: str, path: str, method: str = "GET",
                         path_params: Optional[List[str]] = None,
                         query_params: Optional[Dict[str, Any]] = None) -> 'GenericRESTConfigurationBuilder':
        """Add a REST endpoint."""
        endpoint = RESTEndpointConfig(
            url=f"{self.base_url.rstrip('/')}/{path.lstrip('/')}",
            method=method,
            path_params=path_params or [],
            query_params=query_params or {}
        )
        
        self.add_endpoint(name, endpoint)
        return self


class ConfigurationFactory:
    """Factory for creating pre-configured API configurations."""
    
    @staticmethod
    def create_broadway_sf_config(domain: str = "broadwaysf.com") -> ApiScrapingConfig:
        """Create Broadway SF API configuration with optional custom domain."""
        builder = BroadwaySFConfigurationBuilder(domain)
        return builder.build_config()
    
    @staticmethod
    def create_generic_graphql_config(base_url: str) -> GenericGraphQLConfigurationBuilder:
        """Create generic GraphQL configuration builder."""
        return GenericGraphQLConfigurationBuilder(base_url)
    
    @staticmethod
    def create_generic_rest_config(base_url: str) -> GenericRESTConfigurationBuilder:
        """Create generic REST configuration builder."""
        return GenericRESTConfigurationBuilder(base_url)
    
    @staticmethod
    def from_environment(scraper_name: str) -> Optional[ApiScrapingConfig]:
        """
        Create configuration from environment variables.
        
        Environment variables should follow the pattern:
        SCRAPER_{SCRAPER_NAME}_{SETTING}
        
        Example:
        SCRAPER_BROADWAY_SF_TIMEOUT=45
        SCRAPER_BROADWAY_SF_PROXY_REQUIRED=true
        """
        env_prefix = f"SCRAPER_{scraper_name.upper()}_"
        
        # Check if any environment variables exist for this scraper
        env_vars = {k: v for k, v in os.environ.items() if k.startswith(env_prefix)}
        
        if not env_vars:
            return None
        
        # Create base configuration
        if scraper_name.lower() == "broadway_sf":
            builder = BroadwaySFConfigurationBuilder()
        else:
            # Generic configuration
            base_url = env_vars.get(f"{env_prefix}BASE_URL", "")
            if not base_url:
                return None
            
            strategy = env_vars.get(f"{env_prefix}STRATEGY", "rest_json")
            if strategy == "graphql":
                builder = GenericGraphQLConfigurationBuilder(base_url)
            else:
                builder = GenericRESTConfigurationBuilder(base_url)
        
        # Apply environment overrides
        if f"{env_prefix}TIMEOUT" in env_vars:
            timeout = int(env_vars[f"{env_prefix}TIMEOUT"])
            builder.request_config.timeout = timeout
        
        if f"{env_prefix}MAX_RETRIES" in env_vars:
            max_retries = int(env_vars[f"{env_prefix}MAX_RETRIES"])
            builder.request_config.max_retries = max_retries
        
        if f"{env_prefix}PROXY_REQUIRED" in env_vars:
            proxy_required = env_vars[f"{env_prefix}PROXY_REQUIRED"].lower() == "true"
            builder.set_proxy_required(proxy_required)
        
        if f"{env_prefix}RATE_LIMIT_DELAY" in env_vars:
            rate_limit = float(env_vars[f"{env_prefix}RATE_LIMIT_DELAY"])
            builder.set_rate_limit(rate_limit)
        
        if f"{env_prefix}USER_AGENT" in env_vars:
            user_agent = env_vars[f"{env_prefix}USER_AGENT"]
            builder.set_user_agent(user_agent)
        
        return builder.build_config()


# Convenience functions for common configurations
def get_broadway_sf_config(domain: str = "broadwaysf.com") -> ApiScrapingConfig:
    """Get Broadway SF API configuration with environment overrides and optional custom domain."""
    env_config = ConfigurationFactory.from_environment("broadway_sf")
    return env_config or ConfigurationFactory.create_broadway_sf_config(domain)


def get_graphql_config(base_url: str) -> GenericGraphQLConfigurationBuilder:
    """Get GraphQL configuration builder."""
    return ConfigurationFactory.create_generic_graphql_config(base_url)


def get_rest_config(base_url: str) -> GenericRESTConfigurationBuilder:
    """Get REST configuration builder."""
    return ConfigurationFactory.create_generic_rest_config(base_url)