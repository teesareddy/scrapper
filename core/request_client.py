"""
HTTP Request Client with robust error handling, retry logic, and connection pooling.

This module implements the Single Responsibility Principle by focusing solely on 
HTTP request management with enterprise-grade reliability features.
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, Optional, Union, List
from urllib.parse import urljoin, urlparse
import json

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from asgiref.sync import sync_to_async
import functools

from ..exceptions.scraping_exceptions import (
    NetworkException, TimeoutException, ScrapingException
)


class RequestMethod(Enum):
    """HTTP request methods supported by the client."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


@dataclass
class RequestConfig:
    """Configuration for HTTP requests with sensible defaults."""
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.3
    retry_status_codes: List[int] = field(default_factory=lambda: [500, 502, 503, 504, 429])
    connection_pool_size: int = 10
    max_pool_connections: int = 20
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    follow_redirects: bool = True
    verify_ssl: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.backoff_factor < 0:
            raise ValueError("Backoff factor cannot be negative")


@dataclass
class ProxyConfig:
    """Proxy configuration for requests."""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = "http"
    
    def to_requests_format(self) -> Dict[str, str]:
        """Convert to requests library proxy format."""
        if self.username and self.password:
            proxy_url = f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        else:
            proxy_url = f"{self.protocol}://{self.host}:{self.port}"
        
        return {
            'http': proxy_url,
            'https': proxy_url
        }


@dataclass
class RequestResult:
    """Result of an HTTP request with metadata."""
    status_code: int
    data: Dict[str, Any]
    headers: Dict[str, str]
    url: str
    elapsed_time: float
    attempt_count: int
    success: bool = True
    error_message: Optional[str] = None


class IHttpClient(ABC):
    """Abstract interface for HTTP clients following Interface Segregation Principle."""
    
    @abstractmethod
    async def get(self, url: str, headers: Optional[Dict[str, str]] = None, 
                  params: Optional[Dict[str, Any]] = None) -> RequestResult:
        """Perform GET request."""
        pass
    
    @abstractmethod
    async def post(self, url: str, data: Optional[Dict[str, Any]] = None,
                   headers: Optional[Dict[str, str]] = None,
                   json_data: Optional[Dict[str, Any]] = None) -> RequestResult:
        """Perform POST request."""
        pass


class HttpRequestClient(IHttpClient):
    """
    Production-ready HTTP client with enterprise features.
    
    Features:
    - Connection pooling and keep-alive
    - Exponential backoff with jitter
    - Circuit breaker pattern
    - Comprehensive error handling
    - Request/response logging
    - Proxy support integration
    """
    
    def __init__(self, config: Optional[RequestConfig] = None,
                 proxy_config: Optional[ProxyConfig] = None):
        """
        Initialize HTTP client with configuration.
        
        Args:
            config: Request configuration with defaults
            proxy_config: Optional proxy configuration
        """
        self.config = config or RequestConfig()
        self.proxy_config = proxy_config
        self.logger = logging.getLogger(__name__)
        self._session = None
        self._circuit_breaker_failures = 0
        self._circuit_breaker_last_failure = 0
        self._circuit_breaker_timeout = 60  # seconds
        
    def _create_session(self) -> requests.Session:
        """Create configured requests session with connection pooling."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            status_forcelist=self.config.retry_status_codes,
            backoff_factor=self.config.backoff_factor,
            raise_on_status=False
        )
        
        # Configure HTTP adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=self.config.connection_pool_size,
            pool_maxsize=self.config.max_pool_connections
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        # Configure proxy if provided
        if self.proxy_config:
            session.proxies.update(self.proxy_config.to_requests_format())
            self.logger.info(f"Configured proxy: {self.proxy_config.host}:{self.proxy_config.port}")
        
        return session
    
    def _get_session(self) -> requests.Session:
        """Get or create requests session (lazy initialization)."""
        if self._session is None:
            self._session = self._create_session()
        return self._session
    
    def _check_circuit_breaker(self) -> None:
        """Check circuit breaker state and raise exception if open."""
        current_time = time.time()
        
        # Reset circuit breaker after timeout
        if (current_time - self._circuit_breaker_last_failure) > self._circuit_breaker_timeout:
            self._circuit_breaker_failures = 0
        
        # Circuit breaker is open (too many failures)
        if self._circuit_breaker_failures >= 5:
            time_remaining = self._circuit_breaker_timeout - (current_time - self._circuit_breaker_last_failure)
            if time_remaining > 0:
                raise NetworkException(
                    f"Circuit breaker is open. Too many failures. "
                    f"Try again in {time_remaining:.1f} seconds."
                )
    
    def _record_failure(self) -> None:
        """Record a failure for circuit breaker tracking."""
        self._circuit_breaker_failures += 1
        self._circuit_breaker_last_failure = time.time()
    
    def _record_success(self) -> None:
        """Record a success for circuit breaker tracking."""
        self._circuit_breaker_failures = 0
    
    async def _make_request(self, method: RequestMethod, url: str,
                           headers: Optional[Dict[str, str]] = None,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           json_data: Optional[Dict[str, Any]] = None) -> RequestResult:
        """
        Make HTTP request with comprehensive error handling and monitoring.
        
        Args:
            method: HTTP method to use
            url: Target URL
            headers: Additional headers
            params: URL parameters
            data: Form data
            json_data: JSON payload
            
        Returns:
            RequestResult with response data and metadata
            
        Raises:
            NetworkException: For network-related errors
            TimeoutException: For timeout errors
            ScrapingException: For other request errors
        """
        self._check_circuit_breaker()
        
        start_time = time.time()
        attempt_count = 0
        last_exception = None
        
        # Prepare request arguments
        request_kwargs = {
            'timeout': self.config.timeout,
            'allow_redirects': self.config.follow_redirects,
            'verify': self.config.verify_ssl
        }
        
        if headers:
            request_kwargs['headers'] = headers
        if params:
            request_kwargs['params'] = params
        if data:
            request_kwargs['data'] = data
        if json_data:
            request_kwargs['json'] = json_data
        
        # Validate URL
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ScrapingException(f"Invalid URL format: {url}")
        
        self.logger.info(f"Making {method.value} request to {url}")
        
        while attempt_count <= self.config.max_retries:
            attempt_count += 1
            
            try:
                # Make async request using thread pool
                session = self._get_session()
                request_func = getattr(session, method.value.lower())
                
                response = await sync_to_async(functools.partial(
                    request_func, url, **request_kwargs
                ), thread_sensitive=True)()
                
                elapsed_time = time.time() - start_time
                
                # Check for HTTP errors
                if response.status_code >= 400:
                    error_msg = f"HTTP {response.status_code}: {response.reason}"
                    
                    # Retry on specific status codes
                    if response.status_code in self.config.retry_status_codes and attempt_count <= self.config.max_retries:
                        self.logger.warning(f"Retryable error on attempt {attempt_count}: {error_msg}")
                        await self._wait_with_backoff(attempt_count)
                        continue
                    
                    # Non-retryable error
                    self._record_failure()
                    return RequestResult(
                        status_code=response.status_code,
                        data={},
                        headers=dict(response.headers),
                        url=url,
                        elapsed_time=elapsed_time,
                        attempt_count=attempt_count,
                        success=False,
                        error_message=error_msg
                    )
                
                # Parse response data
                try:
                    if response.headers.get('content-type', '').startswith('application/json'):
                        response_data = response.json()
                    else:
                        response_data = {'text': response.text}
                except json.JSONDecodeError:
                    response_data = {'text': response.text}
                
                # Success
                self._record_success()
                self.logger.info(f"Request successful: {response.status_code} in {elapsed_time:.2f}s")
                
                return RequestResult(
                    status_code=response.status_code,
                    data=response_data,
                    headers=dict(response.headers),
                    url=url,
                    elapsed_time=elapsed_time,
                    attempt_count=attempt_count,
                    success=True
                )
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt_count <= self.config.max_retries:
                    self.logger.warning(f"Timeout on attempt {attempt_count}: {e}")
                    await self._wait_with_backoff(attempt_count)
                    continue
                break
                
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt_count <= self.config.max_retries:
                    self.logger.warning(f"Connection error on attempt {attempt_count}: {e}")
                    await self._wait_with_backoff(attempt_count)
                    continue
                break
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                self.logger.error(f"Request exception on attempt {attempt_count}: {e}")
                break
                
            except Exception as e:
                last_exception = e
                self.logger.error(f"Unexpected error on attempt {attempt_count}: {e}")
                break
        
        # All retries exhausted
        self._record_failure()
        elapsed_time = time.time() - start_time
        
        if isinstance(last_exception, requests.exceptions.Timeout):
            raise TimeoutException(f"Request timed out after {attempt_count} attempts: {last_exception}")
        elif isinstance(last_exception, requests.exceptions.ConnectionError):
            raise NetworkException(f"Connection failed after {attempt_count} attempts: {last_exception}")
        else:
            raise ScrapingException(f"Request failed after {attempt_count} attempts: {last_exception}")
    
    async def _wait_with_backoff(self, attempt: int) -> None:
        """Wait with exponential backoff and jitter."""
        import random
        
        base_delay = self.config.backoff_factor * (2 ** (attempt - 1))
        jitter = random.uniform(0, 0.1) * base_delay  # Add up to 10% jitter
        delay = base_delay + jitter
        
        self.logger.debug(f"Waiting {delay:.2f}s before retry {attempt}")
        await asyncio.sleep(delay)
    
    async def get(self, url: str, headers: Optional[Dict[str, str]] = None, 
                  params: Optional[Dict[str, Any]] = None) -> RequestResult:
        """Perform GET request."""
        return await self._make_request(RequestMethod.GET, url, headers=headers, params=params)
    
    async def post(self, url: str, data: Optional[Dict[str, Any]] = None,
                   headers: Optional[Dict[str, str]] = None,
                   json_data: Optional[Dict[str, Any]] = None) -> RequestResult:
        """Perform POST request."""
        return await self._make_request(RequestMethod.POST, url, headers=headers, 
                                      data=data, json_data=json_data)
    
    def close(self) -> None:
        """Close the session and clean up resources."""
        if self._session:
            self._session.close()
            self._session = None