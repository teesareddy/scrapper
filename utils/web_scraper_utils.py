import asyncio
import time
from typing import Callable, Any, Dict, Optional

from ..proxy.service import get_proxy_service
from ..proxy.base import ProxyType
from ..core.request_client import HttpRequestClient, RequestConfig, ProxyConfig


class WebScraperUtils:
    @staticmethod
    def _should_use_proxy():
        """Check if proxy should be used via proxy service"""
        import os
        return os.getenv('USE_PROXY', 'false').lower() == 'true'

    @staticmethod
    async def _get_proxy_for_scraper(scraper_name: str = None, proxy_type: ProxyType = None):
        """Get proxy configuration using the new proxy service (async-safe)"""
        if not WebScraperUtils._should_use_proxy():
            return None
        
        # Use sync_to_async to safely call proxy service from async context
        from asgiref.sync import sync_to_async
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            get_proxy_sync = sync_to_async(
                lambda: get_proxy_service().get_proxy_for_scraper(scraper_name or "web_scraper_utils", proxy_type), 
                thread_sensitive=True
            )
            
            credentials = await get_proxy_sync()
            
            if credentials:
                # Return a simple dict since ProxyConfig isn't available
                return {
                    'host': credentials.host,
                    'port': credentials.port,
                    'username': credentials.username,
                    'password': credentials.password,
                    'protocol': "http"
                }
            
            return None
        except Exception as e:
            # Check if this is a proxy requirement failure - if so, re-raise
            if "fail_without_proxy=True" in str(e):
                logger.error(f"Proxy requirement failure: {e}")
                raise e
            else:
                # For other errors, log and return None
                logger.error(f"Failed to get proxy configuration: {e}")
                return None

    @staticmethod
    async def _get_default_proxy():
        """Get default proxy configuration - deprecated, use _get_proxy_for_scraper instead"""
        return await WebScraperUtils._get_proxy_for_scraper()

    @staticmethod
    async def scrape_with_playwright(url, capture_func, extract_func, proxy=None, scraper_name=None, proxy_type=None):
        import logging
        logger = logging.getLogger(__name__)
        
        playwright = None
        browser = None
        browser_manager = None

        try:
            # Check if we should use proxy at all
            if not WebScraperUtils._should_use_proxy():
                proxy = None
                logger.info("🚫 Proxy disabled - using direct connection")
            elif proxy is None:
                # Use new proxy service to get configuration
                proxy = await WebScraperUtils._get_proxy_for_scraper(scraper_name, proxy_type)
                if proxy:
                    host = proxy.get('host') if isinstance(proxy, dict) else proxy.host
                    port = proxy.get('port') if isinstance(proxy, dict) else proxy.port
                    logger.info(f"📡 Using proxy from service: {host}:{port}")
                else:
                    logger.info("🚫 No proxy configuration available from service")
            else:
                host = proxy.get('host') if isinstance(proxy, dict) else proxy.host
                port = proxy.get('port') if isinstance(proxy, dict) else proxy.port
                logger.info(f"📡 Using provided proxy: {host}:{port}")
            
            from scrapers.utils.browser_manager import BrowserManager
            browser_manager = BrowserManager()
            playwright, browser, context, page = await browser_manager.setup_page_with_proxy(proxy)

            # Take initial screenshot right after page creation if proxy is enabled
            if proxy:
                try:
                    import os
                    screenshot_dir = os.getenv('SCREENSHOT_DIR', '/tmp/screenshots')
                    os.makedirs(screenshot_dir, exist_ok=True)
                    screenshot_path = f"{screenshot_dir}/debug_initial_page_{int(time.time())}.png"
                    await page.screenshot(path=screenshot_path, full_page=True)
                    logger.info(f"📸 Debug initial page screenshot saved: {screenshot_path}")
                except Exception as e:
                    logger.warning(f"Failed to take initial page screenshot: {e}")

                # Handle proxy authentication if needed
                try:
                    # Wait a moment to see if proxy auth dialog appears
                    auth_wait_ms = int(os.getenv('PROXY_AUTH_WAIT_MS', '2000'))
                    await page.wait_for_timeout(auth_wait_ms)
                    
                    # Check for proxy authentication dialog or page
                    page_content = await page.content()
                    if "407" in page_content or "Proxy Authentication" in page_content or "authentication" in page_content.lower():
                        logger.info("🔐 Proxy authentication page detected")
                        
                        # Try to fill in proxy credentials if there are input fields
                        username_selectors = ['input[type="text"]', 'input[name="username"]', 'input[id="username"]']
                        password_selectors = ['input[type="password"]', 'input[name="password"]', 'input[id="password"]']
                        
                        for selector in username_selectors:
                            if await page.is_visible(selector):
                                username = proxy.get('username') if isinstance(proxy, dict) else proxy.username
                                await page.fill(selector, username)
                                logger.info("✅ Filled proxy username")
                                break
                        
                        for selector in password_selectors:
                            if await page.is_visible(selector):
                                password = proxy.get('password') if isinstance(proxy, dict) else proxy.password
                                await page.fill(selector, password)
                                logger.info("✅ Filled proxy password")
                                break
                        
                        # Try to submit the form
                        submit_selectors = ['input[type="submit"]', 'button[type="submit"]', 'button']
                        for selector in submit_selectors:
                            if await page.is_visible(selector):
                                await page.click(selector)
                                logger.info("✅ Submitted proxy auth form")
                                submit_wait_ms = int(os.getenv('PROXY_SUBMIT_WAIT_MS', '3000'))
                                await page.wait_for_timeout(submit_wait_ms)
                                break
                                
                        # Take screenshot after auth
                        screenshot_dir = os.getenv('SCREENSHOT_DIR', '/tmp/screenshots')
                        screenshot_path = f"{screenshot_dir}/debug_after_proxy_auth_{int(time.time())}.png"
                        await page.screenshot(path=screenshot_path, full_page=True)
                        logger.info(f"📸 Debug screenshot after proxy auth saved: {screenshot_path}")
                        
                except Exception as e:
                    logger.warning(f"Failed to handle proxy authentication: {e}")

            network_data = await capture_func(page, url)
            
            # Take debug screenshot after network capture if proxy is enabled
            if proxy:
                try:
                    import os
                    screenshot_dir = os.getenv('SCREENSHOT_DIR', '/tmp/screenshots')
                    os.makedirs(screenshot_dir, exist_ok=True)
                    screenshot_path = f"{screenshot_dir}/debug_after_network_{int(time.time())}.png"
                    await page.screenshot(path=screenshot_path, full_page=True)
                    logger.info(f"📸 Debug screenshot after network capture saved: {screenshot_path}")
                except Exception as e:
                    logger.warning(f"Failed to take screenshot: {e}")
            
            page_info = await extract_func(page)

            await browser_manager.close_page(page, context)
            return page_info, network_data

        except Exception:
            raise
        finally:
            if browser_manager:
                await browser_manager.close_browser(browser, playwright)


    @staticmethod
    async def wait_for_selector(page, selector, timeout=None, state="visible"):
        import os
        if timeout is None:
            timeout = int(os.getenv('SELECTOR_WAIT_TIMEOUT_MS', '30000'))
        try:
            return await page.wait_for_selector(selector, timeout=timeout, state=state)
        except Exception:
            return None

    @staticmethod
    async def safe_text_content(page, selector):
        try:
            if await page.is_visible(selector):
                text = await page.text_content(selector)
                return text.strip() if text else None
            return None
        except Exception:
            return None

    @staticmethod
    async def monitor_network(page, url_patterns, timeout=None):
        import os
        if timeout is None:
            timeout = float(os.getenv('NETWORK_MONITOR_TIMEOUT', '30.0'))
        results = {key: None for key in url_patterns.values()}
        data_captured = asyncio.Event()

        async def handle_response(response):
            response_url = response.url
            for pattern, key in url_patterns.items():
                if pattern in response_url:
                    if results[key] is None:
                        try:
                            results[key] = await response.json()
                            if all(results.values()):
                                data_captured.set()
                        except Exception:
                            pass

        page.on("response", handle_response)
        seen_urls = set()

        async def handle_request(request):
            request_url = request.url
            if request_url not in seen_urls:
                seen_urls.add(request_url)

        page.on("request", handle_request)

        try:
            await asyncio.wait_for(data_captured.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

        page.remove_listener("response", handle_response)
        page.remove_listener("request", handle_request)

        return results

    # API-based scraping methods
    @staticmethod
    async def scrape_with_api(scraper_name: str, url: str, 
                             scrape_job_id: Optional[str] = None,
                             custom_config: Optional[Dict[str, Any]] = None):
        """
        Scrape data using API-based scraper instead of browser automation.
        
        This method provides a high-level interface for API-based scraping
        that maintains consistency with the existing scrape_with_playwright method.
        
        Args:
            scraper_name: Name of the registered API scraper
            url: Target URL for scraping
            scrape_job_id: Optional job ID for tracking
            custom_config: Optional configuration overrides
            
        Returns:
            Dictionary containing extracted and processed data
            
        Raises:
            ScrapingException: If scraping fails
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"🚀 Starting API-based scraping with {scraper_name} for URL: {url}")
        
        try:
            # Get API scraper factory and create scraper
            factory = get_api_scraper_factory()
            scraper = factory.create_scraper(scraper_name, url, scrape_job_id, custom_config)
            
            # Extract data using API calls
            extracted_data = await scraper.extract_all_data()
            
            logger.info(f"✅ Successfully extracted data via API for {scraper_name}")
            return extracted_data
            
        except Exception as e:
            logger.error(f"❌ API scraping failed for {scraper_name}: {e}")
            raise
        finally:
            # Clean up resources
            if 'scraper' in locals():
                scraper.cleanup()
    
    @staticmethod
    async def make_api_request(url: str, method: str = "GET",
                              headers: Optional[Dict[str, str]] = None,
                              data: Optional[Dict[str, Any]] = None,
                              json_data: Optional[Dict[str, Any]] = None,
                              params: Optional[Dict[str, str]] = None,
                              scraper_name: str = None,
                              timeout: int = 30) -> Dict[str, Any]:
        """
        Make a single API request with proxy support and error handling.
        
        This method provides a low-level interface for making API requests
        with the same proxy integration as the browser-based scraping.
        
        Args:
            url: Target URL
            method: HTTP method (GET, POST, etc.)
            headers: Request headers
            data: Form data for POST requests
            json_data: JSON data for POST requests
            params: URL parameters for GET requests
            scraper_name: Scraper name for proxy configuration
            timeout: Request timeout in seconds
            
        Returns:
            Dictionary containing response data and metadata
            
        Raises:
            NetworkException: For network-related errors
            TimeoutException: For timeout errors
            ScrapingException: For other request errors
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # Get proxy configuration if needed
        proxy_config = None
        if WebScraperUtils._should_use_proxy():
            proxy_creds = await WebScraperUtils._get_proxy_for_scraper(scraper_name)
            if proxy_creds:
                proxy_config = ProxyConfig(
                    host=proxy_creds['host'],
                    port=proxy_creds['port'],
                    username=proxy_creds.get('username'),
                    password=proxy_creds.get('password'),
                    protocol=proxy_creds.get('protocol', 'http')
                )
        
        # Create HTTP client
        request_config = RequestConfig(timeout=timeout)
        client = HttpRequestClient(config=request_config, proxy_config=proxy_config)
        
        try:
            logger.info(f"📡 Making {method} request to {url}")
            
            # Make request based on method
            if method.upper() == "GET":
                result = await client.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                result = await client.post(url, headers=headers, data=data, json_data=json_data)
            else:
                from ..exceptions.scraping_exceptions import ScrapingException
                raise ScrapingException(f"HTTP method {method} not supported")
            
            # Return response data with metadata
            return {
                'data': result.data,
                'status_code': result.status_code,
                'headers': result.headers,
                'success': result.success,
                'elapsed_time': result.elapsed_time,
                'url': result.url
            }
            
        except Exception as e:
            logger.error(f"❌ API request failed: {e}")
            raise
        finally:
            client.close()
    
    @staticmethod
    async def validate_api_response(response_data: Dict[str, Any], 
                                   validator_type: str = "base",
                                   endpoint_name: str = "default") -> bool:
        """
        Validate API response using the centralized validation system.
        
        Args:
            response_data: Response data to validate
            validator_type: Type of validator to use (base, graphql, rest, etc.)
            endpoint_name: Name of the endpoint for logging
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            from ..core.response_validators import ValidatorFactory
            from ..core.request_client import RequestResult
            
            # Create a mock RequestResult for validation
            mock_result = RequestResult(
                status_code=200,
                data=response_data,
                headers={},
                url="",
                elapsed_time=0,
                attempt_count=1,
                success=True
            )
            
            # Get validator and validate
            validator = ValidatorFactory.create_validator(validator_type)
            validation_result = validator.validate(mock_result, endpoint_name)
            
            return validation_result.is_valid
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Response validation failed: {e}")
            return False
