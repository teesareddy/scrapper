import asyncio
from typing import Optional, Dict, Any, List
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, WebDriverException
import logging
from contextlib import asynccontextmanager
from abc import ABC, abstractmethod

from ..exceptions.scraping_exceptions import NetworkException, TimeoutException as ScrapingTimeoutException


class BrowserConfig:
    def __init__(self, 
                 headless: bool = True,
                 viewport_width: int = 1920,
                 viewport_height: int = 1080,
                 user_agent: str = None,
                 timeout: int = 30,
                 extra_args: List[str] = None,
                 proxy: Optional[Dict[str, str]] = None):
        self.headless = headless
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self.user_agent = user_agent or self._default_user_agent()
        self.timeout = timeout
        self.extra_args = extra_args or []
        self.proxy = proxy
    
    def _default_user_agent(self) -> str:
        import os
        return os.getenv('BROWSER_USER_AGENT', 
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


class BaseBrowserManager(ABC):
    def __init__(self, config: BrowserConfig = None):
        self.config = config or BrowserConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def create_browser(self):
        pass
    
    @abstractmethod
    async def close_browser(self):
        pass
    
    @abstractmethod
    async def get_page(self, url: str):
        pass


class PlaywrightManager(BaseBrowserManager):
    def __init__(self, config: BrowserConfig = None, browser_type: str = "chromium"):
        super().__init__(config)
        self.browser_type = browser_type
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self._pages: List[Page] = []
    
    async def create_browser(self):
        if self.browser:
            return self.browser
        
        try:
            self.playwright = await async_playwright().start()
            
            import os
            default_args = [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
                "--no-default-browser-check",
                "--no-first-run"
            ]
            
            # Allow browser args to be configured via environment
            env_args = os.getenv('BROWSER_ARGS', '').split(',') if os.getenv('BROWSER_ARGS') else []
            browser_args = default_args + [arg.strip() for arg in env_args if arg.strip()] + self.config.extra_args
            
            if self.browser_type == "chromium":
                browser_launcher = self.playwright.chromium
            elif self.browser_type == "firefox":
                browser_launcher = self.playwright.firefox
            elif self.browser_type == "webkit":
                browser_launcher = self.playwright.webkit
            else:
                raise ValueError(f"Unsupported browser type: {self.browser_type}")
            
            launch_options = {
                "headless": self.config.headless,
                "args": browser_args
            }
            
            if self.config.proxy:
                launch_options["proxy"] = self.config.proxy
            
            self.browser = await browser_launcher.launch(**launch_options)
            
            context_options = {
                "viewport": {
                    "width": self.config.viewport_width,
                    "height": self.config.viewport_height
                },
                "user_agent": self.config.user_agent
            }
            
            self.context = await self.browser.new_context(**context_options)
            
            await self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
            """)
            
            self.logger.info(f"Playwright {self.browser_type} browser created successfully")
            return self.browser
            
        except Exception as e:
            self.logger.error(f"Failed to create Playwright browser: {e}")
            raise NetworkException(f"Browser creation failed: {e}")
    
    async def verify_browser_ip(self, page: Page) -> str:
        """Verify the IP being used by the browser"""
        try:
            self.logger.info("🔍 Verifying browser IP...")
            import os
            ip_check_url = os.getenv('IP_CHECK_URL', 'http://httpbin.org/ip')
            timeout_ms = int(os.getenv('IP_CHECK_TIMEOUT_MS', '10000'))
            await page.goto(ip_check_url, wait_until="domcontentloaded", timeout=timeout_ms)
            
            # Get the IP from the page content
            content = await page.content()
            
            # Extract IP from JSON response
            import json
            try:
                # Find JSON content in the page
                pre_element = await page.query_selector("pre")
                if pre_element:
                    json_text = await pre_element.inner_text()
                    ip_data = json.loads(json_text)
                    current_ip = ip_data.get('origin', 'Unknown')
                    
                    self.logger.info(f"🌐 BROWSER IP: {current_ip}")
                    
                    import os
                    expected_proxy_ip = os.getenv('EXPECTED_PROXY_IP')
                    if expected_proxy_ip and expected_proxy_ip in current_ip:
                        self.logger.info("✅ CONFIRMED: Browser using expected proxy IP!")
                    else:
                        self.logger.info("ℹ️ Browser using different IP (direct or different proxy)")
                    
                    return current_ip
                else:
                    self.logger.warning("⚠️ Could not find IP element on page")
                    return "Unknown"
                    
            except (json.JSONDecodeError, Exception) as e:
                self.logger.warning(f"⚠️ Could not parse IP response: {e}")
                return "Unknown"
                
        except Exception as e:
            self.logger.warning(f"⚠️ Could not verify browser IP: {e}")
            return "Unknown"

    async def get_page(self, url: str = None) -> Page:
        if not self.context:
            await self.create_browser()
        
        try:
            page = await self.context.new_page()
            self._pages.append(page)
            
            page.set_default_timeout(self.config.timeout * 1000)
            page.set_default_navigation_timeout(self.config.timeout * 1000)
            
            # Verify IP if proxy is configured and this is the first page
            if self.config.proxy and len(self._pages) == 1:
                await self.verify_browser_ip(page)
            
            if url:
                await page.goto(url, wait_until="domcontentloaded")
            
            return page
            
        except Exception as e:
            self.logger.error(f"Failed to create/navigate page: {e}")
            raise NetworkException(f"Page creation/navigation failed: {e}")
    
    async def close_browser(self):
        try:
            for page in self._pages:
                if not page.is_closed():
                    await page.close()
            self._pages.clear()
            
            if self.context:
                await self.context.close()
                self.context = None
            
            if self.browser:
                await self.browser.close()
                self.browser = None
            
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
                
            self.logger.info("Playwright browser closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing Playwright browser: {e}")
    
    async def create_stealth_page(self, url: str = None) -> Page:
        page = await self.get_page()
        
        await page.add_init_script("""
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
            
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
        """)
        
        if url:
            await page.goto(url, wait_until="domcontentloaded")
        
        return page


class SeleniumManager(BaseBrowserManager):
    def __init__(self, config: BrowserConfig = None, browser_type: str = "chrome"):
        super().__init__(config)
        self.browser_type = browser_type
        self.driver = None
    
    async def create_browser(self):
        if self.driver:
            return self.driver
        
        try:
            if self.browser_type.lower() == "chrome":
                options = ChromeOptions()
                if self.config.headless:
                    options.add_argument("--headless")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument(f"--user-agent={self.config.user_agent}")
                options.add_argument(f"--window-size={self.config.viewport_width},{self.config.viewport_height}")
                
                for arg in self.config.extra_args:
                    options.add_argument(arg)
                
                if self.config.proxy:
                    proxy_str = f"{self.config.proxy.get('server', '')}"
                    options.add_argument(f"--proxy-server={proxy_str}")
                
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                
                self.driver = webdriver.Chrome(options=options)
                
            elif self.browser_type.lower() == "firefox":
                options = FirefoxOptions()
                if self.config.headless:
                    options.add_argument("--headless")
                
                profile = webdriver.FirefoxProfile()
                profile.set_preference("general.useragent.override", self.config.user_agent)
                
                if self.config.proxy:
                    proxy_host = self.config.proxy.get('server', '').split(':')[0]
                    proxy_port = int(self.config.proxy.get('server', '').split(':')[1])
                    profile.set_preference("network.proxy.type", 1)
                    profile.set_preference("network.proxy.http", proxy_host)
                    profile.set_preference("network.proxy.http_port", proxy_port)
                    profile.set_preference("network.proxy.ssl", proxy_host)
                    profile.set_preference("network.proxy.ssl_port", proxy_port)
                
                self.driver = webdriver.Firefox(options=options, firefox_profile=profile)
            
            else:
                raise ValueError(f"Unsupported browser type: {self.browser_type}")
            
            self.driver.set_page_load_timeout(self.config.timeout)
            import os
            implicit_wait = int(os.getenv('SELENIUM_IMPLICIT_WAIT', '10'))
            self.driver.implicitly_wait(implicit_wait)
            
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info(f"Selenium {self.browser_type} browser created successfully")
            return self.driver
            
        except Exception as e:
            self.logger.error(f"Failed to create Selenium browser: {e}")
            raise NetworkException(f"Browser creation failed: {e}")
    
    async def get_page(self, url: str = None):
        if not self.driver:
            await self.create_browser()
        
        if url:
            try:
                self.driver.get(url)
            except TimeoutException:
                raise ScrapingTimeoutException(f"Page load timeout for URL: {url}")
            except WebDriverException as e:
                raise NetworkException(f"Failed to load page: {e}")
        
        return self.driver
    
    async def close_browser(self):
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                self.logger.info("Selenium browser closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing Selenium browser: {e}")
    
    def wait_for_element(self, locator: tuple, timeout: int = None) -> bool:
        timeout = timeout or self.config.timeout
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located(locator)
            )
            return True
        except TimeoutException:
            return False
    
    def safe_click(self, element):
        try:
            actions = ActionChains(self.driver)
            actions.move_to_element(element).click().perform()
        except Exception as e:
            self.logger.warning(f"Failed to click element: {e}")
            try:
                self.driver.execute_script("arguments[0].click();", element)
            except Exception as e2:
                self.logger.error(f"Failed to click element with JS: {e2}")
                raise


class BrowserManagerFactory:
    @staticmethod
    def create_playwright_manager(config: BrowserConfig = None, 
                                 browser_type: str = "chromium") -> PlaywrightManager:
        return PlaywrightManager(config, browser_type)
    
    @staticmethod
    def create_selenium_manager(config: BrowserConfig = None, 
                               browser_type: str = "chrome") -> SeleniumManager:
        return SeleniumManager(config, browser_type)
    
    @staticmethod
    def create_from_config(engine: str = "playwright", 
                          browser_type: str = None,
                          config: BrowserConfig = None) -> BaseBrowserManager:
        if engine.lower() == "playwright":
            browser_type = browser_type or "chromium"
            return BrowserManagerFactory.create_playwright_manager(config, browser_type)
        elif engine.lower() == "selenium":
            browser_type = browser_type or "chrome"
            return BrowserManagerFactory.create_selenium_manager(config, browser_type)
        else:
            raise ValueError(f"Unsupported browser engine: {engine}")


class BrowserManager:
    """Simple browser manager compatible with web_scraper_utils"""
    
    def __init__(self):
        self.manager = None
    
    async def setup_page_with_proxy(self, proxy=None):
        """Setup a page with proxy configuration - compatible with web_scraper_utils"""
        from ..exceptions.scraping_exceptions import NetworkException
        
        try:
            # Create browser config
            proxy_config = None
            if proxy:
                if isinstance(proxy, dict):
                    proxy_config = {
                        "server": f"http://{proxy['host']}:{proxy['port']}",
                        "username": proxy.get('username'),
                        "password": proxy.get('password')
                    }
                else:
                    proxy_config = {
                        "server": f"http://{proxy.host}:{proxy.port}",
                        "username": getattr(proxy, 'username', None),
                        "password": getattr(proxy, 'password', None)
                    }
            
            browser_config = BrowserConfig(
                headless=True,
                proxy=proxy_config,
                viewport_width=1920,
                viewport_height=1080
            )
            
            # Create playwright manager
            self.manager = BrowserManagerFactory.create_playwright_manager(browser_config)
            
            # Setup browser (this also creates the context)
            browser = await self.manager.create_browser()
            page = await self.manager.get_page()
            
            # Return playwright, browser, context, page as expected
            return self.manager.playwright, browser, self.manager.context, page
            
        except Exception as e:
            raise NetworkException(f"Failed to setup browser with proxy: {e}")
    
    async def close_page(self, page, context):
        """Close page and context"""
        try:
            if page and not page.is_closed():
                await page.close()
            if context:
                await context.close()
        except Exception as e:
            pass
    
    async def close_browser(self, browser, playwright):
        """Close browser and playwright"""
        try:
            if self.manager:
                await self.manager.close()
            else:
                if browser:
                    await browser.close()
                if playwright:
                    await playwright.stop()
        except Exception as e:
            pass


@asynccontextmanager
async def managed_browser(manager: BaseBrowserManager):
    try:
        await manager.create_browser()
        yield manager
    finally:
        await manager.close_browser()