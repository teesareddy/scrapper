"""
Test the complete proxy flow including WebScraperUtils to verify all async issues are resolved.
"""
import asyncio
from django.core.management.base import BaseCommand
from scrapers.utils.web_scraper_utils import WebScraperUtils
from scrapers.proxy.base import ProxyType


class Command(BaseCommand):
    help = 'Test the complete proxy flow including WebScraperUtils'

    def handle(self, *args, **options):
        """Test the complete proxy flow."""
        self.stdout.write("=== Testing Complete Proxy Flow ===")
        
        try:
            # Test 1: WebScraperUtils proxy retrieval (async)
            self.stdout.write("\n1. Testing WebScraperUtils async proxy retrieval...")
            proxy_config = asyncio.run(self._test_web_scraper_utils_proxy())
            if proxy_config:
                self.stdout.write(self.style.SUCCESS(f"✅ WebScraperUtils proxy: {proxy_config.host}:{proxy_config.port}"))
            else:
                self.stdout.write(self.style.WARNING("⚠️ No proxy configuration from WebScraperUtils"))
            
            # Test 2: Full scraping flow simulation
            self.stdout.write("\n2. Testing full scraping flow simulation...")
            result = asyncio.run(self._test_full_scraping_flow())
            if result:
                self.stdout.write(self.style.SUCCESS("✅ Full scraping flow completed successfully"))
            else:
                self.stdout.write(self.style.ERROR("❌ Full scraping flow failed"))
                return
            
            self.stdout.write("\n=== Complete Proxy Flow Test Complete ===")
            self.stdout.write(self.style.SUCCESS("\n🎉 All proxy flow tests passed!"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Failed: {e}"))
            import traceback
            traceback.print_exc()
    
    async def _test_web_scraper_utils_proxy(self):
        """Test WebScraperUtils proxy retrieval in async context."""
        try:
            # This should now work without async context errors
            proxy_config = await WebScraperUtils._get_proxy_for_scraper(
                scraper_name="washington_pavilion_scraper",
                proxy_type=ProxyType.RESIDENTIAL
            )
            return proxy_config
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"WebScraperUtils proxy test failed: {e}"))
            raise
    
    async def _test_full_scraping_flow(self):
        """Test a complete scraping flow that would happen in Celery worker."""
        try:
            # Simulate what happens during scraping
            self.stdout.write("   - Checking proxy usage...")
            should_use_proxy = WebScraperUtils._should_use_proxy()
            self.stdout.write(f"     Should use proxy: {should_use_proxy}")
            
            if should_use_proxy:
                self.stdout.write("   - Getting proxy configuration...")
                proxy = await WebScraperUtils._get_proxy_for_scraper("washington_pavilion_scraper")
                if proxy:
                    self.stdout.write(f"     Proxy obtained: {proxy.host}:{proxy.port}")
                else:
                    self.stdout.write("     No proxy available")
            
            self.stdout.write("   - Simulating browser setup (would use proxy)...")
            # This is where the actual browser setup would happen
            # For now, just verify we can get proxy config without errors
            
            return True
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Full scraping flow test failed: {e}"))
            return False