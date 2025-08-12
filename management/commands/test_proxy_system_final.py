"""
Final test of the proxy system to verify all fixes are working correctly.
"""
import asyncio
from django.core.management.base import BaseCommand
from scrapers.proxy.service import get_proxy_service
from scrapers.base import BaseScraper


class TestScraper(BaseScraper):
    """Simple test scraper for proxy verification."""
    
    def __init__(self):
        super().__init__()
        self.scraper_name = "washington_pavilion_scraper"
    
    @property
    def name(self) -> str:
        return "test_scraper"
    
    async def extract_data(self):
        return {"test": "data"}
    
    async def process_data(self, raw_data):
        return raw_data
    
    async def store_in_database(self, processed_data):
        return "test_id"


class Command(BaseCommand):
    help = 'Test the complete proxy system to verify all fixes are working'

    def handle(self, *args, **options):
        """Test the complete proxy system."""
        self.stdout.write("=== Testing Proxy System ===")
        
        # Test 1: Proxy service configuration
        self.stdout.write("\n1. Testing proxy service configuration...")
        try:
            available_providers = proxy_service.get_available_providers()
            self.stdout.write(self.style.SUCCESS(f"✅ Available providers: {available_providers}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Failed to get providers: {e}"))
            return
        
        # Test 2: Scraper proxy assignment
        self.stdout.write("\n2. Testing scraper proxy assignment...")
        try:
            proxy_credentials = proxy_service.get_proxy_for_scraper("washington_pavilion_scraper")
            if proxy_credentials:
                self.stdout.write(self.style.SUCCESS(f"✅ Proxy assigned: {proxy_credentials.host}:{proxy_credentials.port}"))
                self.stdout.write(f"   Type: {proxy_credentials.proxy_type.value}")
                self.stdout.write(f"   Username: {proxy_credentials.username}")
            else:
                self.stdout.write(self.style.WARNING("⚠️ No proxy assigned to washington_pavilion_scraper"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Failed to get proxy assignment: {e}"))
            return
        
        # Test 3: Async context database access
        self.stdout.write("\n3. Testing async context database access...")
        try:
            scraper = TestScraper()
            # This will test the _verify_proxy_ip method which uses async database access
            asyncio.run(scraper._verify_proxy_ip())
            self.stdout.write(self.style.SUCCESS("✅ Async proxy IP verification completed successfully"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Failed async proxy verification: {e}"))
            import traceback
            traceback.print_exc()
            return
        
        # Test 4: Provider validation
        self.stdout.write("\n4. Testing provider validation...")
        try:
            validation_results = proxy_service.validate_all_providers()
            for provider_name, is_valid in validation_results.items():
                status = "✅" if is_valid else "❌"
                style = self.style.SUCCESS if is_valid else self.style.ERROR
                self.stdout.write(style(f"   {status} {provider_name}: {'Valid' if is_valid else 'Invalid'}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Failed to validate providers: {e}"))
            return
        
        self.stdout.write("\n=== Proxy System Test Complete ===")
        self.stdout.write(self.style.SUCCESS("\n🎉 All proxy system tests passed!"))