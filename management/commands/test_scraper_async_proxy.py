"""
Test a real scraper in async context to verify proxy system works in Celery worker environment.
"""
import asyncio
from django.core.management.base import BaseCommand
from scrapers.implementations.washington_pavilion.scraper import WashingtonPavilionScraper


class Command(BaseCommand):
    help = 'Test a real scraper with async proxy verification'

    def handle(self, *args, **options):
        """Test scraper in async context."""
        self.stdout.write("=== Testing Real Scraper with Async Proxy ===")
        
        try:
            # Create scraper instance
            scraper = WashingtonPavilionScraper(
                url="https://washingtonpavilion.org/events",
                scrape_job_id="test_job_123"
            )
            
            self.stdout.write("✅ Scraper instance created successfully")
            
            # Test just the proxy verification part in async context
            self.stdout.write("Testing async proxy verification...")
            asyncio.run(scraper._verify_proxy_ip())
            
            self.stdout.write(self.style.SUCCESS("✅ Async proxy verification completed successfully"))
            self.stdout.write(self.style.SUCCESS("🎉 Scraper proxy system is working correctly in async context!"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Failed: {e}"))
            import traceback
            traceback.print_exc()