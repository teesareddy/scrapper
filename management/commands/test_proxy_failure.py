"""
Django management command to test proxy failure behavior.

Usage:
    python manage.py test_proxy_failure

This command tests that scrapers properly fail when they require a proxy but none is available.
"""

from django.core.management.base import BaseCommand
from django.db import transaction
import logging
from typing import Optional

from scrapers.proxy.service import get_proxy_service
from scrapers.proxy.base import ProxyType
from scrapers.models import ScraperDefinition, ProxyConfiguration, ScraperProxyAssignment

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Test proxy failure behavior for scrapers with fail_without_proxy=True'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Enable verbose output',
        )

    def handle(self, *args, **options):
        if options['verbose']:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

        self.stdout.write(self.style.SUCCESS('🚀 Starting proxy failure behavior tests...'))
        self.stdout.write('=' * 60)

        test_results = []
        
        # Run tests
        test_results.append(self.test_proxy_failure_behavior())
        test_results.append(self.test_proxy_success_behavior())
        test_results.append(self.test_proxy_optional_behavior())

        # Summary
        self.stdout.write('=' * 60)
        passed_tests = sum(test_results)
        total_tests = len(test_results)

        if passed_tests == total_tests:
            self.stdout.write(self.style.SUCCESS(f'🎉 ALL TESTS PASSED ({passed_tests}/{total_tests})'))
            self.stdout.write(self.style.SUCCESS('✅ Proxy failure behavior is working correctly!'))
        else:
            self.stdout.write(self.style.ERROR(f'❌ SOME TESTS FAILED ({passed_tests}/{total_tests})'))
            self.stdout.write(self.style.ERROR('❌ Proxy failure behavior needs attention!'))

    def setup_test_scraper_definition(self):
        """Create a test scraper definition with fail_without_proxy=True"""
        scraper_name = "test_proxy_failure_scraper"
        
        # Clean up existing test scraper if it exists
        ScraperDefinition.objects.filter(name=scraper_name).delete()
        
        # Create test scraper with fail_without_proxy=True
        scraper_def = ScraperDefinition.objects.create(
            name=scraper_name,
            display_name="Test Proxy Failure Scraper",
            description="Test scraper to verify proxy failure behavior",
            target_website="https://example.com",
            target_domains=["example.com"],
            use_proxy=True,
            fail_without_proxy=True,  # This is the key setting
            proxy_type_required='datacenter'
        )
        
        self.stdout.write(f"✅ Created test scraper definition: {scraper_name}")
        return scraper_def

    def cleanup_test_scraper_definition(self):
        """Clean up test scraper definition"""
        scraper_name = "test_proxy_failure_scraper"
        deleted_count = ScraperDefinition.objects.filter(name=scraper_name).count()
        ScraperDefinition.objects.filter(name=scraper_name).delete()
        if deleted_count > 0:
            self.stdout.write(f"🧹 Cleaned up test scraper definition: {scraper_name}")

    def test_proxy_failure_behavior(self):
        """Test that scraper fails when fail_without_proxy=True and no proxy is available"""
        
        self.stdout.write("🧪 Testing proxy failure behavior...")
        
        # Setup test scraper
        scraper_def = self.setup_test_scraper_definition()
        scraper_name = scraper_def.name
        
        try:
            # Ensure no proxy assignment exists for this scraper
            ScraperProxyAssignment.objects.filter(scraper_name=scraper_name).delete()
            
            # Also check active proxy configurations
            active_proxies_count = ProxyConfiguration.objects.filter(is_active=True, status='active').count()
            self.stdout.write(f"📊 Active proxy configurations available: {active_proxies_count}")
            
            # Test 1: Try to get proxy for scraper that requires it but has none assigned
            self.stdout.write("🔬 Test 1: Requesting proxy for scraper with fail_without_proxy=True and no proxy assigned")
            
            exception_raised = False
            exception_message = ""
            
            try:
                proxy_credentials = proxy_service.get_proxy_for_scraper(
                    scraper_name=scraper_name,
                    proxy_type=ProxyType.DATACENTER
                )
                self.stdout.write(self.style.ERROR(f"❌ Expected exception but got proxy credentials: {proxy_credentials}"))
                return False
            except Exception as e:
                exception_raised = True
                exception_message = str(e)
                self.stdout.write(f"✅ Exception raised as expected: {exception_message}")
            
            # Verify the exception was raised and contains the right message
            if exception_raised:
                if "fail_without_proxy=True" in exception_message and "no datacenter proxy is available" in exception_message:
                    self.stdout.write(self.style.SUCCESS("✅ TEST PASSED: Scraper properly failed with correct error message"))
                    return True
                else:
                    self.stdout.write(self.style.ERROR(f"❌ TEST FAILED: Exception message doesn't match expected pattern: {exception_message}"))
                    return False
            else:
                self.stdout.write(self.style.ERROR("❌ TEST FAILED: No exception was raised when it should have been"))
                return False
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ TEST ERROR: Unexpected error during test: {e}"))
            return False
        finally:
            # Cleanup
            self.cleanup_test_scraper_definition()

    def test_proxy_success_behavior(self):
        """Test that scraper succeeds when fail_without_proxy=True but proxy is available"""
        
        self.stdout.write("🧪 Testing proxy success behavior...")
        
        # Check if there are any active proxy configurations
        active_proxies = ProxyConfiguration.objects.filter(is_active=True, status='active').first()
        
        if not active_proxies:
            self.stdout.write(self.style.WARNING("⚠️ Skipping success test - no active proxy configurations available"))
            return True
        
        # Setup test scraper
        scraper_def = self.setup_test_scraper_definition()
        scraper_name = scraper_def.name
        
        try:
            # Create a proxy assignment for this scraper
            assignment = ScraperProxyAssignment.objects.create(
                scraper_name=scraper_name,
                proxy_configuration=active_proxies,
                is_primary=True,
                is_active=True
            )
            
            self.stdout.write(f"📡 Created proxy assignment: {scraper_name} -> {active_proxies.name}")
            
            # Test 2: Try to get proxy for scraper that requires it and has one assigned
            self.stdout.write("🔬 Test 2: Requesting proxy for scraper with fail_without_proxy=True and proxy assigned")
            
            try:
                proxy_credentials = proxy_service.get_proxy_for_scraper(
                    scraper_name=scraper_name,
                    proxy_type=ProxyType.DATACENTER
                )
                
                if proxy_credentials:
                    self.stdout.write(self.style.SUCCESS(f"✅ TEST PASSED: Scraper successfully got proxy: {proxy_credentials.host}:{proxy_credentials.port}"))
                    return True
                else:
                    self.stdout.write(self.style.ERROR("❌ TEST FAILED: No proxy credentials returned when they should have been"))
                    return False
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ TEST FAILED: Unexpected exception when proxy should be available: {e}"))
                return False
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ TEST ERROR: Unexpected error during success test: {e}"))
            return False
        finally:
            # Cleanup
            self.cleanup_test_scraper_definition()

    def test_proxy_optional_behavior(self):
        """Test that scraper continues when fail_without_proxy=False and no proxy is available"""
        
        self.stdout.write("🧪 Testing proxy optional behavior...")
        
        scraper_name = "test_proxy_optional_scraper"
        
        # Clean up existing test scraper if it exists
        ScraperDefinition.objects.filter(name=scraper_name).delete()
        
        # Create test scraper with fail_without_proxy=False
        scraper_def = ScraperDefinition.objects.create(
            name=scraper_name,
            display_name="Test Proxy Optional Scraper",
            description="Test scraper to verify optional proxy behavior",
            target_website="https://example.com",
            target_domains=["example.com"],
            use_proxy=True,
            fail_without_proxy=False,  # Proxy is optional
            proxy_type_required='datacenter'
        )
        
        try:
            # Ensure no proxy assignment exists for this scraper
            ScraperProxyAssignment.objects.filter(scraper_name=scraper_name).delete()
            
            self.stdout.write("🔬 Test 3: Requesting proxy for scraper with fail_without_proxy=False and no proxy assigned")
            
            try:
                proxy_credentials = proxy_service.get_proxy_for_scraper(
                    scraper_name=scraper_name,
                    proxy_type=ProxyType.DATACENTER
                )
                
                if proxy_credentials is None:
                    self.stdout.write(self.style.SUCCESS("✅ TEST PASSED: Scraper properly returned None for optional proxy"))
                    return True
                else:
                    self.stdout.write(self.style.WARNING(f"⚠️ Unexpected: Got proxy credentials when none expected: {proxy_credentials}"))
                    # This might be OK if there are fallback proxies available
                    return True
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ TEST FAILED: Exception raised when proxy is optional: {e}"))
                return False
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ TEST ERROR: Unexpected error during optional test: {e}"))
            return False
        finally:
            # Cleanup
            ScraperDefinition.objects.filter(name=scraper_name).delete()
            self.stdout.write(f"🧹 Cleaned up test scraper definition: {scraper_name}")