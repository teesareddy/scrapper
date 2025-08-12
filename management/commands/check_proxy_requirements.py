"""
Django management command to check existing scrapers and their proxy requirements.

Usage:
    python manage.py check_proxy_requirements

This command checks all existing scrapers and shows their proxy configuration status.
"""

from django.core.management.base import BaseCommand
from scrapers.models import ScraperDefinition, ProxyConfiguration, ScraperProxyAssignment
from scrapers.proxy.service import proxy_service
from scrapers.proxy.base import ProxyType


class Command(BaseCommand):
    help = 'Check existing scrapers and their proxy requirements'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('🔍 Checking existing scrapers and proxy requirements...'))
        self.stdout.write('=' * 80)

        # Get all scrapers
        scrapers = ScraperDefinition.objects.all()
        
        if not scrapers.exists():
            self.stdout.write(self.style.WARNING('⚠️ No scrapers found in database'))
            return

        self.stdout.write(f"📊 Found {scrapers.count()} scrapers in database")
        self.stdout.write('')

        # Check proxy configurations
        active_proxies = ProxyConfiguration.objects.filter(is_active=True, status='active')
        self.stdout.write(f"📡 Active proxy configurations: {active_proxies.count()}")
        
        for proxy in active_proxies:
            self.stdout.write(f"  - {proxy.name} ({proxy.proxy_type}) - {proxy.host}:{proxy.port}")
        
        self.stdout.write('')

        # Check each scraper
        for scraper in scrapers:
            self.stdout.write(f"🔧 Scraper: {scraper.name}")
            self.stdout.write(f"   Display Name: {scraper.display_name}")
            self.stdout.write(f"   Use Proxy: {scraper.use_proxy}")
            self.stdout.write(f"   Required Proxy Type: {scraper.proxy_type_required}")
            self.stdout.write(f"   Fail Without Proxy: {scraper.fail_without_proxy}")
            
            # Check proxy assignment
            assignment = ScraperProxyAssignment.objects.filter(
                scraper_name=scraper.name, 
                is_active=True
            ).first()
            
            if assignment:
                self.stdout.write(f"   Assigned Proxy: {assignment.proxy_configuration.name}")
                self.stdout.write(f"   Proxy Status: {assignment.proxy_configuration.status}")
            else:
                self.stdout.write("   Assigned Proxy: None")

            # Test proxy retrieval
            if scraper.use_proxy:
                self.stdout.write("   Testing proxy retrieval...")
                try:
                    proxy_type = None
                    if scraper.proxy_type_required and scraper.proxy_type_required != 'auto':
                        proxy_type = ProxyType(scraper.proxy_type_required)
                    
                    proxy_credentials = proxy_service.get_proxy_for_scraper(
                        scraper_name=scraper.name,
                        proxy_type=proxy_type
                    )
                    
                    if proxy_credentials:
                        self.stdout.write(self.style.SUCCESS(f"   ✅ Proxy available: {proxy_credentials.host}:{proxy_credentials.port}"))
                    else:
                        if scraper.fail_without_proxy:
                            self.stdout.write(self.style.WARNING("   ⚠️ No proxy but scraper requires it - this should fail"))
                        else:
                            self.stdout.write("   ℹ️ No proxy but scraper can continue without it")
                            
                except Exception as e:
                    if scraper.fail_without_proxy and "fail_without_proxy=True" in str(e):
                        self.stdout.write(self.style.SUCCESS(f"   ✅ Properly failed as expected: {str(e)[:100]}..."))
                    else:
                        self.stdout.write(self.style.ERROR(f"   ❌ Unexpected error: {str(e)[:100]}..."))
            else:
                self.stdout.write("   ℹ️ Proxy disabled for this scraper")

            self.stdout.write('')

        # Summary
        self.stdout.write('=' * 80)
        
        # Count scrapers by proxy requirements
        proxy_required = scrapers.filter(use_proxy=True, fail_without_proxy=True).count()
        proxy_optional = scrapers.filter(use_proxy=True, fail_without_proxy=False).count()
        no_proxy = scrapers.filter(use_proxy=False).count()
        
        self.stdout.write("📈 Summary:")
        self.stdout.write(f"   Scrapers requiring proxy: {proxy_required}")
        self.stdout.write(f"   Scrapers with optional proxy: {proxy_optional}")
        self.stdout.write(f"   Scrapers without proxy: {no_proxy}")
        
        # Check for potential issues
        unassigned_required = scrapers.filter(use_proxy=True, fail_without_proxy=True).exclude(
            name__in=ScraperProxyAssignment.objects.filter(is_active=True).values_list('scraper_name', flat=True)
        ).count()
        
        if unassigned_required > 0:
            self.stdout.write(self.style.WARNING(f"⚠️ {unassigned_required} scrapers require proxy but have no assignment"))
        else:
            self.stdout.write(self.style.SUCCESS("✅ All scrapers requiring proxy have assignments"))

        self.stdout.write('')
        self.stdout.write("💡 To test proxy failure behavior, run: python manage.py test_proxy_failure")