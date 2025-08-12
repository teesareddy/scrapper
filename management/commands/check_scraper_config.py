from django.core.management.base import BaseCommand
from scrapers.models.scraper_config import ScraperDefinition
from scrapers.factory import ScraperFactory


class Command(BaseCommand):
    help = 'Check and debug scraper configuration'

    def handle(self, *args, **options):
        self.stdout.write("=== Checking ScraperDefinition Records ===")
        
        # Check if any records exist
        all_scrapers = ScraperDefinition.objects.all()
        self.stdout.write(f"Total ScraperDefinition records: {all_scrapers.count()}")
        
        for scraper in all_scrapers:
            self.stdout.write(f"ID: {scraper.scraper_id}")
            self.stdout.write(f"Name: {scraper.name}")
            self.stdout.write(f"Domains: {scraper.target_domains}")
            self.stdout.write(f"Enabled: {scraper.is_enabled}")
            self.stdout.write(f"Status: {scraper.status}")
            self.stdout.write("---")
        
        self.stdout.write("\n=== Testing Domain Extraction ===")
        test_url = "https://wpmi-3encore.shop.secutix.com/selection/event/date?productId=123"
        domain = ScraperFactory.extract_base_domain(test_url)
        self.stdout.write(f"URL: {test_url}")
        self.stdout.write(f"Extracted domain: {domain}")
        
        self.stdout.write("\n=== Testing Database Query ===")
        config = ScraperFactory.get_scraper_config_by_domain(domain)
        self.stdout.write(f"Config found: {config is not None}")
        if config:
            self.stdout.write(f"Scraper name: {config.get('name')}")
        
        self.stdout.write("\n=== Testing Specific Domain Query ===")
        result = ScraperDefinition.objects.filter(
            target_domains__contains=[domain],
            is_enabled=True,
            status='active'
        )
        self.stdout.write(f"Direct query result count: {result.count()}")
        
        # Check for Washington Pavilion specific entries
        self.stdout.write("\n=== Checking for Washington Pavilion Entries ===")
        wp_scrapers = ScraperDefinition.objects.filter(name__icontains='washington')
        self.stdout.write(f"Washington Pavilion scrapers found: {wp_scrapers.count()}")
        
        for scraper in wp_scrapers:
            self.stdout.write(f"Found: {scraper.name}")
            self.stdout.write(f"Domains: {scraper.target_domains}")
            self.stdout.write(f"Type of target_domains: {type(scraper.target_domains)}")
            self.stdout.write(f"Enabled: {scraper.is_enabled}, Status: {scraper.status}")
        
        # If no Washington Pavilion scraper exists, create one
        if wp_scrapers.count() == 0:
            self.stdout.write("\n=== Creating Washington Pavilion ScraperDefinition ===")
            scraper_def = ScraperDefinition.objects.create(
                name='washington_pavilion_scraper_v5',
                display_name='Washington Pavilion Scraper',
                description='Scraper for Washington Pavilion events',
                target_website='https://wpmi-3encore.shop.secutix.com',
                target_domains=["wpmi-3encore.shop.secutix.com"],
                status='active',
                is_enabled=True,
                browser_engine='playwright',
                timeout_seconds=30,
                retry_attempts=3,
                retry_delay_seconds=5
            )
            self.stdout.write(f"Created scraper definition with ID: {scraper_def.scraper_id}")
            self.stdout.write(f"Target domains: {scraper_def.target_domains}")
            
            # Test the query again
            self.stdout.write("\n=== Testing Query After Creation ===")
            config = ScraperFactory.get_scraper_config_by_domain(domain)
            self.stdout.write(f"Config found after creation: {config is not None}")
            if config:
                self.stdout.write(f"Scraper name: {config.get('name')}")
                
        # Test JSONField contains query specifically
        self.stdout.write("\n=== Testing JSONField Query Variations ===")
        
        # Try different query approaches
        queries = [
            ("target_domains__contains", [domain]),
            ("target_domains__icontains", domain),
            ("target_domains__exact", [domain]),
        ]
        
        for field_lookup, value in queries:
            try:
                query_kwargs = {field_lookup: value}
                result = ScraperDefinition.objects.filter(**query_kwargs, is_enabled=True, status='active')
                self.stdout.write(f"Query {field_lookup}={value}: {result.count()} results")
            except Exception as e:
                self.stdout.write(f"Query {field_lookup}={value}: ERROR - {e}")