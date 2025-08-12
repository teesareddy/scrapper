#!/usr/bin/env python3
"""
Management command to set up a comprehensive proxy system for scrapers
Creates proxy providers, configurations, and assigns them to scrapers
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from scrapers.models import (
    ProxyProvider, ProxyConfiguration, ScraperDefinition, 
    ScraperProxyAssignment
)
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Set up comprehensive proxy system with providers, configurations, and assignments'

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Reset all proxy configurations (WARNING: This will delete existing data)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be created without actually creating',
        )

    def handle(self, *args, **options):
        """Set up the complete proxy system"""
        
        if options['reset'] and not options['dry_run']:
            self.stdout.write(
                self.style.WARNING("Resetting proxy system...")
            )
            # Delete existing data in reverse dependency order
            ScraperProxyAssignment.objects.all().delete()
            ProxyConfiguration.objects.all().delete()
            ProxyProvider.objects.all().delete()
            self.stdout.write(
                self.style.SUCCESS("Existing proxy data cleared.")
            )

        # Step 1: Create Proxy Providers
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("STEP 1: Setting up Proxy Providers"))
        self.stdout.write("="*60)
        
        providers_config = [
            {
                'name': 'bright_data',
                'display_name': 'Bright Data (BRD)',
                'description': 'High-quality residential and datacenter proxies from Bright Data with excellent reliability.',
                'base_url': 'https://brightdata.com',
                'auth_method': 'basic',
                'supports_rotation': True,
                'supports_geolocation': True,
                'supports_session_persistence': True,
                'is_active': True,
                'is_available': True
            },
            {
                'name': 'webshare',
                'display_name': 'Webshare',
                'description': 'Fast and reliable proxy service with good performance for web scraping.',
                'base_url': 'https://webshare.io',
                'auth_method': 'basic',
                'supports_rotation': True,
                'supports_geolocation': False,
                'supports_session_persistence': False,
                'is_active': True,
                'is_available': True
            },
            {
                'name': 'internal',
                'display_name': 'Internal Proxy Pool',
                'description': 'Internal proxy pool for development and testing purposes.',
                'base_url': '',
                'auth_method': 'basic',
                'supports_rotation': False,
                'supports_geolocation': False,
                'supports_session_persistence': False,
                'is_active': True,
                'is_available': True
            }
        ]

        created_providers = {}
        for provider_config in providers_config:
            if options['dry_run']:
                self.stdout.write(
                    self.style.WARNING(f"DRY RUN: Would create provider '{provider_config['name']}'")
                )
                continue

            provider, created = ProxyProvider.objects.get_or_create(
                name=provider_config['name'],
                defaults=provider_config
            )
            
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f"✅ Created provider: {provider.display_name}")
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f"⚠️ Provider already exists: {provider.display_name}")
                )
            
            created_providers[provider_config['name']] = provider

        # Step 2: Create Proxy Configurations
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("STEP 2: Setting up Proxy Configurations"))
        self.stdout.write("="*60)

        if not options['dry_run']:
            bright_data_provider = created_providers['bright_data']
            webshare_provider = created_providers['webshare']
            internal_provider = created_providers['internal']
        
        configurations_config = [
            {
                'provider_name': 'bright_data',
                'name': 'brd_residential_primary',
                'description': 'Primary residential proxy configuration for Bright Data',
                'proxy_type': 'residential',
                'host': '198.23.239.134',
                'port': 6540,
                'username': 'brd-customer-hl_e3f41ce2-zone-datacenter_proxy1',
                'password': 'o7fyb9vqhyog',
                'protocol': 'http',
                'country_code': 'US',
                'region': 'Various',
                'city': '',
                'max_concurrent_connections': 10,
                'timeout_seconds': 30,
                'retry_attempts': 3,
                'status': 'active',
                'is_active': True,
                'priority': 1
            },
            {
                'provider_name': 'bright_data',
                'name': 'brd_datacenter_backup',
                'description': 'Backup datacenter proxy configuration for Bright Data',
                'proxy_type': 'datacenter',
                'host': '198.23.239.134',
                'port': 6541,
                'username': 'brd-customer-hl_e3f41ce2-zone-datacenter_proxy2',
                'password': 'o7fyb9vqhyog',
                'protocol': 'http',
                'country_code': 'US',
                'region': 'Various',
                'city': '',
                'max_concurrent_connections': 5,
                'timeout_seconds': 30,
                'retry_attempts': 3,
                'status': 'active',
                'is_active': True,
                'priority': 2
            },
            {
                'provider_name': 'internal',
                'name': 'development_proxy',
                'description': 'Development proxy for testing purposes',
                'proxy_type': 'static_datacenter',
                'host': '127.0.0.1',
                'port': 8080,
                'username': '',
                'password': '',
                'protocol': 'http',
                'country_code': 'US',
                'region': 'Local',
                'city': 'Local',
                'max_concurrent_connections': 1,
                'timeout_seconds': 30,
                'retry_attempts': 1,
                'status': 'inactive',
                'is_active': False,
                'priority': 10
            }
        ]

        created_configurations = {}
        for config in configurations_config:
            if options['dry_run']:
                self.stdout.write(
                    self.style.WARNING(f"DRY RUN: Would create configuration '{config['name']}'")
                )
                continue

            provider_name = config.pop('provider_name')
            provider = created_providers[provider_name]
            
            proxy_config, created = ProxyConfiguration.objects.get_or_create(
                provider=provider,
                name=config['name'],
                defaults=config
            )
            
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f"✅ Created proxy config: {proxy_config.name}")
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f"⚠️ Proxy config already exists: {proxy_config.name}")
                )
            
            created_configurations[config['name']] = proxy_config

        # Step 3: Assign Proxies to Scrapers
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("STEP 3: Assigning Proxies to Scrapers"))
        self.stdout.write("="*60)

        # Get all scrapers
        if not options['dry_run']:
            scrapers = ScraperDefinition.objects.filter(is_enabled=True)
            
            if not scrapers.exists():
                self.stdout.write(
                    self.style.ERROR("❌ No enabled scrapers found. Run 'python manage.py register_existing_scrapers' first.")
                )
                return

            # Assignment configuration
            scraper_assignments = [
                {
                    'scraper_names': ['broadway_sf_scraper_v5'],
                    'primary_proxy': 'brd_residential_primary',
                    'fallback_proxy': 'brd_datacenter_backup',
                    'max_requests_per_hour': 100,
                    'max_concurrent_requests': 2
                },
                {
                    'scraper_names': ['david_h_koch_theater_scraper_v5'],
                    'primary_proxy': 'brd_residential_primary',
                    'fallback_proxy': 'brd_datacenter_backup',
                    'max_requests_per_hour': 150,
                    'max_concurrent_requests': 1
                },
                {
                    'scraper_names': ['washington_pavilion_scraper_v5'],
                    'primary_proxy': 'brd_residential_primary',
                    'fallback_proxy': 'brd_datacenter_backup',
                    'max_requests_per_hour': 80,
                    'max_concurrent_requests': 1
                }
            ]

            assignment_count = 0
            for assignment in scraper_assignments:
                primary_config = created_configurations.get(assignment['primary_proxy'])
                fallback_config = created_configurations.get(assignment['fallback_proxy'])
                
                for scraper_name in assignment['scraper_names']:
                    try:
                        scraper = scrapers.get(name=scraper_name)
                    except ScraperDefinition.DoesNotExist:
                        self.stdout.write(
                            self.style.ERROR(f"❌ Scraper '{scraper_name}' not found")
                        )
                        continue

                    # Create primary assignment
                    if primary_config:
                        primary_assignment, created = ScraperProxyAssignment.objects.get_or_create(
                            scraper_name=scraper_name,
                            scraper_definition=scraper,
                            proxy_configuration=primary_config,
                            defaults={
                                'is_primary': True,
                                'is_fallback': False,
                                'fallback_order': 0,
                                'max_requests_per_hour': assignment['max_requests_per_hour'],
                                'max_concurrent_requests': assignment['max_concurrent_requests'],
                                'is_active': True
                            }
                        )
                        
                        if created:
                            assignment_count += 1
                            self.stdout.write(
                                self.style.SUCCESS(f"✅ Assigned primary proxy to {scraper_name}")
                            )

                    # Create fallback assignment
                    if fallback_config:
                        fallback_assignment, created = ScraperProxyAssignment.objects.get_or_create(
                            scraper_name=f"{scraper_name}_fallback",
                            scraper_definition=scraper,
                            proxy_configuration=fallback_config,
                            defaults={
                                'is_primary': False,
                                'is_fallback': True,
                                'fallback_order': 1,
                                'max_requests_per_hour': assignment['max_requests_per_hour'] // 2,
                                'max_concurrent_requests': 1,
                                'is_active': True
                            }
                        )
                        
                        if created:
                            assignment_count += 1
                            self.stdout.write(
                                self.style.SUCCESS(f"✅ Assigned fallback proxy to {scraper_name}")
                            )

        else:
            self.stdout.write(
                self.style.WARNING("DRY RUN: Would assign proxies to all enabled scrapers")
            )

        # Step 4: Summary and Next Steps
        self.stdout.write("\n" + "="*80)
        self.stdout.write(self.style.SUCCESS("COMPREHENSIVE PROXY SYSTEM SETUP COMPLETE"))
        self.stdout.write("="*80)
        
        if not options['dry_run']:
            provider_count = ProxyProvider.objects.count()
            config_count = ProxyConfiguration.objects.count()
            assignment_count = ScraperProxyAssignment.objects.count()
            
            self.stdout.write(f"📊 SUMMARY:")
            self.stdout.write(f"   • Proxy Providers: {provider_count}")
            self.stdout.write(f"   • Proxy Configurations: {config_count}")
            self.stdout.write(f"   • Scraper Assignments: {assignment_count}")
            
            self.stdout.write(f"\n🎯 NEXT STEPS:")
            self.stdout.write(f"   1. Access Django admin at /admin/")
            self.stdout.write(f"   2. Test proxy configurations:")
            self.stdout.write(f"      python manage.py test_proxy")
            self.stdout.write(f"   3. Monitor scraper performance in admin dashboard")
            self.stdout.write(f"   4. Adjust proxy assignments as needed")
            
            self.stdout.write(f"\n🔧 ADMIN SECTIONS:")
            self.stdout.write(f"   • Proxy Providers: /admin/scrapers/proxyprovider/")
            self.stdout.write(f"   • Proxy Configurations: /admin/scrapers/proxyconfiguration/")
            self.stdout.write(f"   • Scraper Definitions: /admin/scrapers/scraperdefinition/")
            self.stdout.write(f"   • Proxy Assignments: /admin/scrapers/scraperproxyassignment/")
            
            self.stdout.write(
                self.style.SUCCESS(f"\n✅ Proxy system is ready! Check the Django admin for configuration.")
            )
        else:
            self.stdout.write(
                self.style.WARNING("DRY RUN COMPLETE: Run without --dry-run to actually create the proxy system")
            )