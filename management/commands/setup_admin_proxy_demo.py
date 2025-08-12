"""
Django management command to set up demo proxy data for admin interface.

This command creates sample proxy providers and configurations to demonstrate
the admin interface functionality.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from scrapers.models import ProxyProvider, ProxyConfiguration, ScraperProxyAssignment
from scrapers.proxy.base import ProxyType
import os


class Command(BaseCommand):
    help = 'Set up demo proxy data for admin interface'

    def add_arguments(self, parser):
        parser.add_argument(
            '--with-assignments',
            action='store_true',
            help='Also create scraper proxy assignments'
        )
        parser.add_argument(
            '--clear-existing',
            action='store_true',
            help='Clear existing proxy data before creating demo data'
        )

    def handle(self, *args, **options):
        if options['clear_existing']:
            self._clear_existing_data()

        self.stdout.write('Setting up demo proxy data for admin interface...')

        try:
            with transaction.atomic():
                # Create providers
                webshare_provider = self._create_webshare_provider()
                bright_data_provider = self._create_bright_data_provider()

                # Create configurations
                self._create_proxy_configurations(webshare_provider, bright_data_provider)

                # Create assignments if requested
                if options['with_assignments']:
                    self._create_scraper_assignments()

            self.stdout.write(
                self.style.SUCCESS('Successfully set up demo proxy data!')
            )
            self.stdout.write('')
            self.stdout.write('You can now access the admin interface at:')
            self.stdout.write('- Proxy Providers: /admin/scrapers/proxyprovider/')
            self.stdout.write('- Proxy Configurations: /admin/scrapers/proxyconfiguration/')
            self.stdout.write('- Scraper Assignments: /admin/scrapers/scraperproxyassignment/')
            self.stdout.write('- Usage Logs: /admin/scrapers/proxyusagelog/')

        except Exception as e:
            raise CommandError(f'Failed to set up demo data: {e}')

    def _clear_existing_data(self):
        """Clear existing proxy data."""
        self.stdout.write('Clearing existing proxy data...')
        
        ScraperProxyAssignment.objects.all().delete()
        ProxyConfiguration.objects.all().delete()
        ProxyProvider.objects.all().delete()
        
        self.stdout.write('Cleared existing data.')

    def _create_webshare_provider(self):
        """Create Webshare proxy provider."""
        provider, created = ProxyProvider.objects.get_or_create(
            name='webshare',
            defaults={
                'display_name': 'Webshare',
                'description': 'Webshare rotating proxy service with residential and datacenter options',
                'is_active': True
            }
        )
        
        if created:
            self.stdout.write('Created Webshare provider')
        else:
            self.stdout.write('Webshare provider already exists')
        
        return provider

    def _create_bright_data_provider(self):
        """Create Bright Data proxy provider."""
        provider, created = ProxyProvider.objects.get_or_create(
            name='bright_data',
            defaults={
                'display_name': 'Bright Data',
                'description': 'Bright Data (formerly Luminati) proxy service with global coverage',
                'is_active': True
            }
        )
        
        if created:
            self.stdout.write('Created Bright Data provider')
        else:
            self.stdout.write('Bright Data provider already exists')
        
        return provider

    def _create_proxy_configurations(self, webshare_provider, bright_data_provider):
        """Create proxy configurations for providers."""
        
        # Bright Data configurations (using environment variables if available)
        bright_configs = [
            {
                'name': 'Bright Data Residential',
                'proxy_type': ProxyType.RESIDENTIAL.value,
                'host': os.getenv('BRIGHT_DATA_RESIDENTIAL_HOST', '198.23.239.134'),
                'port': int(os.getenv('BRIGHT_DATA_RESIDENTIAL_PORT', '6540')),
                'username': os.getenv('BRIGHT_DATA_RESIDENTIAL_USERNAME', 'brd-customer-hl_e3f41ce2-zone-residential_proxy1'),
                'password': os.getenv('BRIGHT_DATA_RESIDENTIAL_PASSWORD', 'o7fyb9vqhyog'),
            },
            {
                'name': 'Bright Data Datacenter',
                'proxy_type': ProxyType.DATACENTER.value,
                'host': os.getenv('BRIGHT_DATA_DATACENTER_HOST', '198.23.239.134'),
                'port': int(os.getenv('BRIGHT_DATA_DATACENTER_PORT', '6540')),
                'username': os.getenv('BRIGHT_DATA_DATACENTER_USERNAME', 'brd-customer-hl_e3f41ce2-zone-datacenter_proxy1'),
                'password': os.getenv('BRIGHT_DATA_DATACENTER_PASSWORD', 'o7fyb9vqhyog'),
            }
        ]

        for config_data in bright_configs:
            config, created = ProxyConfiguration.objects.get_or_create(
                provider=bright_data_provider,
                proxy_type=config_data['proxy_type'],
                defaults={
                    'name': config_data['name'],
                    'host': config_data['host'],
                    'port': config_data['port'],
                    'username': config_data['username'],
                    'password': config_data['password'],
                    'is_active': True
                }
            )
            
            if created:
                self.stdout.write(f'Created {config_data["name"]} configuration')
            else:
                self.stdout.write(f'{config_data["name"]} configuration already exists')

        # Webshare configurations (only if environment variables are set)
        if os.getenv('WEBSHARE_RESIDENTIAL_HOST'):
            webshare_configs = [
                {
                    'name': 'Webshare Residential',
                    'proxy_type': ProxyType.RESIDENTIAL.value,
                    'host': os.getenv('WEBSHARE_RESIDENTIAL_HOST'),
                    'port': int(os.getenv('WEBSHARE_RESIDENTIAL_PORT')),
                    'username': os.getenv('WEBSHARE_RESIDENTIAL_USERNAME'),
                    'password': os.getenv('WEBSHARE_RESIDENTIAL_PASSWORD'),
                },
                {
                    'name': 'Webshare Datacenter',
                    'proxy_type': ProxyType.DATACENTER.value,
                    'host': os.getenv('WEBSHARE_DATACENTER_HOST'),
                    'port': int(os.getenv('WEBSHARE_DATACENTER_PORT')),
                    'username': os.getenv('WEBSHARE_DATACENTER_USERNAME'),
                    'password': os.getenv('WEBSHARE_DATACENTER_PASSWORD'),
                }
            ]

            for config_data in webshare_configs:
                config, created = ProxyConfiguration.objects.get_or_create(
                    provider=webshare_provider,
                    proxy_type=config_data['proxy_type'],
                    defaults={
                        'name': config_data['name'],
                        'host': config_data['host'],
                        'port': config_data['port'],
                        'username': config_data['username'],
                        'password': config_data['password'],
                        'is_active': True
                    }
                )
                
                if created:
                    self.stdout.write(f'Created {config_data["name"]} configuration')
                else:
                    self.stdout.write(f'{config_data["name"]} configuration already exists')
        else:
            self.stdout.write('Skipping Webshare configurations - environment variables not set')

    def _create_scraper_assignments(self):
        """Create sample scraper proxy assignments."""
        self.stdout.write('Creating scraper proxy assignments...')

        # Get available configurations
        residential_config = ProxyConfiguration.objects.filter(
            proxy_type=ProxyType.RESIDENTIAL.value,
            is_active=True
        ).first()

        datacenter_config = ProxyConfiguration.objects.filter(
            proxy_type=ProxyType.DATACENTER.value,
            is_active=True
        ).first()

        if not residential_config or not datacenter_config:
            self.stdout.write(self.style.WARNING('Not enough proxy configurations to create assignments'))
            return

        # Create assignments for our scrapers
        assignments = [
            {
                'scraper_name': 'washington_pavilion_scraper',
                'proxy_configuration': residential_config
            },
            {
                'scraper_name': 'broadway_sf_scraper',
                'proxy_configuration': datacenter_config
            },
            {
                'scraper_name': 'david_h_koch_theater_scraper',
                'proxy_configuration': datacenter_config
            }
        ]

        for assignment_data in assignments:
            assignment, created = ScraperProxyAssignment.objects.get_or_create(
                scraper_name=assignment_data['scraper_name'],
                defaults={
                    'proxy_configuration': assignment_data['proxy_configuration'],
                    'is_active': True
                }
            )
            
            if created:
                self.stdout.write(f'Assigned {assignment_data["proxy_configuration"].name} to {assignment_data["scraper_name"]}')
            else:
                self.stdout.write(f'Assignment for {assignment_data["scraper_name"]} already exists')