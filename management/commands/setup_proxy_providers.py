"""
Django management command to set up proxy providers in the database.

This command creates proxy provider and configuration records based on
environment variables or command line arguments.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from scrapers.models import ProxyProvider, ProxyConfiguration
from scrapers.proxy.base import ProxyType
import os


class Command(BaseCommand):
    help = 'Set up proxy providers and configurations in the database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--provider',
            type=str,
            choices=['webshare', 'bright_data'],
            help='Specific provider to set up (default: all configured providers)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force update existing configurations'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be created without actually creating it'
        )

    def handle(self, *args, **options):
        self.dry_run = options['dry_run']
        self.force = options['force']
        provider_filter = options.get('provider')

        if self.dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be made'))

        providers_to_setup = []
        
        # Check which providers are configured
        if not provider_filter or provider_filter == 'webshare':
            if self._is_webshare_configured():
                providers_to_setup.append('webshare')
        
        if not provider_filter or provider_filter == 'bright_data':
            if self._is_bright_data_configured():
                providers_to_setup.append('bright_data')

        if not providers_to_setup:
            self.stdout.write(
                self.style.ERROR('No proxy providers are configured in environment variables')
            )
            self.stdout.write('Please set the required environment variables and try again.')
            return

        self.stdout.write(f'Setting up providers: {", ".join(providers_to_setup)}')

        try:
            with transaction.atomic():
                for provider_name in providers_to_setup:
                    if provider_name == 'webshare':
                        self._setup_webshare_provider()
                    elif provider_name == 'bright_data':
                        self._setup_bright_data_provider()

            if not self.dry_run:
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully set up {len(providers_to_setup)} proxy provider(s)')
                )
        except Exception as e:
            raise CommandError(f'Failed to set up proxy providers: {e}')

    def _is_webshare_configured(self) -> bool:
        """Check if Webshare environment variables are configured."""
        required_vars = [
            'WEBSHARE_RESIDENTIAL_HOST', 'WEBSHARE_RESIDENTIAL_PORT',
            'WEBSHARE_RESIDENTIAL_USERNAME', 'WEBSHARE_RESIDENTIAL_PASSWORD'
        ]
        return all(os.getenv(var) for var in required_vars)

    def _is_bright_data_configured(self) -> bool:
        """Check if Bright Data environment variables are configured."""
        required_vars = [
            'BRIGHT_DATA_RESIDENTIAL_HOST', 'BRIGHT_DATA_RESIDENTIAL_PORT',
            'BRIGHT_DATA_RESIDENTIAL_USERNAME', 'BRIGHT_DATA_RESIDENTIAL_PASSWORD'
        ]
        return all(os.getenv(var) for var in required_vars)

    def _setup_webshare_provider(self):
        """Set up Webshare proxy provider and configurations."""
        provider_name = 'webshare'
        self.stdout.write(f'Setting up {provider_name} provider...')

        # Create or get provider
        provider, created = self._get_or_create_provider(
            name=provider_name,
            display_name='Webshare',
            description='Webshare rotating proxy service'
        )

        if created:
            self.stdout.write(f'  Created provider: {provider_name}')
        else:
            self.stdout.write(f'  Provider exists: {provider_name}')

        # Set up residential proxy configuration
        residential_config = self._setup_proxy_config(
            provider=provider,
            proxy_type=ProxyType.RESIDENTIAL,
            env_prefix='WEBSHARE_RESIDENTIAL',
            name='Webshare Residential'
        )

        # Set up datacenter proxy configuration
        datacenter_config = self._setup_proxy_config(
            provider=provider,
            proxy_type=ProxyType.DATACENTER,
            env_prefix='WEBSHARE_DATACENTER',
            name='Webshare Datacenter'
        )

    def _setup_bright_data_provider(self):
        """Set up Bright Data proxy provider and configurations."""
        provider_name = 'bright_data'
        self.stdout.write(f'Setting up {provider_name} provider...')

        # Create or get provider
        provider, created = self._get_or_create_provider(
            name=provider_name,
            display_name='Bright Data',
            description='Bright Data proxy service (formerly Luminati)'
        )

        if created:
            self.stdout.write(f'  Created provider: {provider_name}')
        else:
            self.stdout.write(f'  Provider exists: {provider_name}')

        # Set up residential proxy configuration
        residential_config = self._setup_proxy_config(
            provider=provider,
            proxy_type=ProxyType.RESIDENTIAL,
            env_prefix='BRIGHT_DATA_RESIDENTIAL',
            name='Bright Data Residential'
        )

        # Set up datacenter proxy configuration
        datacenter_config = self._setup_proxy_config(
            provider=provider,
            proxy_type=ProxyType.DATACENTER,
            env_prefix='BRIGHT_DATA_DATACENTER',
            name='Bright Data Datacenter'
        )

    def _get_or_create_provider(self, name: str, display_name: str, description: str):
        """Get or create a proxy provider."""
        if self.dry_run:
            try:
                provider = ProxyProvider.objects.get(name=name)
                return provider, False
            except ProxyProvider.DoesNotExist:
                self.stdout.write(f'  Would create provider: {name}')
                return None, True

        provider, created = ProxyProvider.objects.get_or_create(
            name=name,
            defaults={
                'display_name': display_name,
                'description': description,
                'is_active': True
            }
        )

        if not created and self.force:
            provider.display_name = display_name
            provider.description = description
            provider.is_active = True
            provider.save()

        return provider, created

    def _setup_proxy_config(self, provider, proxy_type: ProxyType, env_prefix: str, name: str):
        """Set up a proxy configuration."""
        host = os.getenv(f'{env_prefix}_HOST')
        port = os.getenv(f'{env_prefix}_PORT')
        username = os.getenv(f'{env_prefix}_USERNAME')
        password = os.getenv(f'{env_prefix}_PASSWORD')

        if not all([host, port, username, password]):
            self.stdout.write(
                self.style.WARNING(f'  Skipping {proxy_type.value} config - missing environment variables')
            )
            return None

        try:
            port = int(port)
        except ValueError:
            self.stdout.write(
                self.style.ERROR(f'  Invalid port number for {proxy_type.value}: {port}')
            )
            return None

        if self.dry_run:
            try:
                config = ProxyConfiguration.objects.get(
                    provider=provider,
                    proxy_type=proxy_type.value
                )
                self.stdout.write(f'  Would update {proxy_type.value} configuration')
                return config
            except ProxyConfiguration.DoesNotExist:
                self.stdout.write(f'  Would create {proxy_type.value} configuration')
                return None

        config, created = ProxyConfiguration.objects.get_or_create(
            provider=provider,
            proxy_type=proxy_type.value,
            defaults={
                'name': name,
                'host': host,
                'port': port,
                'username': username,
                'password': password,
                'is_active': True
            }
        )

        if not created and self.force:
            config.name = name
            config.host = host
            config.port = port
            config.username = username
            config.password = password
            config.is_active = True
            config.save()
            self.stdout.write(f'  Updated {proxy_type.value} configuration')
        elif created:
            self.stdout.write(f'  Created {proxy_type.value} configuration')
        else:
            self.stdout.write(f'  {proxy_type.value} configuration already exists (use --force to update)')

        return config