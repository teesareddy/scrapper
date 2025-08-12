"""
Django management command to assign proxy configurations to specific scrapers.

This command creates ScraperProxyAssignment records to specify which proxy
configuration each scraper should use.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from scrapers.models import ProxyConfiguration, ScraperProxyAssignment
from scrapers.proxy.base import ProxyType


class Command(BaseCommand):
    help = 'Assign proxy configurations to specific scrapers'

    def add_arguments(self, parser):
        parser.add_argument(
            'scraper_name',
            type=str,
            help='Name of the scraper to assign proxy to'
        )
        parser.add_argument(
            '--provider',
            type=str,
            help='Proxy provider name (e.g., webshare, bright_data)'
        )
        parser.add_argument(
            '--proxy-type',
            type=str,
            choices=['residential', 'datacenter'],
            help='Type of proxy to assign'
        )
        parser.add_argument(
            '--config-id',
            type=int,
            help='Specific proxy configuration ID to assign'
        )
        parser.add_argument(
            '--list-configs',
            action='store_true',
            help='List available proxy configurations'
        )
        parser.add_argument(
            '--list-assignments',
            action='store_true',
            help='List current scraper proxy assignments'
        )
        parser.add_argument(
            '--remove',
            action='store_true',
            help='Remove proxy assignment for the scraper'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force update existing assignment'
        )

    def handle(self, *args, **options):
        if options['list_configs']:
            self._list_proxy_configurations()
            return

        if options['list_assignments']:
            self._list_scraper_assignments()
            return

        scraper_name = options['scraper_name']

        if options['remove']:
            self._remove_assignment(scraper_name)
            return

        # Assign proxy to scraper
        config_id = options.get('config_id')
        provider = options.get('provider')
        proxy_type = options.get('proxy_type')

        if config_id:
            proxy_config = self._get_config_by_id(config_id)
        elif provider and proxy_type:
            proxy_config = self._get_config_by_provider_and_type(provider, proxy_type)
        else:
            raise CommandError(
                'Either --config-id or both --provider and --proxy-type must be specified'
            )

        if not proxy_config:
            raise CommandError('No matching proxy configuration found')

        self._assign_proxy_to_scraper(scraper_name, proxy_config, options['force'])

    def _list_proxy_configurations(self):
        """List all available proxy configurations."""
        self.stdout.write(self.style.SUCCESS('Available Proxy Configurations:'))
        self.stdout.write('')

        configs = ProxyConfiguration.objects.select_related('provider').all()
        if not configs:
            self.stdout.write('No proxy configurations found.')
            return

        for config in configs:
            status = '✓ Active' if config.is_active else '✗ Inactive'
            self.stdout.write(
                f'ID: {config.id} | {config.provider.display_name} | '
                f'{config.proxy_type.capitalize()} | {config.host}:{config.port} | {status}'
            )

    def _list_scraper_assignments(self):
        """List all current scraper proxy assignments."""
        self.stdout.write(self.style.SUCCESS('Current Scraper Proxy Assignments:'))
        self.stdout.write('')

        assignments = ScraperProxyAssignment.objects.select_related(
            'proxy_configuration__provider'
        ).filter(is_active=True)

        if not assignments:
            self.stdout.write('No active scraper proxy assignments found.')
            return

        for assignment in assignments:
            config = assignment.proxy_configuration
            self.stdout.write(
                f'{assignment.scraper_name} → {config.provider.display_name} '
                f'{config.proxy_type.capitalize()} ({config.host}:{config.port})'
            )

    def _get_config_by_id(self, config_id: int):
        """Get proxy configuration by ID."""
        try:
            return ProxyConfiguration.objects.get(id=config_id, is_active=True)
        except ProxyConfiguration.DoesNotExist:
            raise CommandError(f'Proxy configuration with ID {config_id} not found or inactive')

    def _get_config_by_provider_and_type(self, provider_name: str, proxy_type: str):
        """Get proxy configuration by provider name and type."""
        try:
            return ProxyConfiguration.objects.select_related('provider').get(
                provider__name=provider_name,
                proxy_type=proxy_type,
                is_active=True
            )
        except ProxyConfiguration.DoesNotExist:
            raise CommandError(
                f'No active {proxy_type} proxy configuration found for provider {provider_name}'
            )

    def _assign_proxy_to_scraper(self, scraper_name: str, proxy_config: ProxyConfiguration, force: bool):
        """Assign proxy configuration to scraper."""
        try:
            with transaction.atomic():
                assignment, created = ScraperProxyAssignment.objects.get_or_create(
                    scraper_name=scraper_name,
                    defaults={
                        'proxy_configuration': proxy_config,
                        'is_active': True
                    }
                )

                if not created:
                    if force:
                        assignment.proxy_configuration = proxy_config
                        assignment.is_active = True
                        assignment.save()
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'Updated proxy assignment for {scraper_name} to '
                                f'{proxy_config.provider.display_name} {proxy_config.proxy_type}'
                            )
                        )
                    else:
                        raise CommandError(
                            f'Proxy assignment already exists for {scraper_name}. Use --force to update.'
                        )
                else:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'Assigned {proxy_config.provider.display_name} {proxy_config.proxy_type} '
                            f'proxy to {scraper_name}'
                        )
                    )

        except Exception as e:
            raise CommandError(f'Failed to assign proxy: {e}')

    def _remove_assignment(self, scraper_name: str):
        """Remove proxy assignment for scraper."""
        try:
            assignment = ScraperProxyAssignment.objects.get(
                scraper_name=scraper_name,
                is_active=True
            )
            assignment.is_active = False
            assignment.save()
            
            self.stdout.write(
                self.style.SUCCESS(f'Removed proxy assignment for {scraper_name}')
            )
        except ScraperProxyAssignment.DoesNotExist:
            raise CommandError(f'No active proxy assignment found for {scraper_name}')