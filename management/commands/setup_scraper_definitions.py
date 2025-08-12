"""
Django management command to set up initial scraper definitions.

This command creates ScraperDefinition records for all existing scrapers
with proper configuration and proxy requirements.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from scrapers.models import ScraperDefinition, ProxyConfiguration, ScraperProxyAssignment


class Command(BaseCommand):
    help = 'Set up initial scraper definitions with configurations'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force update existing definitions'
        )
        parser.add_argument(
            '--with-assignments',
            action='store_true',
            help='Also create proxy assignments'
        )

    def handle(self, *args, **options):
        self.force = options['force']
        self.with_assignments = options['with_assignments']

        self.stdout.write('Setting up scraper definitions...')

        try:
            with transaction.atomic():
                # Define our scrapers
                scrapers_config = [
                    {
                        'name': 'washington_pavilion_scraper',
                        'display_name': 'Washington Pavilion',
                        'description': 'Scrapes event and seat information from Washington Pavilion theater',
                        'target_website': 'https://washingtonpavilion.org/',
                        'target_domains': ['washingtonpavilion.org'],
                        'use_proxy': True,
                        'fail_without_proxy': True,
                        'optimization_level': 'balanced',
                        'optimization_enabled': True,
                        'timeout_seconds': 60,
                        'retry_attempts': 3,
                        'enable_screenshots': False,
                        'enable_detailed_logging': False,
                    },
                    {
                        'name': 'broadway_sf_scraper_v5',
                        'display_name': 'Broadway SF',
                        'description': 'Scrapes event calendar and seating data from Broadway SF',
                        'target_website': 'https://www.broadwaysf.com/',
                        'target_domains': ['broadwaysf.com', 'www.broadwaysf.com', 'boltapi.broadwaysf.com', 'calendar-service.core.platform.atgtickets.com'],
                        'status': 'active',
                        'is_enabled': True,
                        'use_proxy': False,  # Disabled for now
                        'fail_without_proxy': False,
                        'optimization_level': 'balanced',
                        'optimization_enabled': True,
                        'timeout_seconds': 45,
                        'retry_attempts': 2,
                        'enable_screenshots': False,
                        'enable_detailed_logging': False,
                    },
                    {
                        'name': 'david_h_koch_theater_scraper',
                        'display_name': 'David H Koch Theater',
                        'description': 'Scrapes performance details and seat availability from David H Koch Theater',
                        'target_website': 'https://tickets.davidhkochtheater.com/',
                        'target_domains': ['tickets.davidhkochtheater.com'],
                        'use_proxy': True,
                        'fail_without_proxy': True,
                        'optimization_level': 'balanced',
                        'optimization_enabled': True,
                        'timeout_seconds': 60,
                        'retry_attempts': 3,
                        'enable_screenshots': False,
                        'enable_detailed_logging': False,
                    }
                ]

                created_count = 0
                updated_count = 0

                for config in scrapers_config:
                    scraper, created = ScraperDefinition.objects.get_or_create(
                        name=config['name'],
                        defaults=config
                    )

                    if created:
                        created_count += 1
                        self.stdout.write(f'  ✓ Created: {scraper.display_name}')
                    elif self.force:
                        # Update existing
                        for key, value in config.items():
                            if key != 'name':  # Don't update the name
                                setattr(scraper, key, value)
                        scraper.save()
                        updated_count += 1
                        self.stdout.write(f'  ↻ Updated: {scraper.display_name}')
                    else:
                        self.stdout.write(f'  → Exists: {scraper.display_name}')

                # Create proxy assignments if requested
                if self.with_assignments:
                    self._create_proxy_assignments()

                self.stdout.write('')
                self.stdout.write(
                    self.style.SUCCESS(
                        f'Successfully set up scraper definitions! '
                        f'Created: {created_count}, Updated: {updated_count}'
                    )
                )

                # Print admin URLs
                self.stdout.write('')
                self.stdout.write('Admin URLs:')
                self.stdout.write('- Scraper Definitions: /admin/scrapers/scraperdefinition/')
                self.stdout.write('- Proxy Assignments: /admin/scrapers/scraperproxyassignment/')
                self.stdout.write('- Scraper Executions: /admin/scrapers/scraperexecution/')
                self.stdout.write('- Scraper Schedules: /admin/scrapers/scraperschedule/')

        except Exception as e:
            raise CommandError(f'Failed to set up scraper definitions: {e}')

    def _create_proxy_assignments(self):
        """Create proxy assignments for scrapers."""
        self.stdout.write('Creating proxy assignments...')

        try:
            # Get available proxy configurations
            residential_proxy = ProxyConfiguration.objects.filter(
                proxy_type='residential',
                is_active=True
            ).first()

            datacenter_proxy = ProxyConfiguration.objects.filter(
                proxy_type='datacenter',
                is_active=True
            ).first()

            if not residential_proxy or not datacenter_proxy:
                self.stdout.write(
                    self.style.WARNING(
                        'Not enough proxy configurations available. Skipping assignments.'
                    )
                )
                return

            # Assignment mappings
            assignments = [
                {
                    'scraper_name': 'washington_pavilion_scraper',
                    'proxy_config': residential_proxy
                },
                {
                    'scraper_name': 'broadway_sf_scraper_v5',
                    'proxy_config': datacenter_proxy
                },
                {
                    'scraper_name': 'david_h_koch_theater_scraper',
                    'proxy_config': datacenter_proxy
                }
            ]

            for assignment_data in assignments:
                # Get the scraper definition
                try:
                    scraper_def = ScraperDefinition.objects.get(
                        name=assignment_data['scraper_name']
                    )
                except ScraperDefinition.DoesNotExist:
                    continue

                assignment, created = ScraperProxyAssignment.objects.get_or_create(
                    scraper_name=assignment_data['scraper_name'],
                    defaults={
                        'scraper_definition': scraper_def,
                        'proxy_configuration': assignment_data['proxy_config'],
                        'is_active': True,
                        'is_primary': True
                    }
                )

                if created:
                    self.stdout.write(
                        f'  ✓ Assigned {assignment_data["proxy_config"].name} to {scraper_def.display_name}'
                    )
                else:
                    self.stdout.write(
                        f'  → Assignment exists: {scraper_def.display_name}'
                    )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Failed to create proxy assignments: {e}')
            )