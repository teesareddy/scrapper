#!/usr/bin/env python3
"""
Management command to register all existing scrapers in the database
This creates ScraperDefinition entries for all scrapers
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from scrapers.models import ScraperDefinition
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Register all existing scrapers in the database with their configurations'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force update existing scraper definitions',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be created without actually creating',
        )

    def handle(self, *args, **options):
        """Register all scrapers in the database"""
        
        # Define all existing scrapers with their configurations
        scrapers_config = [
            {
                'name': 'broadway_sf_scraper_v5',
                'display_name': 'Broadway SF Scraper',
                'description': 'Scrapes event data from Broadway SF theater including performance schedules, seat availability, and pricing information.',
                'target_website': 'https://www.broadwaysf.com',
                'target_domains': ['broadwaysf.com', 'www.broadwaysf.com'],
                'status': 'active',
                'is_enabled': True,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'calendar_scraping': True,
                    'seating_chart_analysis': True,
                    'price_tracking': True,
                    'required_fields': ['venue_info', 'event_info']
                }
            },
            {
                'name': 'david_h_koch_theater_scraper_v5',
                'display_name': 'David H. Koch Theater Scraper',
                'description': 'Scrapes performance data from David H. Koch Theater including event catalog, performance times, seating charts, and ticket availability.',
                'target_website': 'https://www.lincolncenter.org',
                'target_domains': ['lincolncenter.org', 'www.lincolncenter.org'],
                'status': 'active',
                'is_enabled': True,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 1500,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 12,
                'custom_settings': {
                    'performance_data_extraction': True,
                    'seat_availability_tracking': True,
                    'venue_capacity_monitoring': True,
                    'required_fields': ['venue_info', 'event_info', 'performance_details']
                }
            },
            {
                'name': 'washington_pavilion_scraper_v5',
                'display_name': 'Washington Pavilion Scraper',
                'description': 'Scrapes event information from Washington Pavilion including show schedules, venue capacity, and booking status.',
                'target_website': 'https://wpmi-3encore.shop.secutix.com',
                'target_domains': ['wpmi-3encore.shop.secutix.com', 'www.washingtonpavilion.org'],
                'status': 'active',
                'is_enabled': True,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 1000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': False,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'pricing_info_extraction': True,
                    'seat_mapping': True,
                    'zone_level_analysis': True,
                    'required_fields': ['venue_info', 'zones', 'levels']
                }
            },
            {
                'name': 'vividseats_scraper_v1',
                'display_name': 'VividSeats Scraper',
                'description': 'Scrapes event data from VividSeats including ticket pricing, seat availability, and venue information.',
                'target_website': 'https://www.vividseats.com',
                'target_domains': ['vividseats.com', 'www.vividseats.com'],
                'status': 'active',
                'is_enabled': True,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'ticket_data_extraction': True,
                    'seat_availability_tracking': True,
                    'price_tracking': True,
                    'venue_mapping': True,
                    'required_fields': ['venue_info', 'event_info', 'zones', 'seats']
                }
            },
            {
                'name': 'tpac_scraper_v1',
                'display_name': 'TPAC Scraper',
                'description': 'Scrapes event data from TPAC (Tennessee Performing Arts Center) including performance schedules, seat availability, pricing zones, and venue information.',
                'target_website': 'https://cart.tpac.org',
                'target_domains': ['cart.tpac.org', 'tpac.org', 'www.tpac.org'],
                'status': 'active',
                'is_enabled': False,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'performance_data_extraction': True,
                    'screen_based_sections': True,
                    'zone_pricing_analysis': True,
                    'consecutive_seat_grouping': True,
                    'required_fields': ['venue_info', 'zones', 'levels']
                }
            },
            {
                'name': 'demo_scraper_v1',
                'display_name': 'Demo Scraper',
                'description': 'Scrapes event data from a local Vite preview website for testing purposes.',
                'target_website': 'http://172.18.0.1:4173',
                'target_domains': ['172.18.0.1:4173', '127.0.0.1:4173', 'localhost:4173', '172.18.0.1', '127.0.0.1', 'localhost',"frontend-demo-scrapper:4173"],
                'status': 'active',
                'is_enabled': False,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'none',
                'optimization_enabled': False,
                'timeout_seconds': 30,
                'retry_attempts': 1,
                'retry_delay_seconds': 1,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 100,
                'headless_mode': False,
                'viewport_width': 1280,
                'viewport_height': 720,
                'enable_screenshots': False,
                'enable_detailed_logging': True,
                'log_level': 'DEBUG',
                'can_be_scheduled': False,
                'schedule_interval_hours': 0,
                'custom_settings': {
                    'performance_info_extraction': True,
                    'meta_data_extraction': True,
                    'seats_data_extraction': True,
                    'required_fields': ['performance_info', 'seats_info']
                }
            },
            {
                'name': 'broadway_sf_scraper_v5',
                'display_name': 'Broadway in Detroit Scraper',
                'description': 'Scrapes event data from Broadway in Detroit theater including performance schedules, seat availability, and pricing information using the Broadway SF scraper.',
                'target_website': 'https://www.broadwayindetroit.com',
                'target_domains': ['broadwayindetroit.com', 'www.broadwayindetroit.com', 'boltapi.broadwayindetroit.com', 'calendar-service.core.platform.atgtickets.com'],
                'status': 'active',
                'is_enabled': True,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'calendar_scraping': True,
                    'seating_chart_analysis': True,
                    'price_tracking': True,
                    'source_id': 'AV_US_EAST',
                    'required_fields': ['venue_info', 'event_info']
                }
            },
            {
                'name': 'broadway_sf_scraper_v5',
                'display_name': 'Saenger NOLA Scraper',
                'description': 'Scrapes event data from Saenger Theatre New Orleans including performance schedules, seat availability, and pricing information using the Broadway SF scraper.',
                'target_website': 'https://www.saengernola.com',
                'target_domains': ['saengernola.com', 'www.saengernola.com', 'boltapi.saengernola.com', 'calendar-service.core.platform.atgtickets.com'],
                'status': 'active',
                'is_enabled': False,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'calendar_scraping': True,
                    'seating_chart_analysis': True,
                    'price_tracking': True,
                    'source_id': 'AV_US_CENTRAL',
                    'venue_slug': 'saenger-theatre',
                    'required_fields': ['venue_info', 'event_info']
                }
            },
            {
                'name': 'broadway_sf_scraper_v5',
                'display_name': 'Kings Theatre Brooklyn Scraper',
                'description': 'Scrapes event data from Kings Theatre Brooklyn including performance schedules, seat availability, and pricing information using the Broadway SF scraper.',
                'target_website': 'https://www.kingstheatre.com',
                'target_domains': ['kingstheatre.com', 'www.kingstheatre.com', 'boltapi.kingstheatre.com', 'calendar-service.core.platform.atgtickets.com'],
                'status': 'active',
                'is_enabled': False,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'calendar_scraping': True,
                    'seating_chart_analysis': True,
                    'price_tracking': True,
                    'source_id': 'AV_US_EAST',
                    'venue_slug': 'kings-theatre-brooklyn',
                    'required_fields': ['venue_info', 'event_info']
                }
            },
            {
                'name': 'broadway_sf_scraper_v5',
                'display_name': 'Emerson Colonial Theatre Scraper',
                'description': 'Scrapes event data from Emerson Colonial Theatre including performance schedules, seat availability, and pricing information using the Broadway SF scraper.',
                'target_website': 'https://www.emersoncolonialtheatre.com',
                'target_domains': ['emersoncolonialtheatre.com', 'www.emersoncolonialtheatre.com', 'boltapi.emersoncolonialtheatre.com', 'calendar-service.core.platform.atgtickets.com'],
                'status': 'active',
                'is_enabled': False,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'calendar_scraping': True,
                    'seating_chart_analysis': True,
                    'price_tracking': True,
                    'source_id': 'AV_US_EAST',
                    'required_fields': ['venue_info', 'event_info']
                }
            },
            {
                'name': 'broadway_sf_scraper_v5',
                'display_name': 'Majestic Empire Scraper',
                'description': 'Scrapes event data from Majestic Empire including performance schedules, seat availability, and pricing information using the Broadway SF scraper.',
                'target_website': 'https://www.majesticempire.com',
                'target_domains': ['majesticempire.com', 'www.majesticempire.com', 'boltapi.majesticempire.com', 'calendar-service.core.platform.atgtickets.com'],
                'status': 'active',
                'is_enabled': False,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 2000,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'calendar_scraping': True,
                    'seating_chart_analysis': True,
                    'price_tracking': True,
                    'source_id': 'AV_US_EAST',
                    'required_fields': ['venue_info', 'event_info']
                }
            },
            {
                'name': 'colorado_ballet_scraper_v1',
                'display_name': 'Colorado Ballet Scraper',
                'description': 'Scrapes event data from Colorado Ballet including performance schedules, seat availability, and pricing information using API endpoints.',
                'target_website': 'https://tickets.coloradoballet.org',
                'target_domains': ['tickets.coloradoballet.org', 'coloradoballet.org'],
                'status': 'active',
                'is_enabled': False,
                'use_proxy': False,
                'fail_without_proxy': False,
                'optimization_level': 'balanced',
                'optimization_enabled': True,
                'timeout_seconds': 60,
                'retry_attempts': 3,
                'retry_delay_seconds': 5,
                'max_concurrent_jobs': 1,
                'delay_between_requests_ms': 1500,
                'headless_mode': True,
                'viewport_width': 1920,
                'viewport_height': 1080,
                'enable_screenshots': False,
                'enable_detailed_logging': False,
                'log_level': 'INFO',
                'can_be_scheduled': True,
                'schedule_interval_hours': 24,
                'custom_settings': {
                    'api_based_scraping': True,
                    'seat_mapping': True,
                    'zone_pricing_analysis': True,
                    'performance_data_extraction': True,
                    'required_fields': ['venue_info', 'zones', 'levels']
                }
            }
        ]

        created_count = 0
        updated_count = 0
        skipped_count = 0

        for scraper_config in scrapers_config:
            scraper_name = scraper_config['display_name']
            
            if options['dry_run']:
                self.stdout.write(
                    self.style.WARNING(f"DRY RUN: Would process scraper '{scraper_name}'")
                )
                continue

            try:
                # Check if scraper already exists
                existing_scraper = None
                try:
                    existing_scraper = ScraperDefinition.objects.get(display_name=scraper_name)
                except ScraperDefinition.DoesNotExist:
                    pass

                if existing_scraper and not options['force']:
                    self.stdout.write(
                        self.style.WARNING(f"Scraper '{scraper_name}' already exists. Use --force to update.")
                    )
                    skipped_count += 1
                    continue

                if existing_scraper and options['force']:
                    # Update existing scraper
                    for key, value in scraper_config.items():
                        if key != 'name':  # Don't update the name
                            setattr(existing_scraper, key, value)
                    existing_scraper.updated_at = timezone.now()
                    existing_scraper.save()
                    
                    self.stdout.write(
                        self.style.SUCCESS(f"Updated scraper definition: {scraper_name}")
                    )
                    updated_count += 1
                else:
                    # Create new scraper
                    scraper = ScraperDefinition.objects.create(**scraper_config)
                    
                    self.stdout.write(
                        self.style.SUCCESS(f"Created scraper definition: {scraper_name}")
                    )
                    created_count += 1

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"Failed to process scraper '{scraper_name}': {e}")
                )
                logger.error(f"Failed to process scraper '{scraper_name}': {e}")
                continue

        # Summary
        if options['dry_run']:
            self.stdout.write(
                self.style.WARNING(f"DRY RUN COMPLETE: Would process {len(scrapers_config)} scrapers")
            )
        else:
            total_processed = created_count + updated_count
            self.stdout.write("\n" + "="*60)
            self.stdout.write(self.style.SUCCESS("SCRAPER REGISTRATION COMPLETE"))
            self.stdout.write("="*60)
            self.stdout.write(f"Created: {created_count}")
            self.stdout.write(f"Updated: {updated_count}")
            self.stdout.write(f"Skipped: {skipped_count}")
            self.stdout.write(f"Total processed: {total_processed}")
            
            if total_processed > 0:
                self.stdout.write(
                    self.style.SUCCESS(f"\n✅ Successfully registered {total_processed} scrapers in the database!")
                )
                self.stdout.write(
                    self.style.WARNING("💡 Next steps:")
                )
                self.stdout.write("   1. Run: python manage.py setup_proxy_providers")
                self.stdout.write("   2. Run: python manage.py assign_scraper_proxy")
                self.stdout.write("   3. Access Django admin to configure scrapers")
            else:
                self.stdout.write(
                    self.style.WARNING("No scrapers were processed. Use --force to update existing scrapers.")
                )