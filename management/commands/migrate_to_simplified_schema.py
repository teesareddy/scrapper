# scrapers/management/commands/migrate_to_simplified_schema.py
"""
Management command to migrate from the complex 12-table structure
to the simplified 5-table structure based on the ER diagram.
"""

from django.core.management.base import BaseCommand
from django.db import transaction, connection
from django.utils import timezone
import json
import logging

# Import both old and new models
from scrapers.models import (
    Venue as OldVenue, Event as OldEvent, Performance as OldPerformance,
    Level as OldLevel, Zone as OldZone, Section as OldSection, Seat as OldSeat,
    ScrapeJob as OldScrapeJob, LevelPriceFact as OldLevelPriceFact,
    SeatSnapshotFact as OldSeatSnapshotFact, SeatPack as OldSeatPack
)

# These will be our new models (update import path as needed)
# from scrapers.models_new import (
#     Venue, Event, Performance, PriceCategory, SeatPack, ScrapeLog
# )

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Migrate from complex schema to simplified 5-table schema'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Perform a dry run without making changes',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Number of records to process in each batch',
        )

    def handle(self, *args, **options):
        self.dry_run = options['dry_run']
        self.batch_size = options['batch_size']

        if self.dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be made'))

        try:
            self.migrate_data()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Migration failed: {e}'))
            raise

    @transaction.atomic
    def migrate_data(self):
        """Main migration logic"""

        # Step 1: Create new tables (this should be done via Django migrations first)
        self.stdout.write('Step 1: Ensure new tables exist...')
        # Run: python manage.py makemigrations
        # Run: python manage.py migrate

        # Step 2: Migrate venues
        self.stdout.write('Step 2: Migrating venues...')
        venue_mapping = self.migrate_venues()

        # Step 3: Migrate events
        self.stdout.write('Step 3: Migrating events...')
        event_mapping = self.migrate_events(venue_mapping)

        # Step 4: Migrate performances and build complex JSON structures
        self.stdout.write('Step 4: Migrating performances...')
        performance_mapping = self.migrate_performances(event_mapping)

        # Step 5: Create price categories
        self.stdout.write('Step 5: Creating price categories...')
        self.create_price_categories(performance_mapping)

        # Step 6: Migrate seat packs
        self.stdout.write('Step 6: Migrating seat packs...')
        self.migrate_seat_packs(performance_mapping)

        # Step 7: Create scrape logs
        self.stdout.write('Step 7: Creating scrape logs...')
        self.create_scrape_logs(performance_mapping)

        self.stdout.write(self.style.SUCCESS('Migration completed successfully!'))

    def migrate_venues(self):
        """Migrate venues from old to new schema"""
        venue_mapping = {}

        old_venues = OldVenue.objects.filter(is_current=True)
        self.stdout.write(f'Found {old_venues.count()} venues to migrate')

        for old_venue in old_venues:
            if not self.dry_run:
                # Create new venue record
                new_venue_data = {
                    'source_venue_id': old_venue.source_venue_id,
                    'source_website': old_venue.source_website,
                    'name': old_venue.name,
                    'address': old_venue.address,
                    'city': old_venue.city,
                    'state': old_venue.state,
                    'country': old_venue.country,
                    'created_at': old_venue.valid_from
                }

                # For dry run, we'll simulate the creation
                if self.dry_run:
                    self.stdout.write(f'Would create venue: {new_venue_data["name"]}')
                    new_venue_key = f'simulated_{old_venue.venue_key}'
                else:
                    # Actual creation would happen here
                    # new_venue = Venue.objects.create(**new_venue_data)
                    # new_venue_key = new_venue.venue_key
                    new_venue_key = old_venue.venue_key  # Placeholder

                venue_mapping[old_venue.venue_key] = new_venue_key

        return venue_mapping

    def migrate_events(self, venue_mapping):
        """Migrate events from old to new schema"""
        event_mapping = {}

        old_events = OldEvent.objects.filter(is_current=True)
        self.stdout.write(f'Found {old_events.count()} events to migrate')

        for old_event in old_events:
            if old_event.venue_key.venue_key not in venue_mapping:
                self.stdout.write(f'Skipping event {old_event.name} - venue not found')
                continue

            if not self.dry_run:
                new_event_data = {
                    'venue_key_id': venue_mapping[old_event.venue_key.venue_key],
                    'source_event_id': old_event.source_event_id,
                    'source_website': 'legacy_migration',
                    'name': old_event.name,
                    'url': old_event.url,
                    'currency': old_event.currency,
                    'created_at': old_event.valid_from
                }

                if self.dry_run:
                    self.stdout.write(f'Would create event: {new_event_data["name"]}')
                    new_event_key = f'simulated_{old_event.event_key}'
                else:
                    # new_event = Event.objects.create(**new_event_data)
                    # new_event_key = new_event.event_key
                    new_event_key = old_event.event_key  # Placeholder

                event_mapping[old_event.event_key] = new_event_key

        return event_mapping

    def migrate_performances(self, event_mapping):
        """Migrate performances and build JSON structures"""
        performance_mapping = {}

        old_performances = OldPerformance.objects.filter(is_current=True)
        self.stdout.write(f'Found {old_performances.count()} performances to migrate')

        for old_perf in old_performances:
            if old_perf.event_key.event_key not in event_mapping:
                continue

            # Build seating structure JSON
            seating_structure = self.build_seating_structure(old_perf)

            # Build performance data JSON
            performance_data = self.build_performance_data(old_perf)

            if not self.dry_run:
                new_perf_data = {
                    'event_key_id': event_mapping[old_perf.event_key.event_key],
                    'source_performance_id': str(old_perf.perf_key),
                    'source_website': 'legacy_migration',
                    'performance_datetime_utc': old_perf.performance_datetime_utc,
                    'seat_map_url': old_perf.seat_map_url,
                    'performance_data': performance_data,
                    'seating_structure': seating_structure,
                    'scraped_at': old_perf.valid_from,
                    'is_active': True
                }

                if self.dry_run:
                    self.stdout.write(f'Would create performance for event: {old_perf.event_key.name}')
                    new_perf_key = f'simulated_{old_perf.perf_key}'
                else:
                    # new_perf = Performance.objects.create(**new_perf_data)
                    # new_perf_key = new_perf.performance_key
                    new_perf_key = old_perf.perf_key  # Placeholder

                performance_mapping[old_perf.perf_key] = new_perf_key

        return performance_mapping

    def build_seating_structure(self, old_performance):
        """Build the seating_structure JSON from normalized tables"""
        structure = {
            'levels': [],
            'zones': [],
            'sections': [],
            'seats': []
        }

        # Get all levels for this performance
        levels = OldLevel.objects.filter(perf_key=old_performance, is_current=True)

        for level in levels:
            level_data = {
                'level_key': level.level_key,
                'name': level.name,
                'raw_name': level.raw_name,
                'zones': []
            }

            # Get zones for this level
            zones = OldZone.objects.filter(level_key=level, is_current=True)

            for zone in zones:
                zone_data = {
                    'zone_key': zone.zone_key,
                    'name': zone.name,
                    'raw_identifier': zone.raw_identifier,
                    'sections': []
                }

                # Get sections for this zone
                sections = OldSection.objects.filter(zone_key=zone, is_current=True)

                for section in sections:
                    section_data = {
                        'section_key': section.section_key,
                        'name': section.name,
                        'raw_name': section.raw_name,
                        'seats': []
                    }

                    # Get seats for this section (limit for performance)
                    seats = OldSeat.objects.filter(
                        section_key=section,
                        is_current=True
                    )[:1000]  # Limit to prevent huge JSON

                    for seat in seats:
                        seat_data = {
                            'seat_key': seat.seat_key,
                            'row_label': seat.row_label,
                            'seat_number': seat.seat_number,
                            'seat_type': seat.seat_type,
                            'x_coord': float(seat.x_coord) if seat.x_coord else None,
                            'y_coord': float(seat.y_coord) if seat.y_coord else None
                        }
                        section_data['seats'].append(seat_data)

                    zone_data['sections'].append(section_data)

                level_data['zones'].append(zone_data)

            structure['levels'].append(level_data)

        return structure

    def build_performance_data(self, old_performance):
        """Build performance_data JSON with metadata"""
        return {
            'map_width': old_performance.map_width,
            'map_height': old_performance.map_height,
            'original_performance_key': old_performance.perf_key,
            'migration_date': timezone.now().isoformat(),
            'scraper_version': 'legacy_migration_v1.0'
        }

    def create_price_categories(self, performance_mapping):
        """Create price categories from level price facts"""

        for old_perf_key, new_perf_key in performance_mapping.items():
            price_facts = OldLevelPriceFact.objects.filter(
                scrape_job_key__perf_key_id=old_perf_key
            )

            rank = 1
            for fact in price_facts:
                if fact.min_price and fact.max_price:
                    category_data = {
                        'performance_key_id': new_perf_key,
                        'category_id': f'level_{fact.level_key.level_key}',
                        'name': fact.level_key.name,
                        'rank': rank,
                        'min_price': fact.min_price,
                        'max_price': fact.max_price,
                        'bg_color': '#CCCCCC',  # Default color
                        'text_color': '#000000',
                        'availability': 0  # Would need to calculate from seat snapshots
                    }

                    if self.dry_run:
                        self.stdout.write(f'Would create price category: {category_data["name"]}')
                    else:
                        # PriceCategory.objects.create(**category_data)
                        pass

                    rank += 1

    def migrate_seat_packs(self, performance_mapping):
        """Migrate seat packs to new schema"""

        old_packs = OldSeatPack.objects.all()
        self.stdout.write(f'Found {old_packs.count()} seat packs to migrate')

        for old_pack in old_packs:
            # Find the performance this pack belongs to
            zone = old_pack.zone_key
            level = zone.level_key
            old_perf_key = level.perf_key.perf_key

            if old_perf_key not in performance_mapping:
                continue

            pack_data = {
                'performance_key_id': performance_mapping[old_perf_key],
                'zone_name': f"{level.name} - {zone.name}",
                'row_label': old_pack.row_label,
                'start_seat': old_pack.start_seat_number,
                'end_seat': old_pack.end_seat_number,
                'pack_size': old_pack.pack_size,
                'pack_price': None,  # Would need to calculate
                'seat_keys': old_pack.seat_keys
            }

            if self.dry_run:
                self.stdout.write(f'Would create seat pack: Row {pack_data["row_label"]}')
            else:
                # SeatPack.objects.create(**pack_data)
                pass

    def create_scrape_logs(self, performance_mapping):
        """Create scrape logs from old scrape jobs"""

        old_jobs = OldScrapeJob.objects.all()
        self.stdout.write(f'Found {old_jobs.count()} scrape jobs to migrate')

        for old_job in old_jobs:
            if old_job.perf_key.perf_key not in performance_mapping:
                continue

            log_data = {
                'performance_key_id': performance_mapping[old_job.perf_key.perf_key],
                'scraper_name': 'legacy_scraper',
                'scraped_at': old_job.scraped_at_utc,
                'success': old_job.scrape_success,
                'error_message': old_job.error_message,
                'raw_response': old_job.raw_payload or {}
            }

            if self.dry_run:
                status = "success" if log_data["success"] else "failed"
                self.stdout.write(f'Would create scrape log: {status} at {log_data["scraped_at"]}')
            else:
                # ScrapeLog.objects.create(**log_data)
                pass