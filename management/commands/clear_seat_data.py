"""
Django management command to clear seat-related data for testing purposes.
This helps identify ghost seat packs by providing a clean slate before each scrape.
"""

import logging
from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings
from scrapers.models import Seat, SeatPack, SeatSnapshot, Performance
from scrapers.models.pos import POSListing

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Clear seat, seat pack, and seat snapshot data for testing ghost pack issues'

    def add_arguments(self, parser):
        parser.add_argument(
            '--performance-id',
            type=str,
            help='Clear data for specific performance (internal_performance_id)'
        )
        parser.add_argument(
            '--source-website',
            type=str,
            default='broadway_sf',
            help='Clear data for specific source website (default: broadway_sf)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting'
        )
        parser.add_argument(
            '--confirm',
            action='store_true',
            help='Confirm deletion (required for safety)'
        )

    def handle(self, *args, **options):
        performance_id = options.get('performance_id')
        source_website = options.get('source_website')
        dry_run = options.get('dry_run')
        confirm = options.get('confirm')

        # Safety check - prevent accidental production deletion
        if hasattr(settings, 'ENVIRONMENT') and settings.ENVIRONMENT == 'production':
            self.stdout.write(
                self.style.ERROR('❌ Cannot run in production environment!')
            )
            return

        if not confirm and not dry_run:
            self.stdout.write(
                self.style.ERROR('❌ Must use --confirm or --dry-run for safety')
            )
            return

        self.stdout.write(self.style.SUCCESS('🧹 Starting seat data cleanup...'))

        # Build filters
        seat_filters = {}
        seat_pack_filters = {}
        seat_snapshot_filters = {}

        if performance_id:
            try:
                performance = Performance.objects.get(internal_performance_id=performance_id)
                seat_filters['zone_id__performance_id'] = performance
                seat_pack_filters['zone_id__performance_id'] = performance
                seat_snapshot_filters['seat_id__zone_id__performance_id'] = performance
                self.stdout.write(f"🎯 Targeting performance: {performance_id}")
            except Performance.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f'❌ Performance not found: {performance_id}')
                )
                return

        if source_website:
            seat_filters['source_website'] = source_website
            seat_pack_filters['source_website'] = source_website
            # SeatSnapshot doesn't have source_website field, filter through seat
            seat_snapshot_filters['seat_id__source_website'] = source_website
            self.stdout.write(f"🌐 Targeting source website: {source_website}")

        # Get counts before deletion
        seat_count = Seat.objects.filter(**seat_filters).count()
        seat_pack_count = SeatPack.objects.filter(**seat_pack_filters).count()
        seat_snapshot_count = SeatSnapshot.objects.filter(**seat_snapshot_filters).count()

        # Also count related POS listings
        pos_listing_count = 0
        if seat_pack_count > 0:
            pos_listing_count = POSListing.objects.filter(
                seat_packs__in=SeatPack.objects.filter(**seat_pack_filters)
            ).count()

        # Display what will be deleted
        self.stdout.write(self.style.WARNING('📊 Data to be cleared:'))
        self.stdout.write(f"   • Seats: {seat_count}")
        self.stdout.write(f"   • Seat Packs: {seat_pack_count}")
        self.stdout.write(f"   • Seat Snapshots: {seat_snapshot_count}")
        self.stdout.write(f"   • Related POS Listings: {pos_listing_count}")

        if dry_run:
            self.stdout.write(self.style.SUCCESS('🔍 Dry run completed - no data deleted'))
            return

        if seat_count == 0 and seat_pack_count == 0 and seat_snapshot_count == 0:
            self.stdout.write(self.style.SUCCESS('✅ No data to clear'))
            return

        # Perform deletion in transaction
        try:
            with transaction.atomic():
                # Delete in correct order to avoid foreign key constraints
                
                # 1. Clear POS listings first (they reference seat packs)
                if pos_listing_count > 0:
                    deleted_pos = POSListing.objects.filter(
                        seat_packs__in=SeatPack.objects.filter(**seat_pack_filters)
                    ).delete()
                    self.stdout.write(f"🗑️  Deleted {deleted_pos[0]} POS listings")

                # 2. Clear seat snapshots (they reference seats)
                if seat_snapshot_count > 0:
                    deleted_snapshots = SeatSnapshot.objects.filter(**seat_snapshot_filters).delete()
                    self.stdout.write(f"🗑️  Deleted {deleted_snapshots[0]} seat snapshots")

                # 3. Clear seat packs (they reference zones/seats)
                if seat_pack_count > 0:
                    deleted_packs = SeatPack.objects.filter(**seat_pack_filters).delete()
                    self.stdout.write(f"🗑️  Deleted {deleted_packs[0]} seat packs")

                # 4. Clear seats last
                if seat_count > 0:
                    deleted_seats = Seat.objects.filter(**seat_filters).delete()
                    self.stdout.write(f"🗑️  Deleted {deleted_seats[0]} seats")

                self.stdout.write(self.style.SUCCESS('✅ Seat data cleanup completed successfully'))
                
                # Log for debugging
                logger.info(f"Cleared seat data - Seats: {seat_count}, Packs: {seat_pack_count}, "
                           f"Snapshots: {seat_snapshot_count}, POS: {pos_listing_count}")

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Error during cleanup: {str(e)}')
            )
            logger.error(f"Seat data cleanup failed: {str(e)}", exc_info=True)
            raise

        self.stdout.write(self.style.SUCCESS('🎉 Ready for fresh scrape testing!'))