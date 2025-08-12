"""
Management command to process split pack admin holds

This command finds seat packs that have been split and applies
admin holds to their StubHub inventory to prevent double-selling.
"""

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import json
import logging

from ...services.split_pack_admin_hold_service import SplitPackAdminHoldService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Process split pack admin holds for StubHub inventory'

    def add_arguments(self, parser):
        parser.add_argument(
            '--source-website',
            type=str,
            help='Filter by source website (e.g. broadway_sf, washington_pavilion)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be processed without making changes'
        )
        parser.add_argument(
            '--stats-only',
            action='store_true',
            help='Only show statistics without processing'
        )
        parser.add_argument(
            '--stubhub-api-base-url',
            type=str,
            default='https://pointofsaleapi.stubhub.net',
            help='StubHub API base URL (default: production)'
        )

    def handle(self, *args, **options):
        source_website = options.get('source_website')
        dry_run = options.get('dry_run', False)
        stats_only = options.get('stats_only', False)
        stubhub_api_base_url = options.get('stubhub_api_base_url')
        
        # Initialize the service
        service = SplitPackAdminHoldService(stubhub_api_base_url)
        
        # Show statistics
        self.stdout.write(self.style.SUCCESS('=== Split Pack Admin Hold Processing ==='))
        
        stats = service.get_split_pack_statistics(source_website)
        self.stdout.write(f"Statistics for source_website: {stats['source_website'] or 'ALL'}")
        self.stdout.write(f"  Total split packs: {stats['total_split_packs']}")
        self.stdout.write(f"  Split packs with POS listing: {stats['split_packs_with_pos_listing']}")
        self.stdout.write(f"  Split packs with StubHub ID: {stats['split_packs_with_stubhub_id']}")
        self.stdout.write(f"  Already processed: {stats['processed_split_packs']}")
        self.stdout.write(f"  Admin holds applied: {stats['admin_holds_applied']}")
        self.stdout.write(f"  Pending processing: {stats['pending_split_packs']}")
        
        if stats_only:
            return
            
        if stats['pending_split_packs'] == 0:
            self.stdout.write(self.style.SUCCESS('No split packs need processing.'))
            return
            
        # Show what would be processed in dry run mode
        if dry_run:
            self.stdout.write(self.style.WARNING('=== DRY RUN MODE ==='))
            split_packs = service.find_split_packs(source_website)
            
            self.stdout.write(f"Would process {len(split_packs)} split packs:")
            for pack in split_packs:
                self.stdout.write(f"  Pack: {pack.internal_pack_id}")
                self.stdout.write(f"    StubHub ID: {pack.pos_listing.stubhub_inventory_id}")
                self.stdout.write(f"    POS Listing ID: {pack.pos_listing.pos_listing_id}")
                self.stdout.write(f"    Row: {pack.row_label}, Seats: {pack.start_seat_number}-{pack.end_seat_number}")
                self.stdout.write("")
                
            return
            
        # Process split packs
        self.stdout.write(self.style.SUCCESS('=== Processing Split Packs ==='))
        
        try:
            results = service.process_split_packs(source_website)
            
            # Display results
            self.stdout.write(f"Processing completed:")
            self.stdout.write(f"  Total split packs found: {results['total_split_packs']}")
            self.stdout.write(f"  Admin holds applied: {results['admin_holds_applied']}")
            self.stdout.write(f"  POS listings deactivated: {results['pos_listings_deactivated']}")
            self.stdout.write(f"  Errors: {results['errors']}")
            
            if results['processed_inventory_ids']:
                self.stdout.write(f"  Processed StubHub IDs: {', '.join(results['processed_inventory_ids'])}")
                
            if results['error_details']:
                self.stdout.write(self.style.ERROR('Error details:'))
                for error in results['error_details']:
                    self.stdout.write(f"  Pack {error['pack_id']} (Inventory {error['inventory_id']}): {error['error']}")
                    
            # Success message
            if results['errors'] == 0:
                self.stdout.write(self.style.SUCCESS('All split packs processed successfully!'))
            else:
                self.stdout.write(self.style.WARNING(f'Processing completed with {results["errors"]} errors'))
                
        except Exception as e:
            raise CommandError(f'Failed to process split packs: {str(e)}')