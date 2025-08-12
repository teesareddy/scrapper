"""
Management command for bulk POS synchronization of seat packs.

This command syncs all active seat packs to POS when the system comes online.
It can sync all performances or specific ones based on command line arguments.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from scrapers.core.pos_bulk_sync_service import POSBulkSyncService, sync_all_to_pos, get_pos_sync_status
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Bulk synchronize seat packs to POS system when it comes online'

    def add_arguments(self, parser):
        parser.add_argument(
            '--performance-id',
            type=str,
            help='Sync a specific performance ID (internal_performance_id)'
        )
        
        parser.add_argument(
            '--performance-ids',
            nargs='+',
            type=str,
            help='Sync multiple specific performance IDs'
        )
        
        parser.add_argument(
            '--source-website',
            type=str,
            default='stubhub',
            help='Source website to sync (default: stubhub)'
        )
        
        parser.add_argument(
            '--status-only',
            action='store_true',
            help='Only show sync status without performing sync'
        )
        
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be synced without actually syncing'
        )
        
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output'
        )

    def handle(self, *args, **options):
        """Handle the management command execution."""
        
        source_website = options['source_website']
        verbose = options['verbose']
        
        if verbose:
            logger.setLevel(logging.DEBUG)
            
        self.stdout.write(f"POS Bulk Sync Command - Source: {source_website}")
        self.stdout.write("=" * 50)
        
        # Initialize the service
        sync_service = POSBulkSyncService(source_website)
        
        # Handle status-only option
        if options['status_only']:
            self._show_status(sync_service, options)
            return
            
        # Handle dry-run option
        if options['dry_run']:
            self._show_dry_run(sync_service, options)
            return
            
        # Perform actual sync
        self._perform_sync(sync_service, options)
    
    def _show_status(self, sync_service: POSBulkSyncService, options: dict):
        """Show current POS sync status."""
        self.stdout.write(self.style.SUCCESS("Current POS Sync Status:"))
        
        performance_id = options.get('performance_id')
        
        if performance_id:
            # Single performance status
            status = sync_service.get_sync_status(performance_id)
            self.stdout.write(f"Performance {performance_id}:")
            self.stdout.write(f"  Total Active Packs: {status['total_active_packs']}")
            self.stdout.write(f"  Packs Needing Sync: {status['packs_needing_sync']}")
            self.stdout.write(f"  Packs Already Synced: {status['packs_synced']}")
            self.stdout.write(f"  Sync Coverage: {status['sync_coverage']:.1f}%")
        else:
            # Overall status
            status = sync_service.get_sync_status()
            self.stdout.write("Overall Status:")
            self.stdout.write(f"  Total Performances: {status['total_performances']}")
            self.stdout.write(f"  Performances Needing Sync: {status['performances_needing_sync']}")
            self.stdout.write(f"  Performances Synced: {status['performances_synced']}")
            self.stdout.write(f"  Performance Sync Coverage: {status['performance_sync_coverage']:.1f}%")
    
    def _show_dry_run(self, sync_service: POSBulkSyncService, options: dict):
        """Show what would be synced without actually syncing."""
        self.stdout.write(self.style.WARNING("DRY RUN - No actual sync will be performed"))
        
        performance_id = options.get('performance_id')
        performance_ids = options.get('performance_ids')
        
        if performance_id:
            # Single performance dry run
            from scrapers.core.seat_pack_sync import get_seat_packs_needing_pos_sync
            packs = get_seat_packs_needing_pos_sync(performance_id, sync_service.source_website)
            
            self.stdout.write(f"Performance {performance_id}:")
            self.stdout.write(f"  Would sync {len(packs)} seat packs to POS")
            self.stdout.write(f"  Criteria: pack_status='active', pos_status in ['pending', 'delisted'], no pos_listing")
            
            if options['verbose'] and packs:
                self.stdout.write("  Pack details (first 10):")
                for pack in packs[:10]:  # Show first 10
                    self.stdout.write(f"    - {pack.internal_pack_id} (Row {pack.row_label}, Size {pack.pack_size}, POS Status: {pack.pos_status})")
                if len(packs) > 10:
                    self.stdout.write(f"    ... and {len(packs) - 10} more")
                    
        elif performance_ids:
            # Multiple performances dry run
            total_to_sync = 0
            for perf_id in performance_ids:
                from scrapers.core.seat_pack_sync import get_seat_packs_needing_pos_sync
                packs = get_seat_packs_needing_pos_sync(perf_id, sync_service.source_website)
                total_to_sync += len(packs)
                self.stdout.write(f"Performance {perf_id}: {len(packs)} packs needing sync")
            
            self.stdout.write(f"Total would sync {total_to_sync} seat packs across {len(performance_ids)} performances")
            self.stdout.write(f"Using updated filtering: active packs with pending/delisted POS status")
        else:
            # All performances dry run
            performances_needing_sync = sync_service._get_performances_needing_sync()
            self.stdout.write(f"Would sync {len(performances_needing_sync)} performances")
            self.stdout.write(f"Using new 4-dimensional model filtering for accurate pack selection")
            
            if options['verbose'] and performances_needing_sync:
                self.stdout.write("Performance breakdown (first 5):")
                for perf_id in performances_needing_sync[:5]:  # Show first 5
                    from scrapers.core.seat_pack_sync import get_seat_packs_needing_pos_sync
                    packs = get_seat_packs_needing_pos_sync(perf_id, sync_service.source_website)
                    self.stdout.write(f"  - {perf_id}: {len(packs)} packs (active with pending/delisted POS status)")
                if len(performances_needing_sync) > 5:
                    self.stdout.write(f"  ... and {len(performances_needing_sync) - 5} more performances")
    
    def _perform_sync(self, sync_service: POSBulkSyncService, options: dict):
        """Perform the actual POS sync."""
        performance_id = options.get('performance_id')
        performance_ids = options.get('performance_ids')
        
        try:
            if performance_id:
                # Single performance sync
                self.stdout.write(f"Syncing performance {performance_id} to POS...")
                
                with transaction.atomic():
                    result = sync_service.sync_single_performance(performance_id)
                
                self._display_sync_results(result, single_performance=True)
                
            elif performance_ids:
                # Multiple specific performances sync
                self.stdout.write(f"Syncing {len(performance_ids)} performances to POS...")
                
                with transaction.atomic():
                    result = sync_service.sync_all_performances(performance_ids)
                
                self._display_sync_results(result, single_performance=False)
                
            else:
                # All performances sync
                self.stdout.write("Syncing ALL performances to POS...")
                self.stdout.write(self.style.WARNING("This may take a while for large datasets..."))
                
                # Use the high-level function for complete sync
                result = sync_all_to_pos(sync_service.source_website)
                
                self._display_sync_results(result, single_performance=False)
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Sync failed: {str(e)}")
            )
            raise CommandError(f"POS sync failed: {str(e)}")
    
    def _display_sync_results(self, result: dict, single_performance: bool = False):
        """Display sync results in a formatted way."""
        if single_performance:
            # Single performance results
            if result['synced'] > 0:
                self.stdout.write(
                    self.style.SUCCESS(f"✓ Successfully synced {result['synced']} seat packs to POS")
                )
            
            if result['failed'] > 0:
                self.stdout.write(
                    self.style.WARNING(f"⚠ Failed to sync {result['failed']} seat packs")
                )
                
                if result.get('errors'):
                    self.stdout.write("Errors:")
                    for error in result['errors']:
                        self.stdout.write(f"  - {error.get('pack_id', 'Unknown')}: {error.get('error', 'Unknown error')}")
        else:
            # Multiple performances results
            self.stdout.write("Sync Results:")
            self.stdout.write(f"  Performances Processed: {result.get('performances_processed', 0)}")
            self.stdout.write(f"  Performances Failed: {result.get('performances_failed', 0)}")
            self.stdout.write(f"  Total Packs Synced: {result.get('total_synced', 0)}")
            self.stdout.write(f"  Total Packs Failed: {result.get('total_failed', 0)}")
            
            if result.get('total_synced', 0) > 0:
                self.stdout.write(
                    self.style.SUCCESS(f"✓ Successfully synced {result['total_synced']} seat packs to POS")
                )
            
            if result.get('total_failed', 0) > 0:
                self.stdout.write(
                    self.style.WARNING(f"⚠ Failed to sync {result['total_failed']} seat packs")
                )
            
            if result.get('errors'):
                error_count = len(result['errors'])
                self.stdout.write(f"Errors ({error_count} total):")
                # Show first few errors
                for error in result['errors'][:5]:
                    self.stdout.write(f"  - {error}")
                if error_count > 5:
                    self.stdout.write(f"  ... and {error_count - 5} more errors")
        
        self.stdout.write("\nSync completed!")