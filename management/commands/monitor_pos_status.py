"""
Management command for POS status monitoring and automatic synchronization.

This command can be called by external systems or scheduled tasks to:
- Check POS status and automatically sync when needed
- Monitor specific performances
- Trigger manual sync operations
"""

from django.core.management.base import BaseCommand, CommandError
from scrapers.core.pos_status_monitor import POSStatusMonitor, check_and_sync_pos_status, get_pos_monitoring_status, trigger_performance_pos_sync
import logging
import json

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Monitor POS status and trigger automatic synchronization when needed'

    def add_arguments(self, parser):
        parser.add_argument(
            '--performance-id',
            type=str,
            help='Monitor and sync a specific performance ID'
        )
        
        parser.add_argument(
            '--source-website',
            type=str,
            default='stubhub',
            help='Source website to monitor (default: stubhub)'
        )
        
        parser.add_argument(
            '--status-only',
            action='store_true',
            help='Only show monitoring status without triggering sync'
        )
        
        parser.add_argument(
            '--force-sync',
            action='store_true',
            help='Force sync even if no changes detected'
        )
        
        parser.add_argument(
            '--json-output',
            action='store_true',
            help='Output results in JSON format'
        )
        
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output'
        )

    def handle(self, *args, **options):
        """Handle the management command execution."""
        
        source_website = options['source_website']
        performance_id = options.get('performance_id')
        verbose = options['verbose']
        json_output = options['json_output']
        
        if verbose:
            logger.setLevel(logging.DEBUG)
        
        if not json_output:
            self.stdout.write(f"POS Status Monitor - Source: {source_website}")
            self.stdout.write("=" * 50)
        
        try:
            if options['status_only']:
                # Just show status
                result = self._show_status(source_website, performance_id, json_output)
            elif performance_id:
                # Monitor specific performance
                result = self._monitor_performance(performance_id, source_website, json_output, options['force_sync'])
            else:
                # Monitor all performances and trigger sync if needed
                result = self._monitor_and_sync_all(source_website, json_output, options['force_sync'])
            
            if json_output:
                self.stdout.write(json.dumps(result, indent=2, default=str))
            
        except Exception as e:
            error_msg = f"POS monitoring failed: {str(e)}"
            if json_output:
                self.stdout.write(json.dumps({
                    'success': False,
                    'error': error_msg
                }, indent=2))
            else:
                self.stdout.write(self.style.ERROR(error_msg))
            raise CommandError(error_msg)
    
    def _show_status(self, source_website: str, performance_id: str = None, json_output: bool = False) -> dict:
        """Show current POS monitoring status."""
        if performance_id:
            # Single performance status
            monitor = POSStatusMonitor(source_website)
            sync_status = monitor.sync_service.get_sync_status(performance_id)
            
            if not json_output:
                self.stdout.write(self.style.SUCCESS(f"Performance {performance_id} Status:"))
                self.stdout.write(f"  Total Active Packs: {sync_status['total_active_packs']}")
                self.stdout.write(f"  Packs Needing Sync: {sync_status['packs_needing_sync']}")
                self.stdout.write(f"  Packs Already Synced: {sync_status['packs_synced']}")
                self.stdout.write(f"  Sync Coverage: {sync_status['sync_coverage']:.1f}%")
            
            return {
                'success': True,
                'performance_status': sync_status
            }
        else:
            # Overall monitoring status
            status = get_pos_monitoring_status(source_website)
            
            if not json_output:
                self.stdout.write(self.style.SUCCESS("Overall POS Monitoring Status:"))
                self.stdout.write(f"  Monitoring Enabled: {status['monitoring_enabled']}")
                self.stdout.write(f"  Total Performances: {status['total_performances']}")
                self.stdout.write(f"  Performances Needing Sync: {status['performances_needing_sync']}")
                self.stdout.write(f"  Performance Sync Coverage: {status['performance_sync_coverage']:.1f}%")
                self.stdout.write(f"  Total Active Packs: {status['total_active_packs']}")
                self.stdout.write(f"  Total Packs Needing Sync: {status['total_packs_needing_sync']}")
                self.stdout.write(f"  Pack Sync Coverage: {status['pack_sync_coverage']:.1f}%")
                self.stdout.write(f"  Last Check: {status['last_check']}")
            
            return {
                'success': True,
                'monitoring_status': status
            }
    
    def _monitor_performance(self, performance_id: str, source_website: str, json_output: bool = False, force_sync: bool = False) -> dict:
        """Monitor and sync a specific performance."""
        if not json_output:
            self.stdout.write(f"Monitoring performance {performance_id}...")
        
        result = trigger_performance_pos_sync(performance_id, source_website)
        
        if not json_output:
            if result['sync_triggered']:
                sync_results = result.get('sync_results', {})
                self.stdout.write(
                    self.style.SUCCESS(f"✓ Sync triggered for performance {performance_id}")
                )
                self.stdout.write(f"  Packs Synced: {sync_results.get('synced', 0)}")
                self.stdout.write(f"  Packs Failed: {sync_results.get('failed', 0)}")
                
                if sync_results.get('errors'):
                    self.stdout.write(f"  Errors: {len(sync_results['errors'])}")
                    for error in sync_results['errors'][:3]:  # Show first 3
                        self.stdout.write(f"    - {error}")
            else:
                self.stdout.write(
                    self.style.WARNING(f"⚠ {result['message']}")
                )
        
        return {
            'success': True,
            'performance_result': result
        }
    
    def _monitor_and_sync_all(self, source_website: str, json_output: bool = False, force_sync: bool = False) -> dict:
        """Monitor all performances and trigger sync if needed."""
        if not json_output:
            self.stdout.write("Monitoring all performances for POS sync needs...")
        
        result = check_and_sync_pos_status(source_website)
        
        if not json_output:
            self.stdout.write(f"Monitoring Status: {result['monitoring_status']}")
            self.stdout.write(f"Performances Checked: {result['performances_checked']}")
            
            if result['sync_triggered']:
                sync_results = result.get('sync_results', {})
                self.stdout.write(
                    self.style.SUCCESS(f"✓ Bulk sync completed")
                )
                self.stdout.write(f"  Performances Processed: {sync_results.get('performances_processed', 0)}")
                self.stdout.write(f"  Performances Failed: {sync_results.get('performances_failed', 0)}")
                self.stdout.write(f"  Total Packs Synced: {sync_results.get('total_synced', 0)}")
                self.stdout.write(f"  Total Packs Failed: {sync_results.get('total_failed', 0)}")
                
                if sync_results.get('errors'):
                    error_count = len(sync_results['errors'])
                    self.stdout.write(f"  Errors: {error_count}")
                    # Show sample errors
                    for error in sync_results['errors'][:3]:
                        if isinstance(error, dict):
                            self.stdout.write(f"    - {error.get('performance_id', 'Unknown')}: {error.get('error', 'Unknown error')}")
                        else:
                            self.stdout.write(f"    - {str(error)}")
                    if error_count > 3:
                        self.stdout.write(f"    ... and {error_count - 3} more errors")
            else:
                self.stdout.write(
                    self.style.WARNING(f"⚠ {result['message']}")
                )
        
        return {
            'success': True,
            'monitoring_result': result
        }