"""
POS Status Monitor Service

This service monitors POS status changes and automatically triggers bulk synchronization
when POS comes online for performances that have active seat packs needing sync.
"""

import logging
from typing import Dict, List, Optional, Any, Set
from django.db import transaction
from django.utils import timezone
from django.conf import settings
from ..models.seat_packs import SeatPack
from ..models.base import Performance
from .pos_bulk_sync_service import POSBulkSyncService, sync_all_to_pos
from .seat_pack_sync import get_seat_packs_needing_pos_sync
from consumer.rabbitmq_producer import producer

logger = logging.getLogger(__name__)


class POSStatusMonitor:
    """
    Monitors POS status changes and triggers automatic synchronization.
    """
    
    def __init__(self, source_website: str = 'stubhub'):
        """
        Initialize the POS status monitor.
        
        Args:
            source_website: Source website identifier
        """
        self.source_website = source_website
        self.sync_service = POSBulkSyncService(source_website)
        
    def check_pos_status_and_sync(self) -> Dict[str, Any]:
        """
        Check for performances that have POS enabled and seat packs needing sync,
        then trigger automatic synchronization.
        
        Returns:
            Dictionary with monitoring and sync results
        """
        logger.info("Starting POS status check and automatic sync")
        
        try:
            # Get all performances that need POS sync
            performances_needing_sync = self._get_performances_needing_pos_sync()
            
            if not performances_needing_sync:
                logger.info("No performances found needing POS sync")
                return {
                    'monitoring_status': 'completed',
                    'performances_checked': 0,
                    'performances_synced': 0,
                    'sync_triggered': False,
                    'message': 'No performances need POS sync'
                }
            
            logger.info(f"Found {len(performances_needing_sync)} performances needing POS sync")
            
            # Check if POS integration is enabled
            if not self._is_pos_integration_enabled():
                logger.warning("POS integration not enabled, skipping sync")
                return {
                    'monitoring_status': 'skipped',
                    'performances_checked': len(performances_needing_sync),
                    'performances_synced': 0,
                    'sync_triggered': False,
                    'message': 'POS integration not configured'
                }
            
            # Trigger bulk sync for all performances needing it
            sync_results = self.sync_service.sync_all_performances(performances_needing_sync)
            
            # Send notification about bulk sync completion
            self._send_bulk_sync_completion_message(sync_results, performances_needing_sync)
            
            return {
                'monitoring_status': 'completed',
                'performances_checked': len(performances_needing_sync),
                'performances_synced': sync_results.get('performances_processed', 0),
                'sync_triggered': True,
                'sync_results': sync_results,
                'message': f"Synced {sync_results.get('performances_processed', 0)} performances"
            }
            
        except Exception as e:
            error_msg = f"Error during POS status monitoring and sync: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            return {
                'monitoring_status': 'error',
                'performances_checked': 0,
                'performances_synced': 0,
                'sync_triggered': False,
                'error': error_msg,
                'message': 'POS monitoring failed'
            }
    
    def trigger_performance_sync(self, performance_id: str) -> Dict[str, Any]:
        """
        Trigger POS sync for a specific performance.
        
        Args:
            performance_id: Internal performance ID to sync
            
        Returns:
            Dictionary with sync results
        """
        logger.info(f"Triggering POS sync for performance {performance_id}")
        
        try:
            # Check if performance needs sync
            packs_needing_sync = get_seat_packs_needing_pos_sync(performance_id, self.source_website)
            
            if not packs_needing_sync:
                logger.info(f"Performance {performance_id} has no packs needing sync")
                return {
                    'performance_id': performance_id,
                    'sync_triggered': False,
                    'message': 'No seat packs need sync for this performance'
                }
            
            # Check if POS integration is enabled
            if not self._is_pos_integration_enabled():
                logger.warning("POS integration not enabled, skipping sync")
                return {
                    'performance_id': performance_id,
                    'sync_triggered': False,
                    'message': 'POS integration not configured'
                }
            
            # Trigger sync for this performance
            sync_result = self.sync_service.sync_single_performance(performance_id)
            
            return {
                'performance_id': performance_id,
                'sync_triggered': True,
                'sync_results': sync_result,
                'message': f"Synced {sync_result.get('synced', 0)} packs, {sync_result.get('failed', 0)} failed"
            }
            
        except Exception as e:
            error_msg = f"Error triggering sync for performance {performance_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            return {
                'performance_id': performance_id,
                'sync_triggered': False,
                'error': error_msg,
                'message': 'Sync trigger failed'
            }
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """
        Get current monitoring status and statistics.
        
        Returns:
            Dictionary with monitoring status information
        """
        try:
            performances_needing_sync = self._get_performances_needing_pos_sync()
            
            # Get total counts for status reporting
            total_active_performances = Performance.objects.filter(
                seat_packs__source_website=self.source_website,
                seat_packs__pack_status='active'
            ).distinct().count()
            
            # Calculate overall sync statistics
            total_packs_needing_sync = SeatPack.objects.filter(
                source_website=self.source_website,
                pack_status='active',
                pos_status__in=['pending', 'delisted'],
                pack_state__in=['create', 'split', 'merge', 'shrink'],
                pos_listing__isnull=True
            ).count()
            
            total_active_packs = SeatPack.objects.filter(
                source_website=self.source_website,
                pack_status='active',
                pack_state__in=['create', 'split', 'merge', 'shrink'],
                delist_reason__isnull=True
            ).count()
            
            return {
                'monitoring_enabled': self._is_pos_integration_enabled(),
                'total_performances': total_active_performances,
                'performances_needing_sync': len(performances_needing_sync),
                'performances_synced': total_active_performances - len(performances_needing_sync),
                'performance_sync_coverage': ((total_active_performances - len(performances_needing_sync)) / total_active_performances * 100) if total_active_performances > 0 else 100,
                'total_active_packs': total_active_packs,
                'total_packs_needing_sync': total_packs_needing_sync,
                'total_packs_synced': total_active_packs - total_packs_needing_sync,
                'pack_sync_coverage': ((total_active_packs - total_packs_needing_sync) / total_active_packs * 100) if total_active_packs > 0 else 100,
                'last_check': timezone.now().isoformat(),
                'source_website': self.source_website
            }
            
        except Exception as e:
            error_msg = f"Error getting monitoring status: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            return {
                'monitoring_enabled': False,
                'error': error_msg,
                'last_check': timezone.now().isoformat(),
                'source_website': self.source_website
            }
    
    def _get_performances_needing_pos_sync(self) -> List[str]:
        """
        Get performances that have active seat packs needing POS sync.
        
        Returns:
            List of performance IDs needing sync
        """
        return self.sync_service._get_performances_needing_sync()
    
    def _is_pos_integration_enabled(self) -> bool:
        """Check if POS integration is enabled in settings."""
        return (
            hasattr(settings, 'STUBHUB_POS_BASE_URL') and 
            settings.STUBHUB_POS_BASE_URL and
            hasattr(settings, 'STUBHUB_POS_AUTH_TOKEN')
        )
    
    def _send_bulk_sync_completion_message(self, sync_results: Dict[str, Any], performance_ids: List[str]):
        """
        Send bulk sync completion message to backend via RabbitMQ.
        
        Args:
            sync_results: Results from bulk sync operation
            performance_ids: List of performance IDs that were synced
        """
        try:
            # Generate operation ID for tracking
            operation_id = f"bulk_sync_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Determine overall success
            total_operations = sync_results.get('performances_processed', 0) + sync_results.get('performances_failed', 0)
            success = sync_results.get('performances_failed', 0) == 0 or sync_results.get('performances_processed', 0) > sync_results.get('performances_failed', 0)
            
            if success:
                # Send success message
                message_data = {
                    'pattern': 'pos.bulk_sync.success',
                    'data': {
                        'operationId': operation_id,
                        'syncResults': {
                            'performancesProcessed': sync_results.get('performances_processed', 0),
                            'performancesFailed': sync_results.get('performances_failed', 0),
                            'totalPacksSynced': sync_results.get('total_synced', 0),
                            'totalPacksFailed': sync_results.get('total_failed', 0),
                            'totalPerformances': len(performance_ids),
                            'message': f"Bulk sync completed: {sync_results.get('performances_processed', 0)} performances processed"
                        },
                        'syncType': 'bulk_completed',
                        'sourceWebsite': self.source_website,
                        'performanceIds': performance_ids[:10],  # Limit for message size
                        'totalPerformanceIds': len(performance_ids)
                    }
                }
                
                logger.info(f"Sending bulk POS sync success message: {sync_results}")
            else:
                # Send error message
                error_details = "; ".join([str(error) for error in sync_results.get('errors', [])[:3]])
                message_data = {
                    'pattern': 'pos.bulk_sync.error',
                    'data': {
                        'operationId': operation_id,
                        'errorMessage': f"Bulk sync failed with {sync_results.get('performances_failed', 0)} performance errors: {error_details}",
                        'syncType': 'bulk_completed',
                        'sourceWebsite': self.source_website,
                        'performancesAttempted': len(performance_ids),
                        'performancesSuccessful': sync_results.get('performances_processed', 0),
                        'performancesFailed': sync_results.get('performances_failed', 0)
                    }
                }
                
                logger.warning(f"Sending bulk POS sync error message: {error_details}")
            
            # Send the message via RabbitMQ
            message_sent = producer.send_message(message_data)
            
            if message_sent:
                status = 'success' if success else 'error'
                logger.info(f"Successfully sent bulk POS sync {status} message for {len(performance_ids)} performances")
            else:
                logger.error(f"Failed to send bulk POS sync completion message")
                
        except Exception as e:
            logger.error(f"Error sending bulk sync completion message: {str(e)}", exc_info=True)


def check_and_sync_pos_status(source_website: str = 'stubhub') -> Dict[str, Any]:
    """
    High-level function to check POS status and trigger sync if needed.
    This is the main entry point for external systems.
    
    Args:
        source_website: Source website identifier
        
    Returns:
        Dictionary with monitoring and sync results
    """
    monitor = POSStatusMonitor(source_website)
    return monitor.check_pos_status_and_sync()


def get_pos_monitoring_status(source_website: str = 'stubhub') -> Dict[str, Any]:
    """
    Get current POS monitoring status for external monitoring systems.
    
    Args:
        source_website: Source website identifier
        
    Returns:
        Dictionary with monitoring status information
    """
    monitor = POSStatusMonitor(source_website)
    return monitor.get_monitoring_status()


def trigger_performance_pos_sync(performance_id: str, source_website: str = 'stubhub') -> Dict[str, Any]:
    """
    Trigger POS sync for a specific performance.
    
    Args:
        performance_id: Internal performance ID to sync
        source_website: Source website identifier
        
    Returns:
        Dictionary with sync results
    """
    monitor = POSStatusMonitor(source_website)
    return monitor.trigger_performance_sync(performance_id)