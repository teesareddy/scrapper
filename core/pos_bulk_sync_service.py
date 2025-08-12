"""
POS Bulk Synchronization Service

This service handles bulk synchronization of seat packs to POS when the system comes online.
It provides functionality to sync all active seat packs that haven't been added to POS yet.
"""

import logging
from typing import Dict, List, Optional, Any
from django.db import transaction
from django.utils import timezone
from ..models.seat_packs import SeatPack
from ..models.base import Performance
from .seat_pack_sync import get_seat_packs_needing_pos_sync
from .stubhub_inventory_creator import StubHubInventoryCreator, bulk_sync_performance_to_pos

logger = logging.getLogger(__name__)


class POSBulkSyncService:
    """
    Service to handle bulk synchronization of seat packs to POS system.
    """
    
    def __init__(self, source_website: str = 'stubhub'):
        """
        Initialize the bulk sync service.
        
        Args:
            source_website: Source website identifier
        """
        self.source_website = source_website
        
    def sync_all_performances(self, performance_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Sync all active seat packs for multiple performances to POS.
        
        Args:
            performance_ids: List of performance IDs to sync. If None, syncs all performances
                           that have seat packs needing sync.
                           
        Returns:
            Dictionary with overall sync results
        """
        if performance_ids is None:
            performance_ids = self._get_performances_needing_sync()
            
        logger.info(f"Starting bulk POS sync for {len(performance_ids)} performances")
        
        overall_results = {
            'performances_processed': 0,
            'performances_failed': 0,
            'total_synced': 0,
            'total_failed': 0,
            'errors': []
        }
        
        for performance_id in performance_ids:
            try:
                logger.info(f"Syncing performance {performance_id} to POS")
                
                # Use the existing bulk sync function
                sync_result = bulk_sync_performance_to_pos(performance_id, self.source_website)
                
                overall_results['performances_processed'] += 1
                overall_results['total_synced'] += sync_result['synced']
                overall_results['total_failed'] += sync_result['failed']
                
                if sync_result['errors']:
                    overall_results['errors'].extend(sync_result['errors'])
                    
                logger.info(f"Performance {performance_id} sync completed: "
                           f"{sync_result['synced']} synced, {sync_result['failed']} failed")
                           
            except Exception as e:
                error_msg = f"Failed to sync performance {performance_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                overall_results['performances_failed'] += 1
                overall_results['errors'].append({
                    'performance_id': performance_id,
                    'error': error_msg
                })
        
        logger.info(f"Bulk sync completed: {overall_results['performances_processed']} performances processed, "
                   f"{overall_results['total_synced']} total packs synced, "
                   f"{overall_results['total_failed']} total failed")
        
        return overall_results
    
    def sync_single_performance(self, performance_id: str) -> Dict[str, Any]:
        """
        Sync all active seat packs for a single performance to POS.
        
        Args:
            performance_id: Performance ID to sync
            
        Returns:
            Dictionary with sync results
        """
        logger.info(f"Starting POS sync for single performance {performance_id}")
        return bulk_sync_performance_to_pos(performance_id, self.source_website)
    
    def get_sync_status(self, performance_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the current POS sync status for performances.
        Uses the new 4-dimensional model fields for accurate status reporting.
        
        Args:
            performance_id: Specific performance ID to check. If None, checks all.
            
        Returns:
            Dictionary with sync status information
        """
        if performance_id:
            # Single performance status
            seat_packs_needing_sync = get_seat_packs_needing_pos_sync(performance_id, self.source_website)
            total_active_packs = SeatPack.objects.filter(
                zone_id__performance_id=performance_id,
                source_website=self.source_website,
                pack_status='active',  # New field: pack is active in our system
                pack_state__in=['create', 'split', 'merge', 'shrink'],  # Not in terminal delisted state
                delist_reason__isnull=True  # Not delisted for any reason
            ).count()
            
            return {
                'performance_id': performance_id,
                'total_active_packs': total_active_packs,
                'packs_needing_sync': len(seat_packs_needing_sync),
                'packs_synced': total_active_packs - len(seat_packs_needing_sync),
                'sync_coverage': ((total_active_packs - len(seat_packs_needing_sync)) / total_active_packs * 100) if total_active_packs > 0 else 100
            }
        else:
            # Overall status across all performances
            performances_needing_sync = self._get_performances_needing_sync()
            
            total_performances = Performance.objects.filter(
                seat_packs__source_website=self.source_website,
                seat_packs__pack_status='active'  # New field: pack is active
            ).distinct().count()
            
            return {
                'total_performances': total_performances,
                'performances_needing_sync': len(performances_needing_sync),
                'performances_synced': total_performances - len(performances_needing_sync),
                'performance_sync_coverage': ((total_performances - len(performances_needing_sync)) / total_performances * 100) if total_performances > 0 else 100
            }
    
    def _get_performances_needing_sync(self) -> List[str]:
        """
        Get list of performance IDs that have seat packs needing POS sync.
        Uses the new 4-dimensional model fields for accurate filtering.
        
        Returns:
            List of performance IDs needing sync
        """
        # Get distinct performance IDs that have seat packs needing POS sync
        # These are active packs with pending or delisted POS status
        performance_ids = list(SeatPack.objects.filter(
            source_website=self.source_website,
            pack_status='active',  # New field: pack is active in our system
            pos_status__in=['pending', 'delisted'],  # New field: needs POS sync or was delisted and needs re-sync
            pack_state__in=['create', 'split', 'merge', 'shrink'],  # Not in terminal delisted state
            pos_listing__isnull=True  # Not synced to POS yet
        ).values_list('zone_id__performance_id__internal_performance_id', flat=True).distinct())
        
        logger.debug(f"Found {len(performance_ids)} performances needing POS sync")
        logger.debug(f"Using new model fields: pack_status=active, pos_status in [pending, delisted]")
        return performance_ids


def sync_all_to_pos(source_website: str = 'stubhub') -> Dict[str, Any]:
    """
    High-level function to sync all seat packs needing POS synchronization.
    This is the main entry point for external systems when POS comes online.
    
    Args:
        source_website: Source website identifier
        
    Returns:
        Dictionary with overall sync results
    """
    service = POSBulkSyncService(source_website)
    return service.sync_all_performances()


def get_pos_sync_status(performance_id: Optional[str] = None, source_website: str = 'stubhub') -> Dict[str, Any]:
    """
    Get current POS sync status for monitoring purposes.
    
    Args:
        performance_id: Optional specific performance ID to check
        source_website: Source website identifier
        
    Returns:
        Dictionary with sync status information
    """
    service = POSBulkSyncService(source_website)
    return service.get_sync_status(performance_id)