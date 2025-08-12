"""
POS Sync Service for Independent POS Operations

This module provides an independent service for syncing seat packs with StubHub POS,
supporting both immediate and on-demand sync modes with push and delist operations.
"""

import logging
import uuid
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from django.db import transaction
from django.utils import timezone
from django.core.exceptions import ValidationError

from ..models.seat_packs import SeatPack
from ..models.pos import POSListing, FailedRollback
from .pos_config_handler import POSConfiguration
from .seat_pack_comparator import SeatPackComparison

logger = logging.getLogger(__name__)


class POSSyncResult:
    """Result of a POS sync operation"""
    
    def __init__(self):
        self.success: bool = False
        self.operation_id: str = str(uuid.uuid4())
        self.processed_count: int = 0
        self.pushed_count: int = 0
        self.delisted_count: int = 0
        self.failed_count: int = 0
        self.errors: List[str] = []
        self.pack_results: Dict[str, Dict[str, Any]] = {}
        self.operation_summary: Dict[str, Any] = {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization"""
        return {
            'success': self.success,
            'operation_id': self.operation_id,
            'processed_count': self.processed_count,
            'pushed_count': self.pushed_count,
            'delisted_count': self.delisted_count,
            'failed_count': self.failed_count,
            'errors': self.errors,
            'pack_results': self.pack_results,
            'operation_summary': self.operation_summary
        }


class POSSyncService:
    """
    Independent service for syncing seat packs with StubHub POS
    """
    
    def __init__(self, config: POSConfiguration):
        """
        Initialize the POS sync service
        
        Args:
            config: POSConfiguration with sync settings
        """
        self.config = config
        self.operation_id = str(uuid.uuid4())
        
    def sync_seat_packs(
        self, 
        comparison: SeatPackComparison,
        sync_mode: Optional[str] = None
    ) -> POSSyncResult:
        """
        Sync seat packs based on comparison results
        
        Args:
            comparison: SeatPackComparison with categorized packs
            sync_mode: Override sync mode ('immediate' or 'on_demand')
            
        Returns:
            POSSyncResult with operation details
        """
        logger.info(f"Starting POS sync operation {self.operation_id}")
        
        # Use provided sync mode or default from config
        effective_sync_mode = sync_mode or self.config.sync_mode
        
        # Check if POS is enabled
        if not self.config.pos_enabled:
            logger.info("POS sync disabled in configuration")
            return self._create_disabled_result()
        
        result = POSSyncResult()
        result.operation_id = self.operation_id
        
        try:
            # Determine which packs need sync operations
            packs_to_push = self._get_packs_for_push_operation(comparison)
            packs_to_delist = self._get_packs_for_delist_operation(comparison)
            
            logger.info(f"POS sync plan: {len(packs_to_push)} to push, {len(packs_to_delist)} to delist")
            
            # Execute sync operations based on mode
            if effective_sync_mode == 'immediate':
                result = self._execute_immediate_sync(packs_to_push, packs_to_delist, result)
            else:  # on_demand
                result = self._execute_on_demand_sync(packs_to_push, packs_to_delist, result)
            
            result.success = result.failed_count == 0
            logger.info(f"POS sync completed: {result.to_dict()['operation_summary']}")
            
        except Exception as e:
            logger.error(f"POS sync operation {self.operation_id} failed: {e}", exc_info=True)
            result.errors.append(f"Sync operation failed: {e}")
            result.success = False
        
        return result
    
    def push_seat_packs(self, seat_packs: List[SeatPack]) -> POSSyncResult:
        """
        Push (create/update) seat packs to StubHub POS
        
        Args:
            seat_packs: List of seat packs to push to POS
            
        Returns:
            POSSyncResult with push operation details
        """
        logger.info(f"Pushing {len(seat_packs)} seat packs to POS")
        
        result = POSSyncResult()
        result.operation_id = self.operation_id
        
        if not self.config.create_enabled:
            logger.warning("POS create operations disabled in configuration")
            result.errors.append("POS create operations disabled")
            return result
        
        try:
            with transaction.atomic():
                for pack in seat_packs:
                    pack_result = self._push_single_pack(pack)
                    result.pack_results[pack.internal_pack_id] = pack_result
                    
                    if pack_result['success']:
                        result.pushed_count += 1
                    else:
                        result.failed_count += 1
                        result.errors.extend(pack_result.get('errors', []))
                    
                    result.processed_count += 1
                
                result.success = result.failed_count == 0
                
        except Exception as e:
            logger.error(f"Error pushing seat packs: {e}", exc_info=True)
            result.errors.append(f"Push operation failed: {e}")
            result.success = False
        
        return result
    
    def delist_seat_packs(self, seat_packs: List[SeatPack]) -> POSSyncResult:
        """
        Delist (delete) seat packs from StubHub POS
        
        Args:
            seat_packs: List of seat packs to delist from POS
            
        Returns:
            POSSyncResult with delist operation details
        """
        logger.info(f"Delisting {len(seat_packs)} seat packs from POS")
        
        result = POSSyncResult()
        result.operation_id = self.operation_id
        
        if not self.config.delete_enabled:
            logger.warning("POS delete operations disabled in configuration")
            result.errors.append("POS delete operations disabled")
            return result
        
        try:
            with transaction.atomic():
                for pack in seat_packs:
                    pack_result = self._delist_single_pack(pack)
                    result.pack_results[pack.internal_pack_id] = pack_result
                    
                    if pack_result['success']:
                        result.delisted_count += 1
                    else:
                        result.failed_count += 1
                        result.errors.extend(pack_result.get('errors', []))
                    
                    result.processed_count += 1
                
                result.success = result.failed_count == 0
                
        except Exception as e:
            logger.error(f"Error delisting seat packs: {e}", exc_info=True)
            result.errors.append(f"Delist operation failed: {e}")
            result.success = False
        
        return result
    
    def _execute_immediate_sync(
        self, 
        packs_to_push: List[SeatPack], 
        packs_to_delist: List[SeatPack],
        result: POSSyncResult
    ) -> POSSyncResult:
        """Execute immediate sync operations"""
        logger.info(f"Executing immediate sync mode")
        
        # Push new/updated packs first
        if packs_to_push:
            push_result = self.push_seat_packs(packs_to_push)
            result.pushed_count = push_result.pushed_count
            result.pack_results.update(push_result.pack_results)
            result.errors.extend(push_result.errors)
            result.failed_count += push_result.failed_count
        
        # Then delist removed packs
        if packs_to_delist:
            delist_result = self.delist_seat_packs(packs_to_delist)
            result.delisted_count = delist_result.delisted_count
            result.pack_results.update(delist_result.pack_results)
            result.errors.extend(delist_result.errors)
            result.failed_count += delist_result.failed_count
        
        result.processed_count = len(packs_to_push) + len(packs_to_delist)
        result.operation_summary = {
            'mode': 'immediate',
            'operations_attempted': result.processed_count,
            'successful_pushes': result.pushed_count,
            'successful_delists': result.delisted_count,
            'failed_operations': result.failed_count
        }
        
        return result
    
    def _execute_on_demand_sync(
        self, 
        packs_to_push: List[SeatPack], 
        packs_to_delist: List[SeatPack],
        result: POSSyncResult
    ) -> POSSyncResult:
        """Execute on-demand sync operations (mark for later sync)"""
        logger.info(f"Executing on-demand sync mode")
        
        try:
            with transaction.atomic():
                # Mark packs for push operations
                for pack in packs_to_push:
                    pack.pos_status = 'pending'
                    pack.synced_to_pos = False
                    pack.save(update_fields=['pos_status', 'synced_to_pos', 'updated_at'])
                    result.pack_results[pack.internal_pack_id] = {
                        'success': True,
                        'operation': 'marked_for_push',
                        'status': 'pending'
                    }
                
                # Mark packs for delist operations
                for pack in packs_to_delist:
                    pack.pos_status = 'pending'
                    pack.synced_to_pos = False
                    pack.save(update_fields=['pos_status', 'synced_to_pos', 'updated_at'])
                    result.pack_results[pack.internal_pack_id] = {
                        'success': True,
                        'operation': 'marked_for_delist',
                        'status': 'pending'
                    }
                
                result.processed_count = len(packs_to_push) + len(packs_to_delist)
                result.pushed_count = len(packs_to_push)  # Marked for push
                result.delisted_count = len(packs_to_delist)  # Marked for delist
                result.operation_summary = {
                    'mode': 'on_demand',
                    'packs_marked_for_sync': result.processed_count,
                    'marked_for_push': len(packs_to_push),
                    'marked_for_delist': len(packs_to_delist)
                }
                
        except Exception as e:
            logger.error(f"Error in on-demand sync marking: {e}", exc_info=True)
            result.errors.append(f"On-demand sync marking failed: {e}")
            result.success = False
        
        return result
    
    def _get_packs_for_push_operation(self, comparison: SeatPackComparison) -> List[SeatPack]:
        """Get packs that need to be pushed to POS"""
        packs_to_push = []
        
        # Get new packs that need to be created in POS
        # Note: new_packs are SeatPackData objects, we need to get the SeatPack objects
        # For now, we'll focus on existing packs that need updates
        
        # Get unchanged packs that might need POS sync
        for pack in comparison.unchanged_packs:
            if pack.pack_status == 'active' and pack.pos_status in ['pending', 'failed']:
                packs_to_push.append(pack)
        
        logger.debug(f"Identified {len(packs_to_push)} packs for push operation")
        return packs_to_push
    
    def _get_packs_for_delist_operation(self, comparison: SeatPackComparison) -> List[SeatPack]:
        """Get packs that need to be delisted from POS"""
        packs_to_delist = []
        
        # Get removed packs that need to be delisted
        for pack in comparison.removed_packs:
            if pack.pos_status == 'active' and pack.pack_status == 'active':
                packs_to_delist.append(pack)
        
        logger.debug(f"Identified {len(packs_to_delist)} packs for delist operation")
        return packs_to_delist
    
    def _push_single_pack(self, pack: SeatPack) -> Dict[str, Any]:
        """Push a single seat pack to StubHub POS"""
        try:
            # Simulate StubHub API call
            stubhub_response = self._call_stubhub_create_api(pack)
            
            if stubhub_response['success']:
                # Update pack with successful sync
                pack.pos_status = 'active'
                pack.synced_to_pos = True
                pack.pos_sync_attempts += 1
                pack.last_pos_sync_attempt = timezone.now()
                pack.pos_sync_error = None
                pack.save(update_fields=[
                    'pos_status', 'synced_to_pos', 'pos_sync_attempts', 
                    'last_pos_sync_attempt', 'pos_sync_error', 'updated_at'
                ])
                
                return {
                    'success': True,
                    'operation': 'push',
                    'stubhub_inventory_id': stubhub_response.get('inventory_id'),
                    'message': 'Successfully pushed to StubHub'
                }
            else:
                # Handle failed sync
                pack.pos_status = 'failed'
                pack.pos_sync_attempts += 1
                pack.last_pos_sync_attempt = timezone.now()
                pack.pos_sync_error = stubhub_response.get('error', 'Unknown error')
                pack.save(update_fields=[
                    'pos_status', 'pos_sync_attempts', 
                    'last_pos_sync_attempt', 'pos_sync_error', 'updated_at'
                ])
                
                return {
                    'success': False,
                    'operation': 'push',
                    'errors': [stubhub_response.get('error', 'Unknown error')]
                }
                
        except Exception as e:
            logger.error(f"Error pushing pack {pack.internal_pack_id}: {e}", exc_info=True)
            return {
                'success': False,
                'operation': 'push',
                'errors': [f"Exception during push: {e}"]
            }
    
    def _delist_single_pack(self, pack: SeatPack) -> Dict[str, Any]:
        """Delist a single seat pack from StubHub POS"""
        try:
            # Simulate StubHub API call
            stubhub_response = self._call_stubhub_delete_api(pack)
            
            if stubhub_response['success']:
                # Update pack with successful delist
                pack.pos_status = 'inactive'
                pack.synced_to_pos = True
                pack.pos_sync_attempts += 1
                pack.last_pos_sync_attempt = timezone.now()
                pack.pos_sync_error = None
                pack.save(update_fields=[
                    'pos_status', 'synced_to_pos', 'pos_sync_attempts', 
                    'last_pos_sync_attempt', 'pos_sync_error', 'updated_at'
                ])
                
                return {
                    'success': True,
                    'operation': 'delist',
                    'message': 'Successfully delisted from StubHub'
                }
            else:
                # Handle failed delist
                pack.pos_status = 'failed'
                pack.pos_sync_attempts += 1
                pack.last_pos_sync_attempt = timezone.now()
                pack.pos_sync_error = stubhub_response.get('error', 'Unknown error')
                pack.save(update_fields=[
                    'pos_status', 'pos_sync_attempts', 
                    'last_pos_sync_attempt', 'pos_sync_error', 'updated_at'
                ])
                
                return {
                    'success': False,
                    'operation': 'delist',
                    'errors': [stubhub_response.get('error', 'Unknown error')]
                }
                
        except Exception as e:
            logger.error(f"Error delisting pack {pack.internal_pack_id}: {e}", exc_info=True)
            return {
                'success': False,
                'operation': 'delist',
                'errors': [f"Exception during delist: {e}"]
            }
    
    def _call_stubhub_create_api(self, pack: SeatPack) -> Dict[str, Any]:
        """
        Simulate StubHub CREATE API call
        In real implementation, this would make actual API calls
        """
        # Simulate API response
        logger.info(f"Simulating StubHub CREATE API for pack {pack.internal_pack_id}")
        
        # Simulate success/failure based on configuration
        if self.config.admin_hold_enabled:
            return {
                'success': False,
                'error': f'Admin hold active: {self.config.admin_hold_reason}'
            }
        
        return {
            'success': True,
            'inventory_id': f"stub_{pack.internal_pack_id}_{int(timezone.now().timestamp())}"
        }
    
    def _call_stubhub_delete_api(self, pack: SeatPack) -> Dict[str, Any]:
        """
        Simulate StubHub DELETE API call
        In real implementation, this would make actual API calls
        """
        # Simulate API response
        logger.info(f"Simulating StubHub DELETE API for pack {pack.internal_pack_id}")
        
        return {
            'success': True,
            'message': 'Inventory deleted from StubHub'
        }
    
    def _create_disabled_result(self) -> POSSyncResult:
        """Create result object for disabled POS"""
        result = POSSyncResult()
        result.success = True
        result.operation_summary = {
            'mode': 'disabled',
            'message': 'POS sync is disabled in configuration'
        }
        return result


def sync_seat_packs_with_pos(
    comparison: SeatPackComparison,
    config: POSConfiguration,
    sync_mode: Optional[str] = None
) -> POSSyncResult:
    """
    Convenience function for syncing seat packs with POS
    
    Args:
        comparison: SeatPackComparison with categorized pack results
        config: POSConfiguration with sync settings
        sync_mode: Override sync mode ('immediate' or 'on_demand')
        
    Returns:
        POSSyncResult with operation details
    """
    service = POSSyncService(config)
    return service.sync_seat_packs(comparison, sync_mode)