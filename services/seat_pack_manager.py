"""
SeatPackManager for handling race condition protection and optimistic locking.
"""

import time
import logging
from typing import Dict, Any, Tuple, Optional
from django.db import transaction, IntegrityError
from django.core.exceptions import ValidationError
from django.utils import timezone
from datetime import timedelta

from ..models.seat_packs import SeatPack

logger = logging.getLogger(__name__)


class SeatPackManager:
    """Enhanced manager with race condition protection."""
    
    def __init__(self):
        self.max_retries = 3
        self.base_delay = 0.1
    
    def safe_update_pack(self, pack_id: str, updates: Dict[str, Any], process_id: str) -> Tuple[bool, Optional[SeatPack], Optional[str]]:
        """
        Safely update a pack with optimistic locking.
        
        Args:
            pack_id: ID of pack to update
            updates: Dictionary of fields to update
            process_id: Unique identifier for this process
            
        Returns:
            tuple: (success: bool, pack: SeatPack or None, error: str or None)
        """
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                # Acquire lock
                pack = self.acquire_pack_lock(pack_id, process_id)
                if not pack:
                    return False, None, "Pack is locked by another process"
                
                # Store original version for optimistic locking
                original_version = pack.version
                
                # Apply updates
                for field, value in updates.items():
                    setattr(pack, field, value)
                
                # Validate state transitions
                if 'pack_state' in updates:
                    self._validate_state_transition(pack, updates['pack_state'])
                
                # Save with version check
                pack.save()
                
                # Release lock
                self.release_pack_lock(pack_id, process_id)
                
                logger.info(f"Successfully updated pack {pack_id} (version {original_version} -> {pack.version})")
                return True, pack, None
                
            except IntegrityError as e:
                # Version conflict - another process updated the pack
                retry_count += 1
                if retry_count >= self.max_retries:
                    return False, None, f"Version conflict after {self.max_retries} retries"
                
                # Exponential backoff
                delay = self.base_delay * (2 ** retry_count)
                logger.warning(f"Version conflict for pack {pack_id}, retrying in {delay}s (attempt {retry_count})")
                time.sleep(delay)
                
            except Exception as e:
                # Release lock on error
                self.release_pack_lock(pack_id, process_id)
                logger.error(f"Error updating pack {pack_id}: {str(e)}")
                return False, None, str(e)
        
        return False, None, "Max retries exceeded"
    
    def acquire_pack_lock(self, pack_id: str, process_id: str) -> Optional[SeatPack]:
        """Acquire optimistic lock on a pack."""
        try:
            with transaction.atomic():
                pack = SeatPack.objects.select_for_update().get(
                    internal_pack_id=pack_id,
                    locked_by__isnull=True
                )
                pack.locked_by = process_id
                pack.locked_at = timezone.now()
                pack.save(update_fields=['locked_by', 'locked_at'])
                
                logger.debug(f"Acquired lock on pack {pack_id} by process {process_id}")
                return pack
                
        except SeatPack.DoesNotExist:
            logger.warning(f"Pack {pack_id} is already locked or doesn't exist")
            return None
        except Exception as e:
            logger.error(f"Error acquiring lock on pack {pack_id}: {str(e)}")
            return None
    
    def release_pack_lock(self, pack_id: str, process_id: str) -> bool:
        """Release optimistic lock on a pack."""
        try:
            updated = SeatPack.objects.filter(
                internal_pack_id=pack_id,
                locked_by=process_id
            ).update(
                locked_by=None,
                locked_at=None
            )
            
            if updated:
                logger.debug(f"Released lock on pack {pack_id} by process {process_id}")
                return True
            else:
                logger.warning(f"No lock found to release for pack {pack_id} by process {process_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error releasing lock on pack {pack_id}: {str(e)}")
            return False
    
    def cleanup_stale_locks(self, max_age_minutes: int = 30) -> int:
        """Clean up locks older than max_age_minutes."""
        try:
            stale_threshold = timezone.now() - timedelta(minutes=max_age_minutes)
            
            stale_locks = SeatPack.objects.filter(
                locked_at__lt=stale_threshold
            ).update(
                locked_by=None,
                locked_at=None
            )
            
            if stale_locks > 0:
                logger.warning(f"Cleaned up {stale_locks} stale pack locks")
            
            return stale_locks
            
        except Exception as e:
            logger.error(f"Error cleaning up stale locks: {str(e)}")
            return 0
    
    def monitor_lock_health(self) -> Dict[str, int]:
        """Monitor lock health and return metrics."""
        try:
            current_locks = SeatPack.objects.filter(
                locked_by__isnull=False
            ).count()
            
            stale_locks = SeatPack.objects.filter(
                locked_at__lt=timezone.now() - timedelta(minutes=5)
            ).count()
            
            if current_locks > 100:
                logger.warning(f"High number of active locks: {current_locks}")
            
            if stale_locks > 10:
                logger.error(f"High number of stale locks: {stale_locks}")
            
            return {
                'active_locks': current_locks,
                'stale_locks': stale_locks
            }
            
        except Exception as e:
            logger.error(f"Error monitoring lock health: {str(e)}")
            return {
                'active_locks': 0,
                'stale_locks': 0
            }
    
    def _validate_state_transition(self, pack: SeatPack, new_state: str) -> None:
        """Validate that a state transition is allowed."""
        VALID_TRANSITIONS = {
            'create': ['split', 'merge', 'shrink', 'delist', 'transformed'],
            'split': ['delist', 'transformed'],
            'merge': ['delist', 'transformed'],
            'shrink': ['delist', 'transformed'],
            'delist': ['create'],  # Only through manual re-enable
            'transformed': [],     # Terminal state
        }
        
        old_state = pack.pack_state
        
        if old_state == new_state:
            return  # No change is always valid
        
        allowed_transitions = VALID_TRANSITIONS.get(old_state, [])
        if new_state not in allowed_transitions:
            raise ValidationError(
                f"Invalid state transition from '{old_state}' to '{new_state}'. "
                f"Allowed transitions: {allowed_transitions}"
            )
    
    def get_pack_for_processing(self, pack_id: str, process_id: str) -> Optional[SeatPack]:
        """Get and lock a pack for processing."""
        return self.acquire_pack_lock(pack_id, process_id)
    
    def finish_pack_processing(self, pack_id: str, process_id: str, updates: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Complete processing and release lock."""
        try:
            # Apply updates
            pack = SeatPack.objects.get(
                internal_pack_id=pack_id,
                locked_by=process_id
            )
            
            for field, value in updates.items():
                setattr(pack, field, value)
            
            pack.save()
            
            # Release lock
            self.release_pack_lock(pack_id, process_id)
            
            return True, None
            
        except SeatPack.DoesNotExist:
            return False, f"Pack {pack_id} not found or not locked by process {process_id}"
        except Exception as e:
            # Try to release lock on error
            self.release_pack_lock(pack_id, process_id)
            return False, str(e)