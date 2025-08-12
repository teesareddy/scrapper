"""
Seat Pack Synchronization (Diffing) Algorithm for Phase 2
Based on the algorithm documented in docs/seat_pack_different_states.md

This module implements the intelligent diffing algorithm that compares
existing seat packs in the database with newly generated ones to produce
a minimal, precise set of sync actions.
"""

from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from .data_schemas import SeatPackData
from ..models.seat_packs import SeatPack
import logging

logger = logging.getLogger(__name__)


@dataclass
class CreationAction:
    """Action to create a new seat pack"""
    pack_data: SeatPackData
    action_type: str  # 'create', 'split', 'shrink', 'merge'
    source_pack_ids: List[str]  # Parent pack IDs this originated from


@dataclass
class UpdateAction:
    """Action to update an existing seat pack's attributes"""
    pack_id: str
    updated_data: SeatPackData


@dataclass
class DelistAction:
    """Action to mark an existing seat pack as inactive"""
    pack_id: str
    reason: str  # 'transformed', 'vanished'


@dataclass
class SyncAction:
    """Action to sync an existing, unchanged seat pack to the POS."""
    pack_data: SeatPackData
    action_type: str = 'sync_existing'

@dataclass
class SyncPlan:
    """Complete sync plan containing all actions to be executed"""
    creation_actions: List[CreationAction]
    update_actions: List[UpdateAction]
    delist_actions: List[DelistAction]
    sync_actions: List[SyncAction]


def diff_seat_packs(
    existing_packs_from_db: List[SeatPack], 
    newly_generated_packs: List[SeatPackData]
) -> SyncPlan:
    """
    Compares existing seat packs with newly generated ones to produce a sync plan.
    
    This implements the 5-phase diffing algorithm:
    1. Build efficient lookup maps
    2. High-level comparison with set logic
    3. Analyze unchanged packs for attribute updates
    4. Correlate new packs to find their origins
    5. Generate final sync actions
    
    Args:
        existing_packs_from_db: List of active SeatPack models from database
        newly_generated_packs: List of SeatPackData from latest generation
        
    Returns:
        SyncPlan containing creation, update, and delist actions
    """
    
    # Phase 1: Build Efficient Lookup Maps
    # Use internal_pack_id for existing packs (primary key) and pack_id for new packs  
    existing_packs_by_id = {pack.internal_pack_id: pack for pack in existing_packs_from_db}
    new_packs_by_id = {pack.pack_id: pack for pack in newly_generated_packs}
    
    # Critical: Build seat-to-old-pack mapping for lineage tracking
    seat_to_old_pack_map = {}
    for pack in existing_packs_from_db:
        for seat_id in pack.seat_keys:
            seat_to_old_pack_map[seat_id] = pack.internal_pack_id
    
    logger.debug(f"Built lookups: {len(existing_packs_by_id)} existing, {len(new_packs_by_id)} new, {len(seat_to_old_pack_map)} seat mappings")
    
    # Phase 2: POS-Aware Pack Comparison with Three Categories
    existing_ids = set(existing_packs_by_id.keys())
    new_ids = set(new_packs_by_id.keys())

    truly_identical_ids = []  # 100% identical packs - no action needed
    functionally_equivalent_ids = []  # Same seats, minor differences - update price only
    
    for pack_id in (existing_ids & new_ids):
        old_pack = existing_packs_by_id[pack_id]
        new_pack = new_packs_by_id[pack_id]

        if _packs_are_identical(old_pack, new_pack):
            truly_identical_ids.append(pack_id)
        elif _packs_are_functionally_equivalent(old_pack, new_pack):
            functionally_equivalent_ids.append(pack_id)



    # Combine preserved packs
    preserved_pack_ids = set(truly_identical_ids + functionally_equivalent_ids)
    
    # Normal pack transformation logic
    deleted_ids = existing_ids - new_ids - preserved_pack_ids
    created_ids = new_ids - existing_ids
    
    # Phase 3: Handle update actions for functionally equivalent packs
    update_actions = []
    
    # Create update actions for functionally equivalent packs (price updates)
    for pack_id in functionally_equivalent_ids:
        old_pack = existing_packs_by_id[pack_id]
        new_pack = new_packs_by_id[pack_id]
        
        # Only create update if price actually changed
        if (old_pack.pack_price != new_pack.pack_price or 
            old_pack.total_price != new_pack.total_price):
            update_actions.append(UpdateAction(
                pack_id=pack_id,
                updated_data=new_pack
            ))
            logger.debug(f"📝 Price update for pack {pack_id}: ${old_pack.pack_price} → ${new_pack.pack_price}")
    
    # Phase 4: Correlate New Packs to Find Their Origins
    new_pack_to_sources_map = {}
    source_to_children_map = {}
    
    for new_pack in [new_packs_by_id[pack_id] for pack_id in created_ids]:
        # Find all old packs that contributed seats to this new pack
        source_pack_ids = set()
        for seat_id in new_pack.seat_ids:
            if seat_id in seat_to_old_pack_map:
                source_pack_ids.add(seat_to_old_pack_map[seat_id])
        
        source_pack_ids_list = list(source_pack_ids)
        new_pack_to_sources_map[new_pack.pack_id] = source_pack_ids_list
        
        # Build reverse mapping: source -> children
        for source_id in source_pack_ids_list:
            if source_id not in source_to_children_map:
                source_to_children_map[source_id] = []
            source_to_children_map[source_id].append(new_pack.pack_id)
    
    logger.debug(f"Built correlation maps: {len(new_pack_to_sources_map)} new->sources, {len(source_to_children_map)} sources->children")
    
    # Phase 5: Generate Final Sync Actions
    creation_actions = []
    delist_actions = []
    sync_actions = []
    
    # Generate sync actions for ALL existing packs that need POS sync
    for pack_id in existing_ids:
        old_pack = existing_packs_by_id[pack_id]
        # Push ALL active packs with pending/failed pos_status to POS
        if old_pack.pos_status in ['pending', 'failed']:
            # Use existing pack data converted to SeatPackData format
            from .data_schemas import SeatPackData
            pack_data = SeatPackData(
                pack_id=old_pack.internal_pack_id,
                zone_id=old_pack.zone_id.source_zone_id if old_pack.zone_id else '',
                source_website=old_pack.source_website,
                row_label=old_pack.row_label,
                start_seat_number=old_pack.start_seat_number,
                end_seat_number=old_pack.end_seat_number,
                pack_size=old_pack.pack_size,
                pack_price=old_pack.pack_price,
                total_price=old_pack.total_price,
                seat_ids=old_pack.seat_keys
            )
            sync_actions.append(SyncAction(pack_data=pack_data))
            logger.debug(f"Queuing existing pack {pack_id} for POS sync (pos_status: {old_pack.pos_status})")
    
    # Generate creation actions with proper classification
    for new_pack_id in created_ids:
        new_pack = new_packs_by_id[new_pack_id]
        source_pack_ids = new_pack_to_sources_map.get(new_pack_id, [])
        
        # Determine action type based on source analysis
        if len(source_pack_ids) == 0:
            action_type = 'create'
        elif len(source_pack_ids) == 1:
            # Check if this is the only child of the source
            source_id = source_pack_ids[0]
            children_count = len(source_to_children_map.get(source_id, []))
            action_type = 'shrink' if children_count == 1 else 'split'
            

            if children_count > 1:
                all_children = source_to_children_map.get(source_id, [])
        else:
            action_type = 'merge'
        
        creation_actions.append(CreationAction(
            pack_data=new_pack,
            action_type=action_type,
            source_pack_ids=source_pack_ids
        ))
        
        logger.debug(f"New pack {new_pack_id}: {action_type} from {len(source_pack_ids)} sources")
    
    # Generate delist actions
    transformed_pack_ids = set(source_to_children_map.keys())
    
    for old_pack_id in deleted_ids:
        if old_pack_id in transformed_pack_ids:
            reason = 'transformed'
        else:
            reason = 'vanished'
        
        delist_actions.append(DelistAction(
            pack_id=old_pack_id,
            reason=reason
        ))
        
        logger.debug(f"Delisting pack {old_pack_id}: {reason}")
    
    sync_plan = SyncPlan(
        creation_actions=creation_actions,
        update_actions=update_actions,
        delist_actions=delist_actions,
        sync_actions=sync_actions
    )
    return sync_plan


def _packs_are_identical(old_pack: SeatPack, new_pack: SeatPackData) -> bool:
    """
    Checks if two packs are completely identical in ALL attributes.
    
    If even one attribute differs, they are considered different packs
    that should be deleted and recreated rather than updated.
    
    Args:
        old_pack: Existing SeatPack model from database
        new_pack: New SeatPackData from generation
        
    Returns:
        True if packs are 100% identical, False otherwise
    """
    # Compare all core attributes
    if old_pack.pack_price != new_pack.pack_price:
        return False
    
    if old_pack.total_price != new_pack.total_price:
        return False
    
    if old_pack.row_label != new_pack.row_label:
        return False
    
    if old_pack.start_seat_number != new_pack.start_seat_number:
        return False
    
    if old_pack.end_seat_number != new_pack.end_seat_number:
        return False
    
    if old_pack.pack_size != new_pack.pack_size:
        return False
    
    # Compare seat lists (order doesn't matter, but content must be identical)
    old_seats = set(old_pack.seat_keys)
    new_seats = set(new_pack.seat_ids)
    if old_seats != new_seats:
        return False
    
    # If we get here, all attributes are identical
    return True


def _packs_are_functionally_equivalent(old_pack: SeatPack, new_pack: SeatPackData) -> bool:
    """
    Checks if two packs are functionally equivalent with minor tolerance.
    Used for POS-aware sync logic to preserve active pending packs when possible.
    
    This is more lenient than _packs_are_identical() and allows for minor
    differences that shouldn't disrupt POS sync (like small price changes).
    
    Args:
        old_pack: Existing SeatPack model from database
        new_pack: New SeatPackData from generation
        
    Returns:
        True if packs are functionally equivalent, False if they need transformation
    """
    # Core identity must match (row, seat range, size)
    if (old_pack.row_label != new_pack.row_label or
        old_pack.start_seat_number != new_pack.start_seat_number or
        old_pack.end_seat_number != new_pack.end_seat_number or
        old_pack.pack_size != new_pack.pack_size):
        return False
    
    # Seat composition must be identical (critical for inventory integrity)
    old_seats = set(old_pack.seat_keys)
    new_seats = set(new_pack.seat_ids)
    if old_seats != new_seats:
        return False
    
    # If we get here, the packs represent the same seats
    # Price differences are allowed for functional equivalence
    return True


def get_active_seat_packs_for_performance(performance_id: str) -> List[SeatPack]:
    """
    Retrieves active seat packs for a performance, respecting manual overrides.
    Uses internal_pack_id to prevent duplicate processing.
    Use the new 4-dimensional model fields for accurate filtering.
    
    Args:
        performance_id: Internal performance ID
        
    Returns:
        List of active SeatPack models that sync can process
    """
    # Build query with new four-dimensional model fields
    query_filter = {
        'performance': performance_id,
        'pack_status': 'active',
    }
    packs = list(SeatPack.objects.filter(**query_filter).select_related(
        'zone_id', 'scrape_job_key', 'pos_listing', 'performance', 'level'
    ).distinct())
    return packs


def get_seat_packs_needing_pos_sync(performance_id: str, source_website: str) -> List[SeatPack]:
    """
    Retrieves active seat packs that need POS synchronization.
    These are packs that exist in our database but haven't been synced to POS yet.
    Uses the new 4-dimensional model fields for accurate filtering.
    
    Args:
        performance_id: Internal performance ID
        source_website: Source website to filter by
        
    Returns:
        List of SeatPack models that need POS synchronization
    """
    # Use the new 4-dimensional model fields for accurate filtering
    # Active packs with pending or delisted POS status need to be synced
    packs = list(SeatPack.objects.filter(
        zone_id__performance_id=performance_id,
        source_website=source_website,
        pack_status='active',  # New field: pack is active in our system
        pos_status__in=['pending', 'delisted'],  # New field: needs POS sync or was delisted and needs re-sync
        pack_state__in=['create', 'split', 'merge', 'shrink'],  # Not in terminal delisted state
        pos_listing__isnull=True  # Not synced to POS yet
    ).select_related('zone_id', 'scrape_job_key', 'pos_listing'))
    
    logger.info(f"Found {len(packs)} seat packs needing POS sync for performance {performance_id}")
    logger.debug(f"Pack filtering: pack_status=active, pos_status in [pending, delisted] → listed after sync")
    return packs


def prepare_seat_pack_data_for_sync(seat_packs: List[SeatPackData]) -> List[SeatPackData]:
    """
    Prepares newly generated seat pack data for the sync process.
    Includes duplicate detection based on pack_id.
    
    Args:
        seat_packs: Raw seat pack data from generation
        
    Returns:
        Processed seat pack data ready for sync comparison
    """
    # Check for duplicates in new data
    pack_ids = [pack.pack_id for pack in seat_packs]
    if len(pack_ids) != len(set(pack_ids)):
        # Remove duplicates while preserving order
        seen = set()
        unique_packs = []
        for pack in seat_packs:
            if pack.pack_id not in seen:
                unique_packs.append(pack)
                seen.add(pack.pack_id)
        
        logger.info(f"Removed {len(seat_packs) - len(unique_packs)} duplicate seat packs from new data")
        seat_packs = unique_packs
    
    return seat_packs