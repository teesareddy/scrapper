"""
Seat Pack Saver for POS Sync Workflow

This module handles saving new seat packs to the database while preserving
pack lineage tracking and creation modes.
"""

import logging
from typing import List, Dict, Any, Optional
from decimal import Decimal
from django.db import transaction
from django.utils import timezone
from ..models.seat_packs import SeatPack
from ..models.base import Performance, Zone, Level, Venue
from .data_schemas import SeatPackData
from .seat_pack_comparator import SeatPackComparison

logger = logging.getLogger(__name__)


class SeatPackSaver:
    """
    Handles saving seat packs to database with proper lineage tracking
    """
    
    def __init__(self, performance_id: str, source_website: str, scrape_job_id: str):
        """
        Initialize the seat pack saver
        
        Args:
            performance_id: Internal performance ID
            source_website: Source website identifier
            scrape_job_id: Current scrape job ID
        """
        self.performance_id = performance_id
        self.source_website = source_website
        self.scrape_job_id = scrape_job_id
        
        # Cache related objects
        self.performance = None
        self.zones_cache = {}
        self.levels_cache = {}
        
    def save_new_seat_packs_with_lineage(self, comparison: SeatPackComparison) -> Dict[str, Any]:
        """
        Save new seat packs and update lineage tracking atomically
        
        Args:
            comparison: SeatPackComparison object with categorized pack results
            
        Returns:
            Dictionary with save results and statistics
        """
        logger.info(f"Starting seat pack save with lineage for performance {self.performance_id}")
        
        save_results = {
            'saved_count': 0,
            'removed_count': 0,
            'transformation_count': 0,
            'errors': [],
            'saved_pack_ids': [],
            'updated_pack_ids': []
        }
        
        try:
            with transaction.atomic():
                # Cache related objects for efficiency
                self._cache_related_objects()
                
                # Step 1: Mark removed packs as inactive with proper delist reason
                removed_results = self.mark_removed_packs_inactive(
                    comparison.removed_packs, 
                    comparison.transformations
                )
                save_results['removed_count'] = removed_results['updated_count']
                save_results['updated_pack_ids'].extend(removed_results['updated_pack_ids'])
                
                # Step 2: Save new packs with proper pack_state and source_pack_ids
                new_results = self.save_new_seat_packs(
                    comparison.new_packs, 
                    comparison.lineage_tracking
                )
                save_results['saved_count'] = new_results['saved_count']
                save_results['saved_pack_ids'].extend(new_results['saved_pack_ids'])
                
                # Step 3: Update pack transformations
                transform_results = self.update_pack_transformations(comparison.transformations)
                save_results['transformation_count'] = transform_results['transformation_count']
                
                logger.info(f"Seat pack save completed successfully: {save_results}")
                
        except Exception as e:
            logger.error(f"Error during seat pack save: {e}", exc_info=True)
            save_results['errors'].append(str(e))
            raise
        
        return save_results
    
    def save_new_seat_packs(
        self, 
        new_packs: List[SeatPackData], 
        lineage_tracking: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save new seat packs to database with proper pack_state and source_pack_ids
        
        Args:
            new_packs: List of new seat pack data
            lineage_tracking: Lineage tracking information from comparison
            
        Returns:
            Dictionary with save results
        """
        logger.info(f"Saving {len(new_packs)} new seat packs")
        
        save_results = {
            'saved_count': 0,
            'saved_pack_ids': [],
            'errors': []
        }
        
        for pack_data in new_packs:
            try:
                # Determine pack creation context
                pack_state, source_pack_ids = self._determine_pack_creation_context(
                    pack_data, lineage_tracking
                )
                
                # Create seat pack with proper lineage
                seat_pack = self._create_seat_pack_with_lineage(
                    pack_data, pack_state, source_pack_ids
                )
                
                save_results['saved_count'] += 1
                save_results['saved_pack_ids'].append(seat_pack.internal_pack_id)
                
                logger.debug(f"Saved seat pack {seat_pack.internal_pack_id} with state {pack_state}")
                
            except Exception as e:
                error_msg = f"Error saving pack {pack_data.pack_id}: {e}"
                logger.error(error_msg)
                save_results['errors'].append(error_msg)
        
        logger.info(f"Successfully saved {save_results['saved_count']} new seat packs")
        return save_results
    
    def mark_removed_packs_inactive(
        self, 
        removed_packs: List[SeatPack], 
        transformations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Mark removed seat packs as inactive and set transformation details
        
        Args:
            removed_packs: List of removed seat packs to mark as inactive
            transformations: List of transformation records for audit trail
            
        Returns:
            Dictionary with update results
        """
        logger.info(f"🗑️ Marking {len(removed_packs)} removed seat packs as inactive")
        
        update_results = {
            'updated_count': 0,
            'updated_pack_ids': [],
            'errors': []
        }
        
        # Create transformation lookup for efficiency
        transformation_map = {t['old_pack_id']: t for t in transformations}
        
        for pack in removed_packs:
            try:
                # Set pack as inactive (dimension 1)
                pack.pack_status = 'inactive'
                
                # Set POS sync needed (dimension 2) 
                if pack.pos_status == 'active':
                    pack.pos_status = 'pending'  # Needs to be delisted from POS
                
                # Set transformation state (dimension 3 & 4)
                transformation = transformation_map.get(pack.internal_pack_id)
                if transformation:
                    pack.pack_state = 'transformed' if transformation.get('children') else 'delist'
                    pack.delist_reason = transformation.get('reason', 'transformed')
                else:
                    pack.pack_state = 'delist'
                    pack.delist_reason = 'vanished'
                
                # Mark as needing POS sync
                pack.synced_to_pos = False
                
                pack.save(update_fields=[
                    'pack_status', 'pos_status', 'pack_state', 
                    'delist_reason', 'synced_to_pos'
                ])
                
                update_results['updated_count'] += 1
                update_results['updated_pack_ids'].append(pack.internal_pack_id)
                
                logger.debug(f"Marked pack {pack.internal_pack_id} as inactive (pack_state={pack.pack_state}, delist_reason={pack.delist_reason})")
                
            except Exception as e:
                error_msg = f"Error marking pack {pack.internal_pack_id} as inactive: {str(e)}"
                logger.error(error_msg)
                update_results['errors'].append(error_msg)
        
        logger.info(f"Successfully marked {update_results['updated_count']} packs as inactive")
        return update_results
    
    def update_pack_transformations(self, transformations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Update pack transformation records for audit trail
        
        Args:
            transformations: List of transformation records
            
        Returns:
            Dictionary with transformation update results
        """
        logger.info(f"Recording {len(transformations)} pack transformations")
        
        transform_results = {
            'transformation_count': len(transformations),
            'recorded_transformations': transformations
        }
        
        # Transformations are already handled in the save/update processes
        # This method can be extended for additional transformation logging if needed
        
        return transform_results
    
    def _cache_related_objects(self):
        """Cache performance, zones, and levels for efficient lookups"""
        try:
            self.performance = Performance.objects.get(internal_performance_id=self.performance_id)
            
            # Cache zones for this performance
            zones = Zone.objects.filter(performance_id=self.performance)
            self.zones_cache = {zone.internal_zone_id: zone for zone in zones}
            
            # Cache levels for this performance
            levels = Level.objects.filter(performancelevel__performance=self.performance)
            self.levels_cache = {level.internal_level_id: level for level in levels}
            
            logger.debug(f"Cached {len(self.zones_cache)} zones and {len(self.levels_cache)} levels")
            
        except Performance.DoesNotExist:
            logger.error(f"Performance {self.performance_id} not found")
            raise ValueError(f"Performance {self.performance_id} not found")
    
    def _determine_pack_creation_context(
        self, 
        pack_data: SeatPackData, 
        lineage_tracking: Dict[str, Any]
    ) -> tuple[str, List[str]]:
        """
        Determine pack_state and source_pack_ids based on lineage tracking
        
        Args:
            pack_data: Seat pack data
            lineage_tracking: Lineage tracking information
            
        Returns:
            Tuple of (pack_state, source_pack_ids)
        """
        # Check if this pack is part of any transformations
        pack_location = f"{pack_data.zone_id}:{pack_data.row_label}"
        
        # Look for transformations at this location
        for transform_type, transforms in lineage_tracking.get('transformations_by_type', {}).items():
            for transform in transforms:
                if transform['location'] == pack_location:
                    # Check if this pack's seats overlap with transformation
                    pack_seats = set(pack_data.seat_keys)
                    transform_seats = set(transform['overlapping_seats'])
                    
                    if pack_seats.intersection(transform_seats):
                        return transform_type, transform['parent_packs']
        
        # Default: this is a newly created pack
        return 'create', []
    
    def _apply_venue_markup(self, original_price: Optional[Decimal], total_price: Optional[Decimal]) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Apply venue-specific markup to seat pack prices
        
        Args:
            original_price: Original pack price
            total_price: Original total price
            
        Returns:
            Tuple of (marked_up_pack_price, marked_up_total_price)
        """
        if not self.performance or not self.performance.venue:
            return original_price, total_price
            
        try:
            # Get venue from performance
            if hasattr(self.performance, 'venue'):
                venue = self.performance.venue
            else:
                # Fallback: get venue through performance
                venue = Venue.objects.filter(
                    events__performances=self.performance
                ).first()
                
            if not venue:
                logger.warning(f"No venue found for performance {self.performance_id}")
                return original_price, total_price
                
            # Check if venue has markup configuration
            if not venue.price_markup_type or venue.price_markup_value is None:
                logger.debug(f"No markup configuration for venue {venue.internal_venue_id}")
                return original_price, total_price
                
            markup_value = Decimal(str(venue.price_markup_value))
            
            # Apply markup to pack_price
            marked_up_pack_price = original_price
            if original_price is not None and original_price > 0:
                if venue.price_markup_type == 'dollar':
                    marked_up_pack_price = original_price + markup_value
                elif venue.price_markup_type == 'percentage':
                    marked_up_pack_price = original_price * (1 + markup_value / 100)
                    
            # Apply markup to total_price
            marked_up_total_price = total_price
            if total_price is not None and total_price > 0:
                if venue.price_markup_type == 'dollar':
                    marked_up_total_price = total_price + markup_value
                elif venue.price_markup_type == 'percentage':
                    marked_up_total_price = total_price * (1 + markup_value / 100)
                    
            logger.debug(f"Applied {venue.price_markup_type} markup of {markup_value} to venue {venue.internal_venue_id}: "
                        f"pack_price {original_price} -> {marked_up_pack_price}, "
                        f"total_price {total_price} -> {marked_up_total_price}")
                        
            return marked_up_pack_price, marked_up_total_price
            
        except Exception as e:
            logger.error(f"Error applying venue markup: {e}")
            return original_price, total_price
    
    def _create_seat_pack_with_lineage(
        self, 
        pack_data: SeatPackData, 
        pack_state: str, 
        source_pack_ids: List[str]
    ) -> SeatPack:
        """
        Create a new SeatPack database object with proper lineage
        
        Args:
            pack_data: Seat pack data
            pack_state: Pack creation state
            source_pack_ids: List of parent pack IDs
            
        Returns:
            Created SeatPack object
        """
        # Get related objects from cache
        zone = self.zones_cache.get(pack_data.zone_id)
        level = self.levels_cache.get(pack_data.level_id) if pack_data.level_id else None
        
        if not zone:
            raise ValueError(f"Zone {pack_data.zone_id} not found in cache")
        
        # Apply venue-specific markup to prices
        marked_up_pack_price, marked_up_total_price = self._apply_venue_markup(
            pack_data.pack_price, pack_data.total_price
        )
        
        # Create seat pack with lineage tracking
        seat_pack = SeatPack.objects.create(
            internal_pack_id=pack_data.pack_id,
            performance=self.performance,
            event=self.performance.event_id if self.performance.event_id else None,
            level=level,
            zone_id=zone,
            scrape_job_key_id=self.scrape_job_id,
            source_pack_id=pack_data.source_pack_id,
            source_website=self.source_website,
            row_label=pack_data.row_label,
            start_seat_number=pack_data.start_seat_number,
            end_seat_number=pack_data.end_seat_number,
            pack_size=pack_data.pack_size,
            pack_price=marked_up_pack_price,
            total_price=marked_up_total_price,
            seat_keys=pack_data.seat_keys,
            
            # Pack lineage and state tracking
            pack_status='active',
            pos_status='pending',
            pack_state=pack_state,
            source_pack_ids=source_pack_ids,
            synced_to_pos=False,
            
            # Timestamps
            created_at=timezone.now(),
            updated_at=timezone.now()
        )
        
        return seat_pack


def save_new_seat_packs_with_lineage(
    comparison: SeatPackComparison,
    performance_id: str,
    source_website: str,
    scrape_job_id: str
) -> Dict[str, Any]:
    """
    Convenience function for saving seat packs with lineage tracking
    
    Args:
        comparison: SeatPackComparison object with categorized pack results
        performance_id: Internal performance ID
        source_website: Source website identifier
        scrape_job_id: Current scrape job ID
        
    Returns:
        Dictionary with save results
    """
    saver = SeatPackSaver(performance_id, source_website, scrape_job_id)
    return saver.save_new_seat_packs_with_lineage(comparison)