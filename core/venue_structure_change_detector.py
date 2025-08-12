"""
Venue Structure Change Detection Module

This module handles the detection of venue seat structure changes, specifically
transitions between 'odd_even' and 'consecutive' seating arrangements.

Business Requirements:
- When a venue's seating structure changes (odd_even ↔ consecutive), all active
  seat packs across all performances for that venue must be delisted
- New seat packs must be generated based on the new structure
- Changes must be tracked with appropriate audit trails

Technical Approach:
- Compare current seat_structure with previous_seat_structure field
- Detect meaningful changes that affect seat pack generation
- Provide structured data for downstream processing

Created: 2025-07-07
Author: Claude Code Assistant
Purpose: Automated detection of venue seat structure changes to trigger seat pack synchronization
"""

from django.db import models
from django.utils import timezone
from typing import Optional, Dict, Any, List
import logging

from ..models.base import Venue

logger = logging.getLogger(__name__)


class VenueStructureChangeDetector:
    """
    Detects changes in venue seat structure that require seat pack synchronization.
    
    This class specifically handles transitions between 'odd_even' and 'consecutive'
    seating arrangements, which fundamentally change how seats are grouped into packs.
    
    The detector provides structured information about changes to enable downstream
    processing systems to appropriately handle seat pack delisting and regeneration.
    """
    
    def __init__(self):
        """Initialize the venue structure change detector."""
        self.logger = logger
    
    def detect_structure_change(self, venue: Venue) -> Optional[Dict[str, Any]]:
        """
        Detect if a venue's seat structure has changed from its previous value.
        
        Args:
            venue: The Venue model instance to check for structure changes
            
        Returns:
            Dict containing change information if a change is detected, None otherwise
            
        Change Dictionary Structure:
        {
            'venue_id': str,
            'venue_name': str,
            'old_structure': str,
            'new_structure': str,
            'change_detected_at': datetime,
            'requires_seat_pack_sync': bool,
            'change_description': str
        }
        """
        # Check if both current and previous structures exist
        if not venue.seat_structure:
            self.logger.debug(f"Venue {venue.internal_venue_id} has no current seat structure")
            return None
            
        # If no previous structure recorded, this is not a change (new venue or first time setting)
        if not venue.previous_seat_structure:
            self.logger.debug(f"Venue {venue.internal_venue_id} has no previous seat structure recorded")
            return None
            
        # Check if structure has actually changed
        if venue.seat_structure == venue.previous_seat_structure:
            self.logger.debug(f"Venue {venue.internal_venue_id} structure unchanged: {venue.seat_structure}")
            return None
            
        # Structure change detected - build change information
        change_info = {
            'venue_id': venue.internal_venue_id,
            'venue_name': venue.name,
            'old_structure': venue.previous_seat_structure,
            'new_structure': venue.seat_structure,
            'change_detected_at': timezone.now(),
            'requires_seat_pack_sync': True,
            'change_description': f"Seat structure changed from {venue.previous_seat_structure} to {venue.seat_structure}"
        }
        
        self.logger.info(
            f"Venue structure change detected: {venue.name} ({venue.internal_venue_id}) "
            f"changed from {venue.previous_seat_structure} to {venue.seat_structure}"
        )
        
        return change_info
    
    def check_multiple_venues(self, venues: List[Venue]) -> List[Dict[str, Any]]:
        """
        Check multiple venues for structure changes.
        
        Args:
            venues: List of Venue model instances to check
            
        Returns:
            List of change information dictionaries for venues with detected changes
        """
        changes = []
        
        for venue in venues:
            change_info = self.detect_structure_change(venue)
            if change_info:
                changes.append(change_info)
                
        if changes:
            self.logger.info(f"Detected {len(changes)} venue structure changes out of {len(venues)} venues checked")
        else:
            self.logger.debug(f"No structure changes detected in {len(venues)} venues checked")
            
        return changes
    
    def is_valid_structure_transition(self, old_structure: str, new_structure: str) -> bool:
        """
        Validate that a structure transition is valid and meaningful.
        
        Args:
            old_structure: The previous seat structure value
            new_structure: The new seat structure value
            
        Returns:
            True if this is a valid transition that requires processing
        """
        valid_structures = {'odd_even', 'consecutive'}
        
        # Both structures must be valid
        if old_structure not in valid_structures or new_structure not in valid_structures:
            return False
            
        # Must be different structures
        if old_structure == new_structure:
            return False
            
        return True
    
    def get_change_impact_summary(self, change_info: Dict[str, Any]) -> str:
        """
        Generate a human-readable summary of the change impact.
        
        Args:
            change_info: Change information dictionary from detect_structure_change
            
        Returns:
            Human-readable summary string
        """
        if not change_info:
            return "No change detected"
            
        return (
            f"Venue '{change_info['venue_name']}' seat structure changed from "
            f"{change_info['old_structure']} to {change_info['new_structure']}. "
            f"This change requires all active seat packs for this venue to be delisted "
            f"and regenerated based on the new seating arrangement."
        )
    
    def prepare_delist_reason(self, change_info: Dict[str, Any]) -> str:
        """
        Prepare the delist reason string for seat packs affected by structure change.
        
        Args:
            change_info: Change information dictionary from detect_structure_change
            
        Returns:
            Formatted delist reason string
        """
        if not change_info:
            return "structure_change"
            
        return (
            f"Seat structure changed from {change_info['old_structure']} "
            f"to {change_info['new_structure']}"
        )