"""
Seat Pack Structure Change Handler Module

This module handles the bulk delisting and regeneration of seat packs when
venue seating structures change between 'odd_even' and 'consecutive' arrangements.

Business Requirements:
- When venue seat structure changes, all active seat packs for that venue
  across all performances must be marked as delisted
- Delist reason must indicate the specific structure change
- Process must be atomic and handle large numbers of seat packs efficiently
- Must respect manually_delisted flag to avoid overriding manual interventions

Technical Approach:
- Bulk operations for performance with large datasets
- Transaction-safe operations to maintain data integrity
- Comprehensive logging for audit trails
- Integration with existing seat pack sync infrastructure

Created: 2025-07-07
Author: Claude Code Assistant
Purpose: Handle bulk seat pack delisting when venue structure changes
"""

from django.db import transaction, models
from django.utils import timezone
from typing import Dict, Any, List, Optional
import logging

from ..models.base import Venue, Performance, Seat, Section, Level, Zone
from ..models.monitoring import ScrapeJob
from ..models.monitoring import ScrapeJob
from ..models.seat_packs import SeatPack
from .venue_structure_change_detector import VenueStructureChangeDetector
from .seat_pack_generator import generate_seat_packs
from .data_schemas import SeatData, SectionData, SeatPackData

logger = logging.getLogger(__name__)


class SeatPackStructureHandler:
    """
    Handles bulk delisting and regeneration of seat packs when venue structure changes.
    
    This class provides the core functionality for managing seat pack lifecycle
    when a venue's seating arrangement changes from odd_even to consecutive or vice versa.
    
    The handler ensures data integrity through atomic transactions and provides
    comprehensive logging for audit and debugging purposes.
    """
    
    def __init__(self):
        """Initialize the seat pack structure handler."""
        self.logger = logger
        self.detector = VenueStructureChangeDetector()
    
    def handle_venue_structure_change(self, venue: Venue, force_process: bool = False) -> Dict[str, Any]:
        """
        Handle the complete process of detecting and processing venue structure changes.
        
        Args:
            venue: The Venue model instance to process
            force_process: If True, process even if no change detected (for manual triggers)
            
        Returns:
            Dictionary containing processing results and statistics
            
        Result Dictionary Structure:
        {
            'venue_id': str,
            'venue_name': str,
            'change_detected': bool,
            'change_info': dict or None,
            'packs_delisted': int,
            'performances_affected': int,
            'processing_time': float,
            'success': bool,
            'error_message': str or None
        }
        """
        start_time = timezone.now()
        result = {
            'venue_id': venue.internal_venue_id,
            'venue_name': venue.name,
            'change_detected': False,
            'change_info': None,
            'packs_delisted': 0,
            'performances_affected': 0,
            'processing_time': 0.0,
            'success': False,
            'error_message': None
        }
        
        try:
            # Detect structure change
            change_info = self.detector.detect_structure_change(venue)
            
            if not change_info and not force_process:
                result['success'] = True
                result['processing_time'] = (timezone.now() - start_time).total_seconds()
                self.logger.debug(f"No structure change detected for venue {venue.internal_venue_id}")
                return result
                
            result['change_detected'] = bool(change_info)
            result['change_info'] = change_info
            
            # Process the structure change
            delist_result = self.delist_venue_seat_packs(venue, change_info)
            
            result['packs_delisted'] = delist_result['packs_delisted']
            result['performances_affected'] = delist_result['performances_affected']
            result['success'] = delist_result['success']
            result['error_message'] = delist_result.get('error_message')
            
            # Update venue's previous_seat_structure to current value
            if result['success'] and change_info:
                self._update_venue_previous_structure(venue)

                # After delisting, regenerate seat packs for affected performances
                # Need to get all performances for this venue
                venue_performances = Performance.objects.filter(
                    venue_id=venue,
                    is_active=True
                )
                
                # Create a dummy venue_prefix_map for now. In a real scenario, this might come from config.
                venue_prefix_map = {venue.source_website: venue.source_website[:3].lower() if venue.source_website else "unk"}

                total_regenerated_packs = 0
                for performance in venue_performances:
                    regenerated_count = self.regenerate_seat_packs_for_performance(performance, venue_prefix_map)
                    total_regenerated_packs += regenerated_count
                
                result['packs_regenerated'] = total_regenerated_packs
                self.logger.info(f"Total {total_regenerated_packs} new seat packs regenerated for venue {venue.internal_venue_id}.")
                
        except Exception as e:
            result['error_message'] = str(e)
            result['success'] = False
            self.logger.error(f"Error processing venue structure change for {venue.internal_venue_id}: {e}")
            
        result['processing_time'] = (timezone.now() - start_time).total_seconds()
        return result

    def regenerate_seat_packs_for_performance(self, performance: Performance, venue_prefix_map: Dict[str, str]) -> int:
        """
        Generates new seat packs for a given performance using existing seat data.
        
        Args:
            performance: The Performance model instance for which to regenerate seat packs.
            venue_prefix_map: Dictionary mapping source_website to short prefix for pack IDs.
            
        Returns:
            The number of new seat packs generated.
        """
        generated_count = 0
        try:
            # 1. Fetch all relevant Seat and Section data for this performance
            # Seats are linked to Section, Section to Level, Level to Performance
            
            # Fetch Sections and Levels first to build the hierarchy
            sections_query = Section.objects.filter(
                level_id__performance_id=performance
            ).select_related('level_id')

            all_sections_data: List[SectionData] = []
            for section_obj in sections_query:
                all_sections_data.append(SectionData(
                    section_id=section_obj.internal_section_id,
                    level_id=section_obj.level_id.internal_level_id,
                    source_website=section_obj.source_website,
                    name=section_obj.name,
                    raw_name=section_obj.raw_name,
                    section_type=section_obj.section_type,
                    display_order=section_obj.display_order,
                    numbering_scheme=section_obj.level_id.performance_id.venue_id.seat_structure or "consecutive" # Use venue's current seat structure
                ))

            # Fetch Seats
            seats_query = Seat.objects.filter(
                section_id__level_id__performance_id=performance
            ).select_related('section_id', 'zone_id', 'section_id__level_id')

            all_seats_data: List[SeatData] = []
            for seat_obj in seats_query:
                all_seats_data.append(SeatData(
                    seat_id=seat_obj.internal_seat_id,
                    section_id=seat_obj.section_id.internal_section_id,
                    zone_id=seat_obj.zone_id.internal_zone_id,
                    source_website=seat_obj.source_website,
                    row_label=seat_obj.row_label,
                    seat_number=seat_obj.seat_number,
                    seat_type=seat_obj.seat_type,
                    x_coord=seat_obj.x_coord,
                    y_coord=seat_obj.y_coord,
                    status=seat_obj.current_status,
                    price=seat_obj.current_price,
                    available=seat_obj.is_available(),
                    level_id=seat_obj.section_id.level_id.internal_level_id
                ))

            if not all_seats_data:
                self.logger.info(f"No seat data found for performance {performance.internal_performance_id}. Skipping pack regeneration.")
                return 0

            # 2. Generate new seat packs using the generator function
            # Use venue-specific min_pack_size configuration
            venue = performance.venue_id
            min_pack_size = 2  # Default for most venues
            if venue and venue.source_website == "broadway_sf":
                min_pack_size = 2  # Broadway SF requires minimum 2 seats per pack
            elif venue and venue.source_website == "washington_pavilion":
                min_pack_size = 2  # Washington Pavilion requires minimum 2 seats per pack
            # Add other venue-specific configurations as needed
            
            new_seat_packs_data: List[SeatPackData] = generate_seat_packs(
                all_seats=all_seats_data,
                all_sections=all_sections_data,
                performance_id=performance.internal_performance_id,
                venue_prefix_map=venue_prefix_map,
                min_pack_size=min_pack_size,  # Use venue-specific configuration
                packing_strategy="maximal"  # Use maximal strategy by default
            )

            # 3. Save the newly generated seat packs to the database
            # Ensure old packs are not reactivated. New packs will have new internal_pack_ids.
            seat_packs_to_create = []
            for pack_data in new_seat_packs_data:
                # Fetch the actual Zone and ScrapeJob objects
                zone_obj = Zone.objects.get(internal_zone_id=pack_data.zone_id)
                # Find the latest ScrapeJob for this performance's venue and source_website
                scrape_job_obj = ScrapeJob.objects.filter(
                    performance_id__venue_id=performance.venue_id,
                    source_website=performance.source_website,
                    scrape_success=True
                ).order_by('-scraped_at_utc').first()

                if not scrape_job_obj:
                    self.logger.warning(f"No completed ScrapeJob found for venue {performance.venue_id.internal_venue_id} and source {performance.source_website}. Skipping pack {pack_data.pack_id}.")
                    continue


                seat_packs_to_create.append(SeatPack(
                    internal_pack_id=pack_data.pack_id,
                    zone_id=zone_obj,
                    scrape_job_key=scrape_job_obj, # This needs to be correctly linked
                    source_website=pack_data.source_website,
                    row_label=pack_data.row_label,
                    start_seat_number=pack_data.start_seat_number,
                    end_seat_number=pack_data.end_seat_number,
                    pack_size=pack_data.pack_size,
                    pack_price=pack_data.pack_price,
                    total_price=pack_data.total_price,
                    seat_keys=pack_data.seat_ids,
                    is_active=True, # Newly generated packs are active
                    creation_event='create', # Or 'regenerate' if we add that choice
                    updated_at=timezone.now()
                ))
            
            if seat_packs_to_create:
                SeatPack.objects.bulk_create(seat_packs_to_create, ignore_conflicts=True)
                generated_count = len(seat_packs_to_create)
                self.logger.info(f"Generated {generated_count} new seat packs for performance {performance.internal_performance_id}.")
            else:
                self.logger.info(f"No new seat packs generated for performance {performance.internal_performance_id}.")

        except Exception as e:
            self.logger.error(f"Error regenerating seat packs for performance {performance.internal_performance_id}: {e}")
            raise # Re-raise to be caught by the main handler

        return generated_count

    def delist_venue_seat_packs(self, venue: Venue, change_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Delist all active seat packs for a venue across all performances.
        
        Args:
            venue: The Venue model instance whose seat packs should be delisted
            change_info: Optional change information for detailed logging
            
        Returns:
            Dictionary containing operation results
        """
        result = {
            'packs_delisted': 0,
            'performances_affected': 0,
            'success': False,
            'error_message': None
        }
        
        try:
            with transaction.atomic():
                # Find all active seat packs for this venue
                # We need to traverse through Performance -> Zone -> SeatPack
                venue_performances = Performance.objects.filter(
                    venue_id=venue,
                    is_active=True
                )
                
                performance_ids = list(venue_performances.values_list('internal_performance_id', flat=True))
                result['performances_affected'] = len(performance_ids)
                
                if not performance_ids:
                    self.logger.info(f"No active performances found for venue {venue.internal_venue_id}")
                    result['success'] = True
                    return result
                
                # Find all active seat packs for these performances
                # SeatPack -> Zone -> Performance relationship
                active_packs = SeatPack.objects.filter(
                    zone_id__performance_id__internal_performance_id__in=performance_ids,
                    is_active=True,
                    manually_delisted=False  # Respect manual delisting flag
                )
                
                pack_count = active_packs.count()
                
                if pack_count == 0:
                    self.logger.info(f"No active seat packs found for venue {venue.internal_venue_id}")
                    result['success'] = True
                    return result
                
                # Prepare delist reason
                delist_reason = 'structure_change'
                if change_info:
                    delist_reason = self.detector.prepare_delist_reason(change_info)
                
                # Bulk update to delist packs
                updated_count = active_packs.update(
                    is_active=False,
                    delist_reason='structure_change',
                    updated_at=timezone.now()
                )
                
                result['packs_delisted'] = updated_count
                result['success'] = True
                
                self.logger.info(
                    f"Successfully delisted {updated_count} seat packs for venue {venue.internal_venue_id} "
                    f"across {len(performance_ids)} performances. "
                    f"Reason: {delist_reason}"
                )
                
        except Exception as e:
            result['error_message'] = str(e)
            result['success'] = False
            self.logger.error(f"Error delisting seat packs for venue {venue.internal_venue_id}: {e}")
            
        return result
    
    def get_affected_seat_packs_count(self, venue: Venue) -> Dict[str, int]:
        """
        Get count of seat packs that would be affected by a structure change.
        
        Args:
            venue: The Venue model instance to check
            
        Returns:
            Dictionary with counts of different pack categories
        """
        try:
            # Find all performances for this venue
            venue_performances = Performance.objects.filter(
                venue_id=venue,
                is_active=True
            )
            
            performance_ids = list(venue_performances.values_list('internal_performance_id', flat=True))
            
            if not performance_ids:
                return {
                    'total_active_packs': 0,
                    'manually_delisted_packs': 0,
                    'auto_delisted_packs': 0,
                    'performances_count': 0
                }
            
            # Count different categories of packs
            total_active = SeatPack.objects.filter(
                zone_id__performance_id__in=performance_ids,
                is_active=True
            ).count()
            
            manually_delisted = SeatPack.objects.filter(
                zone_id__performance_id__in=performance_ids,
                is_active=True,
                manually_delisted=True
            ).count()
            
            auto_delisted = total_active - manually_delisted
            
            return {
                'total_active_packs': total_active,
                'manually_delisted_packs': manually_delisted,
                'auto_delisted_packs': auto_delisted,
                'performances_count': len(performance_ids)
            }
            
        except Exception as e:
            self.logger.error(f"Error counting affected seat packs for venue {venue.internal_venue_id}: {e}")
            return {
                'total_active_packs': 0,
                'manually_delisted_packs': 0,
                'auto_delisted_packs': 0,
                'performances_count': 0
            }
    
    def _update_venue_previous_structure(self, venue: Venue) -> None:
        """
        Update venue's previous_seat_structure to current seat_structure value.
        
        This method is called after successful processing to mark the change as handled.
        
        Args:
            venue: The Venue model instance to update
        """
        try:
            venue.previous_seat_structure = venue.seat_structure
            venue.save(update_fields=['previous_seat_structure', 'updated_at'])
            
            self.logger.info(
                f"Updated venue {venue.internal_venue_id} previous_seat_structure to {venue.seat_structure}"
            )
            
        except Exception as e:
            self.logger.error(f"Error updating venue previous_seat_structure: {e}")
            raise
    
    def bulk_process_venues(self, venues: List[Venue]) -> Dict[str, Any]:
        """
        Process multiple venues for structure changes.
        
        Args:
            venues: List of Venue model instances to process
            
        Returns:
            Dictionary containing bulk processing results
        """
        results = []
        summary = {
            'total_venues': len(venues),
            'venues_with_changes': 0,
            'total_packs_delisted': 0,
            'total_performances_affected': 0,
            'successful_processes': 0,
            'failed_processes': 0,
            'venue_results': []
        }
        
        for venue in venues:
            result = self.handle_venue_structure_change(venue)
            results.append(result)
            
            if result['change_detected']:
                summary['venues_with_changes'] += 1
                
            if result['success']:
                summary['successful_processes'] += 1
                summary['total_packs_delisted'] += result['packs_delisted']
                summary['total_performances_affected'] += result['performances_affected']
            else:
                summary['failed_processes'] += 1
                
        summary['venue_results'] = results
        
        self.logger.info(
            f"Bulk processed {len(venues)} venues. "
            f"Changes detected: {summary['venues_with_changes']}, "
            f"Successful: {summary['successful_processes']}, "
            f"Failed: {summary['failed_processes']}"
        )
        
        return summary