"""
Seat Pack Comparator for POS Sync Workflow

This module compares newly generated seat packs with existing ones in the database
to determine what needs to be synced, while preserving pack lineage and creation tracking.
"""

import logging
from typing import Dict, List, Set, Any, Optional, Tuple
from django.db.models import QuerySet
from ..models.seat_packs import SeatPack
from .data_schemas import SeatPackData

logger = logging.getLogger(__name__)


class SeatPackComparison:
    """Results of seat pack comparison between new and existing packs"""
    
    def __init__(self):
        self.new_packs: List[SeatPackData] = []
        self.unchanged_packs: List[SeatPack] = []
        self.removed_packs: List[SeatPack] = []
        self.transformations: List[Dict[str, Any]] = []
        self.lineage_tracking: Dict[str, Any] = {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert comparison results to dictionary for easy handling"""
        return {
            'new_packs': self.new_packs,
            'unchanged_packs': self.unchanged_packs,
            'removed_packs': self.removed_packs,
            'transformations': self.transformations,
            'lineage_tracking': self.lineage_tracking,
            'summary': {
                'new_count': len(self.new_packs),
                'unchanged_count': len(self.unchanged_packs),
                'removed_count': len(self.removed_packs),
                'transformations_count': len(self.transformations)
            }
        }


class SeatPackComparator:
    """
    Compares newly generated seat packs with existing database packs
    to determine sync requirements while preserving pack lineage
    """
    
    def __init__(self, performance_id: str, source_website: str):
        """
        Initialize the comparator
        
        Args:
            performance_id: Internal performance ID
            source_website: Source website identifier
        """
        self.performance_id = performance_id
        self.source_website = source_website
        
    def compare_seat_packs(
        self, 
        new_packs: List[SeatPackData], 
        existing_packs: Optional[QuerySet] = None
    ) -> SeatPackComparison:
        """
        Compare new seat packs with existing ones in the database
        
        Args:
            new_packs: Newly generated seat pack data
            existing_packs: Existing packs queryset (optional, will query if not provided)
            
        Returns:
            SeatPackComparison object with categorized results
        """
        logger.info(f"Starting seat pack comparison for performance {self.performance_id}")
        logger.info(f"New packs count: {len(new_packs)}")
        
        # Get existing packs if not provided
        if existing_packs is None:
            existing_packs = self._get_existing_packs()
        
        existing_packs_list = list(existing_packs)
        logger.info(f"Existing packs count: {len(existing_packs_list)}")
        
        # Create comparison result
        comparison = SeatPackComparison()
        
        # Create sets for efficient comparison
        new_pack_ids = {self._generate_pack_key(pack) for pack in new_packs}
        existing_pack_map = {self._generate_pack_key_from_db(pack): pack for pack in existing_packs_list}
        existing_pack_ids = set(existing_pack_map.keys())
        
        # Identify different categories
        comparison.new_packs = self.identify_new_packs(new_packs, existing_pack_ids)
        comparison.unchanged_packs = self.identify_unchanged_packs(new_pack_ids, existing_pack_map)
        comparison.removed_packs = self.identify_removed_packs(new_pack_ids, existing_pack_map)
        
        # Track transformations and lineage
        comparison.transformations = self._identify_transformations(
            comparison.new_packs, 
            comparison.removed_packs
        )
        comparison.lineage_tracking = self._build_lineage_tracking(comparison.transformations)
        
        logger.info(f"Comparison results: {comparison.to_dict()['summary']}")
        return comparison
    
    def identify_new_packs(
        self, 
        new_packs: List[SeatPackData], 
        existing_pack_ids: Set[str]
    ) -> List[SeatPackData]:
        """
        Identify seat packs that are new and don't exist in the database
        
        Args:
            new_packs: List of newly generated seat packs
            existing_pack_ids: Set of existing pack identifiers
            
        Returns:
            List of new seat packs that need to be created
        """
        new_packs_filtered = []
        
        for pack in new_packs:
            pack_key = self._generate_pack_key(pack)
            if pack_key not in existing_pack_ids:
                new_packs_filtered.append(pack)
                logger.debug(f"New pack identified: {pack_key}")
        
        logger.info(f"Identified {len(new_packs_filtered)} new packs")
        return new_packs_filtered
    
    def identify_unchanged_packs(
        self, 
        new_pack_ids: Set[str], 
        existing_pack_map: Dict[str, SeatPack]
    ) -> List[SeatPack]:
        """
        Identify seat packs that exist in both new and existing sets (unchanged)
        
        Args:
            new_pack_ids: Set of new pack identifiers
            existing_pack_map: Map of existing pack identifiers to SeatPack objects
            
        Returns:
            List of unchanged seat packs
        """
        unchanged_packs = []
        
        for pack_id in new_pack_ids:
            if pack_id in existing_pack_map:
                unchanged_packs.append(existing_pack_map[pack_id])
                logger.debug(f"Unchanged pack: {pack_id}")
        
        logger.info(f"Identified {len(unchanged_packs)} unchanged packs")
        return unchanged_packs
    
    def identify_removed_packs(
        self, 
        new_pack_ids: Set[str], 
        existing_pack_map: Dict[str, SeatPack]
    ) -> List[SeatPack]:
        """
        Identify seat packs that exist in database but not in new scrape (removed/vanished)
        
        Args:
            new_pack_ids: Set of new pack identifiers
            existing_pack_map: Map of existing pack identifiers to SeatPack objects
            
        Returns:
            List of removed seat packs that need delisting
        """
        removed_packs = []
        
        for pack_id, pack in existing_pack_map.items():
            if pack_id not in new_pack_ids and pack.pack_status == 'active':
                removed_packs.append(pack)
                logger.debug(f"Removed pack: {pack_id}")
        
        logger.info(f"Identified {len(removed_packs)} removed packs")
        return removed_packs
    
    def _get_existing_packs(self) -> QuerySet:
        """Get existing active seat packs for the performance"""
        return SeatPack.objects.filter(
            performance=self.performance_id,
            pack_status='active'
        ).select_related('zone_id', 'level', 'pos_listing')
    
    def _generate_pack_key(self, pack: SeatPackData) -> str:
        """
        Generate a unique key for a seat pack for comparison
        
        Args:
            pack: SeatPackData object
            
        Returns:
            Unique string identifier for the pack
        """
        # Use zone, row, and seat range to create unique identifier
        sorted_seat_keys = sorted(pack.seat_keys)
        return f"{pack.zone_id}:{pack.row_label}:{sorted_seat_keys[0]}:{sorted_seat_keys[-1]}"
    
    def _generate_pack_key_from_db(self, pack: SeatPack) -> str:
        """
        Generate a unique key for a database seat pack for comparison
        
        Args:
            pack: SeatPack database object
            
        Returns:
            Unique string identifier for the pack
        """
        # Use zone, row, and seat range to create unique identifier
        sorted_seat_keys = sorted(pack.seat_keys)
        zone_id = pack.zone_id.internal_zone_id if pack.zone_id else 'unknown'
        return f"{zone_id}:{pack.row_label}:{sorted_seat_keys[0]}:{sorted_seat_keys[-1]}"
    
    def _identify_transformations(
        self, 
        new_packs: List[SeatPackData], 
        removed_packs: List[SeatPack]
    ) -> List[Dict[str, Any]]:
        """
        Identify pack transformations (splits, merges) by analyzing overlapping seats
        
        Args:
            new_packs: List of new seat packs
            removed_packs: List of removed seat packs
            
        Returns:
            List of transformation records
        """
        transformations = []
        
        # Group by zone and row for efficient comparison
        removed_by_location = self._group_packs_by_location(removed_packs)
        new_by_location = self._group_new_packs_by_location(new_packs)
        
        # Identify transformations within each location
        for location, removed_packs_in_location in removed_by_location.items():
            if location in new_by_location:
                new_packs_in_location = new_by_location[location]
                location_transformations = self._analyze_location_transformations(
                    removed_packs_in_location, 
                    new_packs_in_location,
                    location
                )
                transformations.extend(location_transformations)
        
        logger.info(f"Identified {len(transformations)} pack transformations")
        return transformations
    
    def _group_packs_by_location(self, packs: List[SeatPack]) -> Dict[str, List[SeatPack]]:
        """Group database packs by zone and row"""
        grouped = {}
        for pack in packs:
            zone_id = pack.zone_id.internal_zone_id if pack.zone_id else 'unknown'
            location = f"{zone_id}:{pack.row_label}"
            if location not in grouped:
                grouped[location] = []
            grouped[location].append(pack)
        return grouped
    
    def _group_new_packs_by_location(self, packs: List[SeatPackData]) -> Dict[str, List[SeatPackData]]:
        """Group new packs by zone and row"""
        grouped = {}
        for pack in packs:
            location = f"{pack.zone_id}:{pack.row_label}"
            if location not in grouped:
                grouped[location] = []
            grouped[location].append(pack)
        return grouped
    
    def _analyze_location_transformations(
        self, 
        removed_packs: List[SeatPack], 
        new_packs: List[SeatPackData],
        location: str
    ) -> List[Dict[str, Any]]:
        """
        Analyze transformations within a specific zone/row location
        
        Args:
            removed_packs: Removed packs in this location
            new_packs: New packs in this location
            location: Location identifier (zone:row)
            
        Returns:
            List of transformation records for this location
        """
        transformations = []
        
        # Get all seats involved
        removed_seats = set()
        for pack in removed_packs:
            removed_seats.update(pack.seat_keys)
        
        new_seats = set()
        for pack in new_packs:
            new_seats.update(pack.seat_keys)
        
        # Check for overlapping seats (indicating transformation)
        overlapping_seats = removed_seats.intersection(new_seats)
        
        if overlapping_seats:
            transformation_type = self._determine_transformation_type(
                removed_packs, new_packs, overlapping_seats
            )
            
            transformations.append({
                'type': transformation_type,
                'location': location,
                'parent_packs': [pack.internal_pack_id for pack in removed_packs],
                'overlapping_seats': list(overlapping_seats),
                'removed_pack_count': len(removed_packs),
                'new_pack_count': len(new_packs),
                'seat_overlap_ratio': len(overlapping_seats) / len(removed_seats) if removed_seats else 0
            })
        
        return transformations
    
    def _determine_transformation_type(
        self, 
        removed_packs: List[SeatPack], 
        new_packs: List[SeatPackData],
        overlapping_seats: Set[str]
    ) -> str:
        """
        Determine the type of transformation based on pack counts and seat overlap
        
        Returns:
            Transformation type: 'split', 'merge', 'shrink', or 'complex'
        """
        removed_count = len(removed_packs)
        new_count = len(new_packs)
        
        if removed_count == 1 and new_count > 1:
            return 'split'
        elif removed_count > 1 and new_count == 1:
            return 'merge'
        elif removed_count == 1 and new_count == 1:
            return 'shrink'
        else:
            return 'complex'
    
    def _build_lineage_tracking(self, transformations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Build lineage tracking information for pack transformations
        
        Args:
            transformations: List of transformation records
            
        Returns:
            Dictionary with lineage tracking information
        """
        lineage = {
            'transformations_by_type': {},
            'parent_child_map': {},
            'transformation_summary': {
                'total_transformations': len(transformations),
                'splits': 0,
                'merges': 0,
                'shrinks': 0,
                'complex': 0
            }
        }
        
        for transformation in transformations:
            transform_type = transformation['type']
            
            # Count by type
            lineage['transformation_summary'][f"{transform_type}s"] += 1
            
            # Group by type
            if transform_type not in lineage['transformations_by_type']:
                lineage['transformations_by_type'][transform_type] = []
            lineage['transformations_by_type'][transform_type].append(transformation)
            
            # Build parent-child relationships
            for parent_id in transformation['parent_packs']:
                lineage['parent_child_map'][parent_id] = {
                    'transformation_type': transform_type,
                    'location': transformation['location'],
                    'overlapping_seats': transformation['overlapping_seats']
                }
        
        return lineage