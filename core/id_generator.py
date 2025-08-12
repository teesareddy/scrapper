"""
Internal ID generation utility for scrapers.
Generates deterministic internal IDs based on content data, not source IDs.
"""

import hashlib
from typing import Optional
from datetime import datetime


class InternalIDGenerator:
    """
    Generates deterministic internal IDs for scraped entities.
    
    Logic:
    1. If source_id exists and is not empty/0: use prefix + source_id
    2. Otherwise: generate deterministic hash from content attributes
    """
    
    @staticmethod
    def _create_hash(content: str, length: int = 8) -> str:
        """Create a deterministic hash from content string"""
        hash_object = hashlib.md5(content.encode('utf-8'))
        return hash_object.hexdigest()[:length]
    
    @staticmethod
    def _is_valid_source_id(source_id: Optional[str]) -> bool:
        """Check if source_id is valid (not empty, None, or '0')"""
        if not source_id:
            return False
        if source_id.strip() in ['', '0']:
            return False
        return True
    
    @classmethod
    def generate_venue_id(cls, prefix: str, venue_data) -> str:
        """Generate internal venue ID"""
        # Try using source_id first
        if cls._is_valid_source_id(venue_data.source_venue_id):
            return f"{prefix}_venue_{venue_data.source_venue_id}"
        
        # Fallback: content-based hash
        content = (
            f"{venue_data.name.lower().strip()}"
            f"{venue_data.city.lower().strip()}"
            f"{venue_data.state.lower().strip()}"
            f"{venue_data.country.lower().strip()}"
            f"{venue_data.address.lower().strip() if venue_data.address else ''}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_venue_{hash_value}"
    
    @classmethod
    def generate_event_id(cls, prefix: str, event_data) -> str:
        """Generate internal event ID"""
        # Try using source_id first
        if cls._is_valid_source_id(event_data.source_event_id):
            return f"{prefix}_event_{event_data.source_event_id}"
        
        # Fallback: content-based hash
        content = (
            f"{event_data.name.lower().strip()}"
            f"{event_data.event_type.lower().strip() if event_data.event_type else ''}"
            f"{event_data.title.lower().strip() if event_data.title else ''}"
            f"{event_data.description.lower().strip() if event_data.description else ''}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_event_{hash_value}"
    
    @classmethod
    def generate_performance_id(cls, prefix: str, performance_data, event_internal_id: str, venue_internal_id: str) -> str:
        """Generate internal performance ID"""
        # Try using source_id first
        if cls._is_valid_source_id(performance_data.source_performance_id):
            return f"{prefix}_perf_{performance_data.source_performance_id}"
        
        # Fallback: content-based hash including parent IDs
        datetime_str = performance_data.performance_datetime_utc.isoformat() if performance_data.performance_datetime_utc else ""
        content = (
            f"{event_internal_id}"
            f"{venue_internal_id}"
            f"{datetime_str}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_perf_{hash_value}"
    
    @classmethod
    def generate_level_id(cls, prefix: str, level_data, performance_internal_id: str) -> str:
        """Generate internal level ID"""
        # Try using level_id as source_id if it looks like a source identifier
        if cls._is_valid_source_id(level_data.level_id):
            return f"{prefix}_level_{level_data.level_id}"
        
        # Fallback: content-based hash
        content = (
            f"{level_data.name.lower().strip()}"
            f"{performance_internal_id}"
            f"{level_data.level_number if level_data.level_number else 0}"
            f"{level_data.level_type.lower().strip() if level_data.level_type else ''}"
            f"{level_data.display_order}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_level_{hash_value}"
    
    @classmethod
    def generate_zone_id(cls, prefix: str, zone_data, performance_internal_id: str) -> str:
        """Generate internal zone ID"""
        # Try using zone_id as source_id if it looks like a source identifier
        if cls._is_valid_source_id(zone_data.zone_id):
            return f"{prefix}_zone_{zone_data.zone_id}"
        
        # Fallback: content-based hash
        content = (
            f"{zone_data.name.lower().strip()}"
            f"{zone_data.raw_identifier.lower().strip() if zone_data.raw_identifier else ''}"
            f"{performance_internal_id}"
            f"{zone_data.zone_type.lower().strip() if zone_data.zone_type else ''}"
            f"{zone_data.view_type.lower().strip() if zone_data.view_type else ''}"
            f"{zone_data.color_code.lower().strip() if zone_data.color_code else ''}"
            f"{zone_data.display_order}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_zone_{hash_value}"
    
    @classmethod
    def generate_section_id(cls, prefix: str, section_data, level_internal_id: str, performance_internal_id: str) -> str:
        """Generate internal section ID"""
        # Try using section_id as source_id if it looks like a source identifier
        if cls._is_valid_source_id(section_data.section_id):
            return f"{prefix}_section_{section_data.section_id}"
        
        # Fallback: content-based hash
        content = (
            f"{section_data.name.lower().strip()}"
            f"{level_internal_id}"
            f"{performance_internal_id}"
            f"{section_data.section_type.lower().strip() if section_data.section_type else ''}"
            f"{section_data.display_order}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_section_{hash_value}"
    
    @classmethod
    def generate_seat_id(cls, prefix: str, seat_data, section_internal_id: str, zone_internal_id: str) -> str:
        """Generate internal seat ID"""
        # Try using seat_id as source_id if it looks like a source identifier
        if cls._is_valid_source_id(seat_data.seat_id):
            return f"{prefix}_seat_{seat_data.seat_id}"
        
        # Fallback: content-based hash
        content = (
            f"{seat_data.row_label.lower().strip()}"
            f"{seat_data.seat_number.lower().strip()}"
            f"{section_internal_id}"
            f"{zone_internal_id}"
            f"{seat_data.seat_type.lower().strip() if seat_data.seat_type else 'standard'}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_seat_{hash_value}"
    
    @classmethod
    def generate_pack_id(cls, prefix: str, pack_data, zone_internal_id: str) -> str:
        """Generate internal seat pack ID"""
        # Try using pack_id as source_id if it looks like a source identifier
        if cls._is_valid_source_id(pack_data.pack_id):
            return f"{prefix}_pack_{pack_data.pack_id}"
        
        # Fallback: content-based hash
        content = (
            f"{pack_data.row_label.lower().strip()}"
            f"{pack_data.start_seat_number.lower().strip()}"
            f"{pack_data.end_seat_number.lower().strip()}"
            f"{zone_internal_id}"
            f"{pack_data.pack_size}"
        )
        hash_value = cls._create_hash(content)
        return f"{prefix}_pack_{hash_value}"