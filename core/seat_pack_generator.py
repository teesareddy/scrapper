import re
import hashlib
import logging
from typing import List, Dict, Optional
from decimal import Decimal
from .data_schemas import SeatData, SectionData, SeatPackData
from .seat_pack_comparator import SeatPackComparator, SeatPackComparison

logger = logging.getLogger(__name__)


def natural_sort_key(seat: SeatData) -> tuple:
    parts = [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', seat.seat_number)]
    return (parts, seat.seat_id)


def parse_seat_number(seat: SeatData) -> Optional[tuple]:
    numbers = re.findall(r'\d+', str(seat.seat_number))
    if numbers:
        return (int(numbers[-1]), seat.seat_id)
    return None


def are_seats_adjacent(seat1: SeatData, seat2: SeatData, numbering_scheme: str) -> bool:
    parsed_num1 = parse_seat_number(seat1)
    parsed_num2 = parse_seat_number(seat2)

    if parsed_num1 is None or parsed_num2 is None:
        return False

    num1 = parsed_num1[0]
    num2 = parsed_num2[0]

    if numbering_scheme == "consecutive":
        return num2 == num1 + 1
    elif numbering_scheme in ["odd-even", "odd_even"]:
        return num2 == num1 + 2
    elif numbering_scheme == "broadway_sf_consecutive":
        return num2 == num1 + 2

    return False


def generate_deterministic_pack_id(
    source_website: str,
    performance_id: str,
    level_id: str,
    zone_id: str,
    row_label: str,
    seat_ids: List[str],
    venue_prefix_map: Dict[str, str]
) -> str:
    prefix = venue_prefix_map.get(source_website, "unk")
    sorted_seat_ids = ",".join(sorted(seat_ids))
    canonical_string = f"{source_website}|{performance_id}|{level_id}|{zone_id}|{row_label}|{sorted_seat_ids}"

    hasher = hashlib.md5()
    hasher.update(canonical_string.encode('utf-8'))
    hash_hex = hasher.hexdigest()

    return f"{prefix}_pk_{hash_hex[:16]}"


def generate_seat_packs(
    all_seats: List[SeatData],
    all_sections: List[SectionData],
    performance: any,
    venue_prefix_map: Dict[str, str],
    venue: any = None,
    min_pack_size: int = 2,
    packing_strategy: str = "maximal",
    seating_strategy: str = None,
    scraper_instance: any = None
) -> List[SeatPackData]:
    if min_pack_size < 1:
        min_pack_size = 1
    if packing_strategy not in ["maximal", "exhaustive"]:
        raise ValueError("packing_strategy must be 'maximal' or 'exhaustive'")
    
    # Strategy-aware seat pack generation
    if seating_strategy is None and scraper_instance is not None:
        # Get strategy from scraper instance
        try:
            seating_strategy = scraper_instance.get_seat_pack_strategy()
        except Exception:
            seating_strategy = "consecutive"  # Safe fallback
    elif seating_strategy is None:
        seating_strategy = "consecutive"  # Default strategy
    
    # Route to appropriate generation method based on strategy
    if seating_strategy == "consecutive":
        return _generate_consecutive_packs(
            all_seats, all_sections, performance, venue_prefix_map, 
            venue, min_pack_size, packing_strategy
        )
    elif seating_strategy == "odd_even":
        return _generate_odd_even_packs(
            all_seats, all_sections, performance, venue_prefix_map,
            venue, min_pack_size, packing_strategy
        )
    elif seating_strategy == "mixed":
        # Check for recursion guard to prevent infinite loops
        if scraper_instance and hasattr(scraper_instance, '_generating_packs') and scraper_instance._generating_packs:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("Recursion detected in mixed strategy, falling back to consecutive")
            return _generate_consecutive_packs(
                all_seats, all_sections, performance, venue_prefix_map, 
                venue, min_pack_size, packing_strategy
            )
        
        return _generate_mixed_strategy_packs(
            all_seats, all_sections, performance, venue_prefix_map,
            venue, min_pack_size, packing_strategy, scraper_instance
        )
    elif seating_strategy == "custom" and scraper_instance is not None:
        # Call scraper's custom generation method
        return scraper_instance.generate_seat_packs(all_seats, all_sections, performance)
    else:
        # Fallback to existing logic for backward compatibility
        seating_strategy = "consecutive"

    sections_map = {s.section_id: s for s in all_sections}
    available_seats = [seat for seat in all_seats if seat.status == "available"]

    enhanced_seats = []
    for seat in available_seats:
        section = sections_map.get(seat.section_id)
        level_id = section.level_id if section else None
        
        enhanced_seat = {
            'seat_data': seat,
            'level_id': level_id,
            'event_id': performance.event_source_id if hasattr(performance, 'event_source_id') else getattr(performance, 'event_id', None),
            'performance_id': performance.source_performance_id,
            'performance_obj': performance,
            'section_obj': section
        }
        enhanced_seats.append(enhanced_seat)

    seats_by_zone_section_row: Dict[str, Dict[str, Dict[str, List[dict]]]] = {}
    for enhanced_seat in enhanced_seats:
        seat = enhanced_seat['seat_data']
        if seat.zone_id not in seats_by_zone_section_row:
            seats_by_zone_section_row[seat.zone_id] = {}
        if seat.section_id not in seats_by_zone_section_row[seat.zone_id]:
            seats_by_zone_section_row[seat.zone_id][seat.section_id] = {}
        if seat.row_label not in seats_by_zone_section_row[seat.zone_id][seat.section_id]:
            seats_by_zone_section_row[seat.zone_id][seat.section_id][seat.row_label] = []
        seats_by_zone_section_row[seat.zone_id][seat.section_id][seat.row_label].append(enhanced_seat)

    for zone in seats_by_zone_section_row.values():
        for section in zone.values():
            for row in section.values():
                row.sort(key=lambda enhanced_seat: natural_sort_key(enhanced_seat['seat_data']))

    contiguous_blocks: List[List[dict]] = []

    for sections in seats_by_zone_section_row.values():
        for rows in sections.values():
            for sorted_enhanced_seats in rows.values():
                if not sorted_enhanced_seats:
                    continue

                first_enhanced = sorted_enhanced_seats[0]
                section = first_enhanced['section_obj']
                if not section:
                    continue

                if section.numbering_scheme in ["odd-even", "odd_even"]:
                    odd_seats = []
                    even_seats = []

                    for enhanced_seat in sorted_enhanced_seats:
                        seat_data = enhanced_seat['seat_data']
                        parsed_num = parse_seat_number(seat_data)
                        if parsed_num is not None:
                            num = parsed_num[0]
                            if num % 2 == 1:
                                odd_seats.append(enhanced_seat)
                            else:
                                even_seats.append(enhanced_seat)

                    for seats_group in [odd_seats, even_seats]:
                        if len(seats_group) < 1:
                            continue

                        current_block = [seats_group[0]]

                        for i in range(1, len(seats_group)):
                            seat_prev = seats_group[i-1]['seat_data']
                            seat_curr = seats_group[i]['seat_data']
                            if are_seats_adjacent(seat_prev, seat_curr, section.numbering_scheme):
                                current_block.append(seats_group[i])
                            else:
                                if current_block:
                                    contiguous_blocks.append(current_block)
                                current_block = [seats_group[i]]

                        if current_block:
                            contiguous_blocks.append(current_block)
                else:
                    current_block = [sorted_enhanced_seats[0]]

                    for i in range(1, len(sorted_enhanced_seats)):
                        seat_prev = sorted_enhanced_seats[i-1]['seat_data']
                        seat_curr = sorted_enhanced_seats[i]['seat_data']
                        if are_seats_adjacent(seat_prev, seat_curr, section.numbering_scheme):
                            current_block.append(sorted_enhanced_seats[i])
                        else:
                            if current_block:
                                contiguous_blocks.append(current_block)
                            current_block = [sorted_enhanced_seats[i]]

                    if current_block:
                        contiguous_blocks.append(current_block)

    generated_packs: List[SeatPackData] = []

    for block in contiguous_blocks:
        block_len = len(block)

        def create_pack_object(enhanced_seats_in_pack: List[dict]) -> SeatPackData:
            first_enhanced = enhanced_seats_in_pack[0]
            last_enhanced = enhanced_seats_in_pack[-1]
            first_seat = first_enhanced['seat_data']
            last_seat = last_enhanced['seat_data']
            
            seat_ids = [enhanced['seat_data'].seat_id for enhanced in enhanced_seats_in_pack]
            
            # Calculate pack price using: base_seat_price * pack_size
            # Get base price from first seat (all seats in same zone should have same price)
            base_seat_price = first_seat.price
            pack_size = len(enhanced_seats_in_pack)
            
            if base_seat_price is not None:
                # Calculate base pack price: seat_price * pack_size
                pack_price = base_seat_price * pack_size
                
                # Apply venue markup to total pack price
                total_price = pack_price
                if venue and hasattr(venue, 'price_markup_value') and venue.price_markup_value:
                    if venue.price_markup_type == 'percentage':
                        markup_amount = pack_price * (venue.price_markup_value / Decimal('100'))
                        total_price = pack_price + markup_amount
                    elif venue.price_markup_type == 'dollar':
                        # For dollar markup, apply per pack (not per seat)
                        total_price = pack_price + venue.price_markup_value
                    else:
                        total_price = pack_price
                else:
                    total_price = pack_price
            else:
                pack_price = None
                total_price = None

            level_id = first_enhanced['level_id'] or "unknown"
            event_id = first_enhanced['event_id']
            performance_obj = first_enhanced['performance_obj']

            return SeatPackData(
                pack_id=generate_deterministic_pack_id(
                    source_website=first_seat.source_website,
                    performance_id=first_enhanced['performance_id'],
                    level_id=level_id,
                    zone_id=first_seat.zone_id,
                    row_label=first_seat.row_label,
                    seat_ids=seat_ids,
                    venue_prefix_map=venue_prefix_map
                ),
                zone_id=first_seat.zone_id,
                source_website=first_seat.source_website,
                row_label=first_seat.row_label,
                start_seat_number=first_seat.seat_number,
                end_seat_number=last_seat.seat_number,
                pack_size=len(enhanced_seats_in_pack),
                pack_price=pack_price,
                total_price=total_price,
                seat_ids=seat_ids,
                row=first_seat.row_label,
                start_seat=first_seat.seat_number,
                end_seat=last_seat.seat_number,
                performance=performance_obj,  # Keep the full object for database handler
                event=event_id,
                level=level_id,
                level_id=level_id  # Added to include the level identifier
            )

        if packing_strategy == "maximal":
            if block_len >= min_pack_size:
                generated_packs.append(create_pack_object(block))

        elif packing_strategy == "exhaustive":
            for pack_size in range(min_pack_size, block_len + 1):
                for i in range(block_len - pack_size + 1):
                    generated_packs.append(create_pack_object(block[i:i + pack_size]))

    return generated_packs


def detect_seat_numbering_scheme(seats: List[SeatData]) -> str:
    if len(seats) < 2:
        return "consecutive"

    sorted_seats = sorted(seats, key=natural_sort_key)

    numbers = []
    for seat in sorted_seats:
        parsed_num = parse_seat_number(seat)
        if parsed_num is not None:
            numbers.append(parsed_num[0])

    if len(numbers) < 2:
        return "consecutive"

    differences = [numbers[i+1] - numbers[i] for i in range(len(numbers)-1)]

    if differences.count(1) >= len(differences) * 0.7:
        return "consecutive"

    if differences.count(2) >= len(differences) * 0.7:
        return "odd-even"

    return "consecutive"


def detect_venue_seat_structure(all_seats: List[SeatData]) -> str:
    sections_rows = {}
    for seat in all_seats:
        key = (seat.section_id, seat.row_label)
        if key not in sections_rows:
            sections_rows[key] = []
        sections_rows[key].append(seat)

    schemes = []
    for seats_in_row in sections_rows.values():
        if len(seats_in_row) >= 2:
            scheme = detect_seat_numbering_scheme(seats_in_row)
            schemes.append(scheme)

    if not schemes:
        return "consecutive"

    consecutive_count = schemes.count("consecutive")
    odd_even_count = schemes.count("odd-even") + schemes.count("odd_even")

    if odd_even_count > consecutive_count:
        return "odd_even"
    else:
        return "consecutive"


# ===============================================
# STRATEGY-SPECIFIC SEAT PACK GENERATION FUNCTIONS
# ===============================================

def _generate_consecutive_packs(
    all_seats: List[SeatData],
    all_sections: List[SectionData],
    performance: any,
    venue_prefix_map: Dict[str, str],
    venue: any = None,
    min_pack_size: int = 2,
    packing_strategy: str = "maximal"
) -> List[SeatPackData]:
    """Generate seat packs using consecutive numbering strategy."""
    # Set numbering scheme to consecutive for all sections
    for section in all_sections:
        section.numbering_scheme = "consecutive"
    
    # Use the original logic with consecutive adjacency
    return _execute_pack_generation_with_scheme(
        all_seats, all_sections, performance, venue_prefix_map,
        venue, min_pack_size, packing_strategy, "consecutive"
    )


def _generate_odd_even_packs(
    all_seats: List[SeatData],
    all_sections: List[SectionData],
    performance: any,
    venue_prefix_map: Dict[str, str],
    venue: any = None,
    min_pack_size: int = 2,
    packing_strategy: str = "maximal"
) -> List[SeatPackData]:
    """Generate seat packs using odd/even numbering strategy."""
    # Set numbering scheme to odd_even for all sections
    for section in all_sections:
        section.numbering_scheme = "odd_even"
    
    # Use the original logic with odd/even adjacency
    return _execute_pack_generation_with_scheme(
        all_seats, all_sections, performance, venue_prefix_map,
        venue, min_pack_size, packing_strategy, "odd_even"
    )


def _generate_mixed_strategy_packs(
    all_seats: List[SeatData],
    all_sections: List[SectionData],
    performance: any,
    venue_prefix_map: Dict[str, str],
    venue: any = None,
    min_pack_size: int = 2,
    packing_strategy: str = "maximal",
    scraper_instance: any = None
) -> List[SeatPackData]:
    """Generate seat packs using mixed strategy (analyze each row dynamically)."""
    import logging
    logger = logging.getLogger(__name__)
    
    if scraper_instance is None:
        logger.warning("Mixed strategy requires scraper instance, falling back to consecutive")
        return _generate_consecutive_packs(
            all_seats, all_sections, performance, venue_prefix_map,
            venue, min_pack_size, packing_strategy
        )
    
    # Additional recursion guard for mixed strategy
    if hasattr(scraper_instance, '_analyzing_structure') and scraper_instance._analyzing_structure:
        logger.warning("Structure analysis in progress, falling back to consecutive to prevent recursion")
        return _generate_consecutive_packs(
            all_seats, all_sections, performance, venue_prefix_map,
            venue, min_pack_size, packing_strategy
        )
    
    if hasattr(scraper_instance, '_generating_packs') and scraper_instance._generating_packs:
        logger.warning("Pack generation in progress, falling back to consecutive to prevent recursion")
        return _generate_consecutive_packs(
            all_seats, all_sections, performance, venue_prefix_map,
            venue, min_pack_size, packing_strategy
        )
    
    # Convert seats to dictionary format for analysis
    seat_dicts = []
    for seat in all_seats:
        seat_dict = {
            'level': getattr(seat, 'level_id', ''),
            'row': getattr(seat, 'row_label', ''),
            'number': getattr(seat, 'seat_number', ''),
            'x': getattr(seat, 'x_coord', 0),
            'available': getattr(seat, 'available', True),
            'section': getattr(seat, 'section_id', '')
        }
        seat_dicts.append(seat_dict)
    
    # Group seats by section and row for analysis
    seating_map = {}
    for i, seat in enumerate(all_seats):
        section_id = seat.section_id
        row_label = seat.row_label
        
        if section_id not in seating_map:
            seating_map[section_id] = {}
        if row_label not in seating_map[section_id]:
            seating_map[section_id][row_label] = []
        seating_map[section_id][row_label].append(seat)
    
    # Analyze each row and set appropriate numbering scheme
    sections_map = {s.section_id: s for s in all_sections}
    
    for section_id, rows in seating_map.items():
        section = sections_map.get(section_id)
        if not section:
            continue
        
        # Analyze patterns across all rows in this section
        row_patterns = []
        for row_seats in rows.values():
            if len(row_seats) < 2:
                continue
            
            # Convert to dict format for analysis
            row_seat_dicts = []
            for seat in row_seats:
                row_seat_dicts.append({
                    'number': seat.seat_number,
                    'x': getattr(seat, 'x_coord', 0)
                })
            
            try:
                row_analysis = scraper_instance._analyze_row_structure(row_seat_dicts)
                row_patterns.append(row_analysis['dominant_pattern'])
            except Exception as e:
                logger.warning(f"Failed to analyze row structure: {e}")
                row_patterns.append('consecutive')
        
        # Determine section's numbering scheme based on majority pattern
        if not row_patterns:
            section.numbering_scheme = "consecutive"
        else:
            pattern_counts = {
                'consecutive': row_patterns.count('consecutive'),
                'odd': row_patterns.count('odd'),
                'even': row_patterns.count('even'),
                'odd_even': row_patterns.count('odd_even')
            }
            
            # If we have both odd and even, or explicit odd_even, use odd_even strategy
            if pattern_counts['odd'] > 0 and pattern_counts['even'] > 0:
                section.numbering_scheme = "odd_even"
            elif pattern_counts['odd_even'] > 0:
                section.numbering_scheme = "odd_even"
            elif pattern_counts['consecutive'] > 0:
                section.numbering_scheme = "consecutive"
            else:
                section.numbering_scheme = "consecutive"
        
        logger.debug(f"Section {section_id}: detected scheme '{section.numbering_scheme}' from patterns {row_patterns}")
    
    # Generate packs using the detected schemes
    return _execute_pack_generation_with_scheme(
        all_seats, all_sections, performance, venue_prefix_map,
        venue, min_pack_size, packing_strategy, "mixed"
    )


def _execute_pack_generation_with_scheme(
    all_seats: List[SeatData],
    all_sections: List[SectionData],
    performance: any,
    venue_prefix_map: Dict[str, str],
    venue: any = None,
    min_pack_size: int = 2,
    packing_strategy: str = "maximal",
    overall_scheme: str = "consecutive"
) -> List[SeatPackData]:
    """Execute the actual pack generation logic with specified numbering schemes."""
    
    sections_map = {s.section_id: s for s in all_sections}
    available_seats = [seat for seat in all_seats if seat.status == "available"]

    enhanced_seats = []
    for seat in available_seats:
        section = sections_map.get(seat.section_id)
        level_id = section.level_id if section else None
        
        enhanced_seat = {
            'seat_data': seat,
            'level_id': level_id,
            'event_id': performance.event_source_id if hasattr(performance, 'event_source_id') else getattr(performance, 'event_id', None),
            'performance_id': performance.source_performance_id,
            'performance_obj': performance,
            'section_obj': section
        }
        enhanced_seats.append(enhanced_seat)

    seats_by_zone_section_row: Dict[str, Dict[str, Dict[str, List[dict]]]] = {}
    for enhanced_seat in enhanced_seats:
        seat = enhanced_seat['seat_data']
        if seat.zone_id not in seats_by_zone_section_row:
            seats_by_zone_section_row[seat.zone_id] = {}
        if seat.section_id not in seats_by_zone_section_row[seat.zone_id]:
            seats_by_zone_section_row[seat.zone_id][seat.section_id] = {}
        if seat.row_label not in seats_by_zone_section_row[seat.zone_id][seat.section_id]:
            seats_by_zone_section_row[seat.zone_id][seat.section_id][seat.row_label] = []
        seats_by_zone_section_row[seat.zone_id][seat.section_id][seat.row_label].append(enhanced_seat)

    for zone in seats_by_zone_section_row.values():
        for section in zone.values():
            for row in section.values():
                row.sort(key=lambda enhanced_seat: natural_sort_key(enhanced_seat['seat_data']))

    contiguous_blocks: List[List[dict]] = []

    for sections in seats_by_zone_section_row.values():
        for rows in sections.values():
            for sorted_enhanced_seats in rows.values():
                if not sorted_enhanced_seats:
                    continue

                first_enhanced = sorted_enhanced_seats[0]
                section = first_enhanced['section_obj']
                if not section:
                    continue

                # Use section-specific numbering scheme for mixed strategy
                section_scheme = getattr(section, 'numbering_scheme', overall_scheme)
                
                if section_scheme in ["odd-even", "odd_even"]:
                    odd_seats = []
                    even_seats = []

                    for enhanced_seat in sorted_enhanced_seats:
                        seat_data = enhanced_seat['seat_data']
                        parsed_num = parse_seat_number(seat_data)
                        if parsed_num is not None:
                            num = parsed_num[0]
                            if num % 2 == 1:
                                odd_seats.append(enhanced_seat)
                            else:
                                even_seats.append(enhanced_seat)

                    for seats_group in [odd_seats, even_seats]:
                        if len(seats_group) < 1:
                            continue

                        current_block = [seats_group[0]]

                        for i in range(1, len(seats_group)):
                            seat_prev = seats_group[i-1]['seat_data']
                            seat_curr = seats_group[i]['seat_data']
                            if are_seats_adjacent(seat_prev, seat_curr, section_scheme):
                                current_block.append(seats_group[i])
                            else:
                                if current_block:
                                    contiguous_blocks.append(current_block)
                                current_block = [seats_group[i]]

                        if current_block:
                            contiguous_blocks.append(current_block)
                else:
                    # Consecutive logic
                    current_block = [sorted_enhanced_seats[0]]

                    for i in range(1, len(sorted_enhanced_seats)):
                        seat_prev = sorted_enhanced_seats[i-1]['seat_data']
                        seat_curr = sorted_enhanced_seats[i]['seat_data']
                        if are_seats_adjacent(seat_prev, seat_curr, section_scheme):
                            current_block.append(sorted_enhanced_seats[i])
                        else:
                            if current_block:
                                contiguous_blocks.append(current_block)
                            current_block = [sorted_enhanced_seats[i]]

                    if current_block:
                        contiguous_blocks.append(current_block)

    generated_packs: List[SeatPackData] = []

    for block in contiguous_blocks:
        block_len = len(block)

        def create_pack_object(enhanced_seats_in_pack: List[dict]) -> SeatPackData:
            first_enhanced = enhanced_seats_in_pack[0]
            last_enhanced = enhanced_seats_in_pack[-1]
            first_seat = first_enhanced['seat_data']
            last_seat = last_enhanced['seat_data']
            
            seat_ids = [enhanced['seat_data'].seat_id for enhanced in enhanced_seats_in_pack]
            
            # Calculate pack price using: base_seat_price * pack_size
            # Get base price from first seat (all seats in same zone should have same price)
            base_seat_price = first_seat.price
            pack_size = len(enhanced_seats_in_pack)
            
            if base_seat_price is not None:
                # Calculate base pack price: seat_price * pack_size
                pack_price = base_seat_price * pack_size
                
                # Apply venue markup to total pack price
                total_price = pack_price
                if venue and hasattr(venue, 'price_markup_value') and venue.price_markup_value:
                    if venue.price_markup_type == 'percentage':
                        markup_amount = pack_price * (venue.price_markup_value / Decimal('100'))
                        total_price = pack_price + markup_amount
                    elif venue.price_markup_type == 'dollar':
                        # For dollar markup, apply per pack (not per seat)
                        total_price = pack_price + venue.price_markup_value
                    else:
                        total_price = pack_price
                else:
                    total_price = pack_price
            else:
                pack_price = None
                total_price = None

            level_id = first_enhanced['level_id'] or "unknown"
            event_id = first_enhanced['event_id']
            performance_obj = first_enhanced['performance_obj']

            return SeatPackData(
                pack_id=generate_deterministic_pack_id(
                    source_website=first_seat.source_website,
                    performance_id=first_enhanced['performance_id'],
                    level_id=level_id,
                    zone_id=first_seat.zone_id,
                    row_label=first_seat.row_label,
                    seat_ids=seat_ids,
                    venue_prefix_map=venue_prefix_map
                ),
                zone_id=first_seat.zone_id,
                source_website=first_seat.source_website,
                row_label=first_seat.row_label,
                start_seat_number=first_seat.seat_number,
                end_seat_number=last_seat.seat_number,
                pack_size=len(enhanced_seats_in_pack),
                pack_price=pack_price,
                total_price=total_price,
                seat_ids=seat_ids,
                row=first_seat.row_label,
                start_seat=first_seat.seat_number,
                end_seat=last_seat.seat_number,
                performance=performance_obj,
                event=event_id,
                level=level_id,
                level_id=level_id
            )

        if packing_strategy == "maximal":
            if block_len >= min_pack_size:
                generated_packs.append(create_pack_object(block))

        elif packing_strategy == "exhaustive":
            for pack_size in range(min_pack_size, block_len + 1):
                for i in range(block_len - pack_size + 1):
                    generated_packs.append(create_pack_object(block[i:i + pack_size]))

    return generated_packs

def generate_and_compare_seat_packs(
    scraped_data: Dict,
    performance_id: str,
    source_website: str = "demo_scraper",
    venue_prefix_map: Optional[Dict[str, str]] = None,
    min_pack_size: int = 2,
    packing_strategy: str = "maximal"
) -> SeatPackComparison:
    """
    Generate seat packs from scraped data and compare with existing database packs
    
    Args:
        scraped_data: Scraped data containing seats, sections, performance info
        performance_id: Internal performance ID
        source_website: Source website identifier
        venue_prefix_map: Mapping of venue prefixes
        min_pack_size: Minimum pack size for generation
        packing_strategy: Packing strategy ('maximal' or 'exhaustive')
        
    Returns:
        SeatPackComparison object with categorized pack results
    """
    logger.info(f"Starting seat pack generation and comparison for performance {performance_id}")
    
    try:
        # Extract data from scraped_data
        seats_data = scraped_data.get('seats', [])
        sections_data = scraped_data.get('sections', [])
        performance_data = scraped_data.get('performance_info', {})
        
        # Convert to proper data objects if needed
        all_seats = []
        if isinstance(seats_data, list) and seats_data:
            # Assume seats_data is already in proper format or convert if needed
            all_seats = seats_data
        
        all_sections = []
        if isinstance(sections_data, list) and sections_data:
            all_sections = sections_data
        
        # Set default venue prefix map if not provided
        if venue_prefix_map is None:
            venue_prefix_map = {source_website: "sp"}
        
        logger.info(f"Generating packs from {len(all_seats)} seats and {len(all_sections)} sections")
        
        # Generate new seat packs
        new_seat_packs = generate_seat_packs(
            all_seats=all_seats,
            all_sections=all_sections,
            performance=performance_data,
            venue_prefix_map=venue_prefix_map,
            min_pack_size=min_pack_size,
            packing_strategy=packing_strategy
        )
        
        logger.info(f"Generated {len(new_seat_packs)} new seat packs")
        
        # Compare with existing packs in database
        comparator = SeatPackComparator(performance_id, source_website)
        comparison = comparator.compare_seat_packs(new_seat_packs)
        
        logger.info(f"Comparison completed: {comparison.to_dict()['summary']}")
        return comparison
        
    except Exception as e:
        logger.error(f"Error during seat pack generation and comparison: {e}", exc_info=True)
        raise
