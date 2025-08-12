"""
Broadway SF API-based scraper using the new centralized architecture.

This module implements Broadway SF scraping using direct API calls instead of
browser automation, following all the architectural best practices established
in the core modules.
"""

import re
import logging
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
import asyncio

from ...core.api_scraper_base import BaseApiScraper
from ...core.api_configurations import get_broadway_sf_config
from ...core.response_validators import BroadwaySFResponseValidator, ValidationLevel
from ...exceptions.scraping_exceptions import ScrapingException, ParseException


class BroadwaySFApiScraper(BaseApiScraper):
    """
    Broadway SF scraper using API-based approach.
    
    This scraper follows the new architecture with:
    - Direct API calls instead of browser automation
    - Centralized proxy management
    - Structured error handling and validation
    - Clean separation of concerns
    """
    
    def __init__(self, url: str = None, scrape_job_id: str = None,
                 optimization_enabled: bool = True, optimization_level: str = "balanced",
                 config: Dict[str, Any] = None, scraper_definition=None):
        super().__init__(url, scrape_job_id, config, scraper_definition)
        
        # Memory optimization: Limit cache size
        self._max_cache_size = 20
        self._response_cache = {}
        
        # Memory optimization: Track HTTP client usage
        self._http_clients = []
        self._max_http_clients = 5
    
    @property
    def name(self) -> str:
        """Return the unique name of the scraper."""
        return "broadway_sf_api_scraper"
    
    def _get_default_config(self):
        """Get default configuration for Broadway SF scraper with dynamic domain support."""
        if hasattr(self, 'url') and self.url:
            domain_info = self._extract_domain_info(self.url)
            return get_broadway_sf_config(domain_info['base_domain'])
        return get_broadway_sf_config()
    
    def _extract_domain_info(self, url: str) -> Dict[str, str]:
        """
        Extract domain information from URL for dynamic headers and API calls.
        
        Args:
            url: Event URL (e.g., https://www.kingstheatre.com/events/...)
            
        Returns:
            Dictionary with domain info: {'base_domain': 'kingstheatre.com', 'full_domain': 'https://www.kingstheatre.com'}
        """
        try:
            parsed_url = urlparse(url)
            
            # Validate that we have a proper URL with scheme and netloc
            if not parsed_url.scheme or not parsed_url.netloc:
                raise ValueError(f"Invalid URL format: {url}")
            
            full_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # Extract base domain (remove www. if present)
            base_domain = parsed_url.netloc
            if base_domain.startswith('www.'):
                base_domain = base_domain[4:]
            
            return {
                'base_domain': base_domain,
                'full_domain': full_domain
            }
        except Exception as e:
            self.logger.warning(f"Failed to extract domain from URL {url}: {e}")
            # Fallback to Broadway SF defaults
            return {
                'base_domain': 'broadwaysf.com',
                'full_domain': 'https://www.broadwaysf.com'
            }

    def _get_venue_slug_mapping(self, domain: str) -> Optional[str]:
        """
        Get venue slug for single-venue domains.
        
        Args:
            domain: Base domain (e.g., 'saengernola.com')
            
        Returns:
            Venue slug for single-venue sites, None for multi-venue sites
        """
        venue_mapping = {
            'saengernola.com': 'saenger-theatre',
            'kingstheatre.com': 'kings-theatre-brooklyn',
            'emersoncolonialtheatre.com': 'emerson-colonial-theatre',
        }
        return venue_mapping.get(domain)

    async def parse_url_parameters(self, url: str) -> Dict[str, str]:
        """
        Parse Broadway SF URL to extract parameters needed for API calls.
        
        Example URL:
        https://www.broadwaysf.com/events/bsf-twilight-in-concert/golden-gate-theatre/tickets/DE37163B-A1C1-4D86-BFD6-D45F8313A2E0
        
        Args:
            url: Broadway SF event URL
            
        Returns:
            Dictionary with titleSlug, venueSlug, and performanceId
            
        Raises:
            ParseException: If URL format is invalid
        """
        if not url or not isinstance(url, str):
            raise ParseException(f"Invalid URL provided: {url}")
        
        # Extract domain info for venue slug mapping
        domain_info = self._extract_domain_info(url)
        base_domain = domain_info['base_domain']
        
        # Try multi-venue pattern first: /events/{title}/{venue}/tickets/{id}
        multi_venue_pattern = r'/events/([^/]+)/([^/]+)/tickets/'
        match = re.search(multi_venue_pattern, url)
        
        if match:
            title_slug = match.group(1)
            venue_slug = match.group(2)
            self.logger.info(f"Multi-venue URL pattern detected for {base_domain}")
        else:
            # Try single-venue pattern: /events/{title}/tickets/{id}
            single_venue_pattern = r'/events/([^/]+)/tickets/'
            match = re.search(single_venue_pattern, url)
            
            if match:
                title_slug = match.group(1)
                venue_slug = self._get_venue_slug_mapping(base_domain)
                
                if not venue_slug:
                    self.logger.warning(f"No venue slug mapping found for domain {base_domain}, using default")
                    venue_slug = "unknown-venue"
                
                self.logger.info(f"Single-venue URL pattern detected for {base_domain}, using venue slug: {venue_slug}")
            else:
                self.logger.error(f"URL does not match any expected format: {url}")
                raise ParseException(
                    f"Could not parse URL. Expected formats: "
                    f"/events/[title]/[venue]/tickets/[id] or /events/[title]/tickets/[id]"
                )
        
        # Validate that slugs are not empty
        if not title_slug or not venue_slug:
            raise ParseException(
                f"Extracted empty slugs from URL: titleSlug='{title_slug}', venueSlug='{venue_slug}'"
            )
        
        # Extract performance ID from URL (UUID at the end)
        performance_id = None
        performance_id_match = re.search(r'/([A-F0-9-]{36})/?$', url, re.IGNORECASE)
        if performance_id_match:
            performance_id = performance_id_match.group(1)
            self.logger.info(f"Extracted performance ID from URL: {performance_id}")
        
        self.logger.info(f"Parsed URL slugs - titleSlug: {title_slug}, venueSlug: {venue_slug}")
        
        return {
            'title_slug': title_slug,
            'venue_slug': venue_slug,
            'performance_id': performance_id
        }
    
    async def validate_response(self, result, endpoint_name: str) -> bool:
        """
        Validate Broadway SF API responses with endpoint-specific logic.
        
        Args:
            result: The API response result
            endpoint_name: Name of the endpoint being validated
            
        Returns:
            True if response is valid, False otherwise
        """
        # Use base validation first
        if not await super().validate_response(result, endpoint_name):
            return False
        
        # Endpoint-specific validation
        if endpoint_name == "calendar_service":
            return self._validate_calendar_response(result.data)
        elif endpoint_name == "bolt_api":
            return self._validate_bolt_response(result.data)
        
        return True
    
    def _validate_calendar_response(self, data: Dict[str, Any]) -> bool:
        """Validate calendar service GraphQL response."""
        if not isinstance(data, dict):
            self.logger.warning("Calendar response is not a dictionary")
            return False
        
        # Check for GraphQL structure
        if 'data' not in data:
            self.logger.warning("Calendar response missing 'data' field")
            return False
        
        # Check for show data
        show_data = data.get('data', {}).get('getShow', {}).get('show', {})
        if not show_data:
            self.logger.warning("Calendar response missing show data")
            return False
        
        # Check for performances
        performances = show_data.get('performances', [])
        if not performances:
            self.logger.warning("Calendar response contains no performances")
            return False
        
        self.logger.debug(f"Calendar validation passed: {len(performances)} performances found")
        return True
    
    def _validate_bolt_response(self, data: Dict[str, Any]) -> bool:
        """Validate Bolt API response."""
        if not isinstance(data, dict):
            self.logger.warning("Bolt response is not a dictionary")
            return False
        
        # Check for required fields
        required_fields = ['seats', 'zones']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            self.logger.warning(f"Bolt response missing required fields: {missing_fields}")
            return False
        
        # Check that we have actual data
        seats = data.get('seats', [])
        zones = data.get('zones', {})
        
        if not seats:
            self.logger.warning("Bolt response contains no seats")
            return False
        
        if not zones:
            self.logger.warning("Bolt response contains no zones")
            return False
        
        self.logger.debug(f"Bolt validation passed: {len(seats)} seats, {len(zones)} zones")
        return True
    
    def process_api_response(self, response_data: Dict[str, Any], 
                           endpoint_name: str) -> Dict[str, Any]:
        """
        Process raw API response data for Broadway SF endpoints.
        
        Args:
            response_data: Raw response data from API
            endpoint_name: Name of the endpoint
            
        Returns:
            Processed data dictionary
        """
        if endpoint_name == "calendar_service":
            return self._process_calendar_response(response_data)
        elif endpoint_name == "bolt_api":
            return self._process_bolt_response(response_data)
        else:
            # Unknown endpoint, return as-is
            return response_data
    
    def _process_calendar_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process calendar service response."""
        # Extract and structure the relevant data
        show_data = data.get('data', {}).get('getShow', {}).get('show', {})
        
        processed = {
            'raw_data': data,
            'show_info': show_data,
            'performances': show_data.get('performances', []),
            'show_images': show_data.get('images', {}),
            'show_status': show_data.get('status'),
            'dates': show_data.get('dates', {}),
            'performance_mode': show_data.get('performanceMode')
        }
        
        self.logger.debug(f"Processed calendar data: {len(processed['performances'])} performances")
        return processed
    
    def _process_bolt_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process Bolt API response."""
        # Structure the seating data
        processed = {
            'raw_data': data,
            'seats': data.get('seats', []),
            'zones': data.get('zones', {}),
            'sections': data.get('sections', []),
            'legends': data.get('legends', []),
            'performance': data.get('performance', {}),
            'tickets': data.get('tickets', {}),
            'shapes': data.get('shapes', {}),
            'labels': data.get('labels', {})
        }
        
        self.logger.debug(f"Processed bolt data: {len(processed['seats'])} seats, {len(processed['zones'])} zones")
        return processed
    
    def combine_responses(self, responses: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine Broadway SF API responses into the format expected by the processor.
        
        Args:
            responses: Dictionary mapping endpoint names to response data
            
        Returns:
            Combined data in the format expected by BroadwaySFProcessor
        """
        calendar_data = responses.get('calendar_service', {})
        seating_data = responses.get('bolt_api', {})
        
        # Extract event info from seating data for consistency with original implementation
        event_info = self._extract_event_info_from_seating(seating_data)
        
        # Prioritize title from seating_data if available
        performance = seating_data.get('performance', {})
        if performance.get('title'):
            event_info['title'] = performance['title']
        
        # Add event_info to calendar_data for processor compatibility
        if calendar_data:
            calendar_data['event_info'] = event_info
        
        combined = {
            'calendar_data': calendar_data,
            'seating_data': seating_data,
            'event_info': event_info
        }
        
        self.logger.info(f"Combined API responses: calendar={bool(calendar_data)}, seating={bool(seating_data)}")
        return combined
    
    def _extract_event_info_from_seating(self, seating_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract event information from seating data."""
        if not seating_data:
            return {
                'title': 'Broadway SF Event',
                'venue': 'Broadway SF Venue',
                'datetime': None,
                'description': None,
                'image': None
            }
        
        performance = seating_data.get('performance', {})
        
        return {
            'title': performance.get('title', 'Broadway SF Event'),
            'venue': performance.get('venue', 'Broadway SF Venue'),
            'datetime': performance.get('dateTimeISO'),
            'description': None,  # Not available in API data
            'image': None  # Not available in API data
        }
    
    def analyze_seating_structure(self, seat_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze Broadway SF seating structure using sophisticated section-aware detection.
        
        Returns detailed structure information instead of just a strategy string.
        This enables proper handling of mixed consecutive/odd-even patterns within the same venue.
        """
        # Recursion guard to prevent infinite loops during structure analysis
        if hasattr(self, '_analyzing_structure') and self._analyzing_structure:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("Recursion detected in analyze_seating_structure, returning fallback")
            return {"strategy": "consecutive", "sections": {}}
        
        try:
            self._analyzing_structure = True
            
            # Perform sophisticated seating structure analysis
            structure_info = self._analyze_detailed_seating_structure(seat_data)
            
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"Detected seating structure: {structure_info['strategy']} with {len(structure_info['sections'])} section groups")
            
            return structure_info
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error analyzing seating structure: {e}, using fallback")
            return {"strategy": "consecutive", "sections": {}}
        finally:
            self._analyzing_structure = False
    
    def _is_consecutive(self, numbers: List[int]) -> bool:
        """Check if a list of numbers is a consecutive sequence."""
        if not numbers or len(numbers) < 2:
            return True  # A single seat is technically consecutive
        sorted_nums = sorted(numbers)
        return sorted_nums == list(range(min(sorted_nums), max(sorted_nums) + 1))
    
    def _is_all_odd(self, numbers: List[int]) -> bool:
        """Check if all numbers in a list are odd."""
        return all(n % 2 != 0 for n in numbers)
    
    def _is_all_even(self, numbers: List[int]) -> bool:
        """Check if all numbers in a list are even."""
        return all(n % 2 == 0 for n in numbers)
    
    def _format_seats_with_status(self, seat_list: List[Dict[str, Any]]) -> str:
        """Helper function to format seat numbers with their availability."""
        if not seat_list:
            return "[]"
        # Sort seats by their number before formatting
        sorted_seats = sorted(seat_list, key=lambda s: int(s.get('number', 0)))
        formatted_strings = []
        for seat in sorted_seats:
            status = "Available" if seat.get('available') else "Unavailable"
            formatted_strings.append(f"{seat['number']} ({status})")
        return ", ".join(formatted_strings)
    
    def _analyze_detailed_seating_structure(self, seat_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze all seat data to classify every row's numbering structure with enhanced pattern detection.
        
        Based on the comprehensive analysis script provided, this method now properly detects
        odd/even and consecutive patterns for Broadway SF venues.
        
        Returns:
            Dictionary containing strategy and detailed section information
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # 1. Group all seats by their level and row
        seating_map = {}
        for seat in seat_data:
            level = seat.get('level')
            row = seat.get('row')
            
            # Skip any seats that are missing key info or have non-integer numbers
            if not all([level, row, seat.get('number'), seat.get('x') is not None]):
                continue
            
            try:
                # Test conversion to ensure number is valid
                int(seat.get('number'))
            except (ValueError, TypeError):
                continue

            if level not in seating_map:
                seating_map[level] = {}
            if row not in seating_map[level]:
                seating_map[level][row] = []
            seating_map[level][row].append(seat)

        logger.info(f"--- Full Seating Structure Analysis ---")
        logger.info(f"Processing {len(seat_data)} raw seats from scrape")
        logger.debug(f"Analyzing seating structure for {len(seating_map)} levels")

        # 2. Analyze each level and row to determine patterns
        structure_sections = {}
        overall_patterns = []
        
        for level in sorted(seating_map.keys()):
            logger.info(f"\n----- LEVEL: {level} -----")
            structure_sections[level] = {}
            
            for row in sorted(seating_map[level].keys()):
                row_seats = seating_map[level][row]

                # Sort seats by their horizontal position to group them into physical sections
                sorted_seats = sorted(row_seats, key=lambda s: s['x'])

                # 3. Identify sections based on large gaps in 'x' coordinates (aisles)
                sections = []
                if sorted_seats:
                    current_section = [sorted_seats[0]]
                    # Use a more sensitive threshold for Broadway SF venues
                    X_GAP_THRESHOLD = 50
                    
                    for i in range(1, len(sorted_seats)):
                        prev_seat = sorted_seats[i - 1]
                        current_seat = sorted_seats[i]
                        if current_seat['x'] - prev_seat['x'] > X_GAP_THRESHOLD:
                            sections.append(current_section)
                            current_section = []
                        current_section.append(current_seat)
                    sections.append(current_section)

                # 4. Analyze each section and categorize the seats
                consecutive_seats = []
                odd_seats = []
                even_seats = []

                for section in sections:
                    # Convert seat numbers to integers for analysis
                    section_numbers = [int(s['number']) for s in section]

                    # Check the pattern for this section
                    if self._is_consecutive(section_numbers):
                        consecutive_seats.extend(section)
                        overall_patterns.append('consecutive')
                    elif self._is_all_odd(section_numbers):
                        odd_seats.extend(section)
                        overall_patterns.append('odd')
                    elif self._is_all_even(section_numbers):
                        even_seats.extend(section)
                        overall_patterns.append('even')

                # 5. Print the formatted results for the row (matching the analysis script)
                logger.info(f"Row '{row}':")
                logger.info(f"  Consecutive Section Seats: {self._format_seats_with_status(consecutive_seats)}")
                logger.info(f"  Odd-Numbered Section Seats:  {self._format_seats_with_status(odd_seats)}")
                logger.info(f"  Even-Numbered Section Seats: {self._format_seats_with_status(even_seats)}")

                # Store the section analysis for this row
                structure_sections[level][row] = {
                    'consecutive_sections': [consecutive_seats] if consecutive_seats else [],
                    'odd_sections': [odd_seats] if odd_seats else [],
                    'even_sections': [even_seats] if even_seats else [],
                    'total_sections': len(sections),
                    'consecutive_seats': consecutive_seats,
                    'odd_seats': odd_seats,
                    'even_seats': even_seats
                }

        # 6. Determine overall strategy based on patterns found
        if not overall_patterns:
            strategy = "consecutive"  # Fallback
        elif all(p == 'consecutive' for p in overall_patterns):
            strategy = "consecutive"
        elif all(p in ['odd', 'even'] for p in overall_patterns):
            strategy = "odd_even"
        else:
            strategy = "mixed"  # Mix of consecutive and odd/even

        logger.info(f"\n--- Analysis Complete ---")
        logger.info(f"Detected Strategy: {strategy}")
        logger.info(f"Pattern Counts: {overall_patterns.count('consecutive')} consecutive, {overall_patterns.count('odd')} odd, {overall_patterns.count('even')} even")

        return {
            'strategy': strategy,
            'sections': structure_sections,
            'pattern_counts': {
                'consecutive': overall_patterns.count('consecutive'),
                'odd': overall_patterns.count('odd'),
                'even': overall_patterns.count('even')
            }
        }
    
    def generate_seat_packs(self, seats: List[Any], sections: List[Any], performance: Any) -> List[Any]:
        """
        Generate seat packs for Broadway SF using enhanced section-aware strategy.
        
        This method now uses the detailed seating structure analysis to create packs
        that respect physical section boundaries and numbering patterns.
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # Recursion guard to prevent infinite loops
        if hasattr(self, '_generating_packs') and self._generating_packs:
            logger.warning("Recursion detected in generate_seat_packs, returning empty list")
            return []
        
        try:
            self._generating_packs = True
            
            # Get raw seat data for structure analysis
            raw_seat_data = []
            for seat in seats:
                if hasattr(seat, '__dict__'):
                    # Convert SeatData object to dict for analysis
                    raw_seat_data.append({
                        'id': getattr(seat, 'seat_id', ''),
                        'number': getattr(seat, 'seat_number', ''),
                        'row': getattr(seat, 'row_label', ''),
                        'level': getattr(seat, 'level_id', '').replace('bsf_venue_', '') if hasattr(seat, 'level_id') else '',
                        'x': float(getattr(seat, 'x_coord', 0)) if getattr(seat, 'x_coord', None) else 0,
                        'available': getattr(seat, 'available', False)
                    })
                else:
                    raw_seat_data.append(seat)
            
            # Analyze seating structure using the enhanced method
            structure_info = self.analyze_seating_structure(raw_seat_data)
            
            logger.info(f"=== SEAT PACK GENERATION ===")
            logger.info(f"Using {structure_info['strategy']} strategy with section-aware pack generation")
            logger.info(f"Pattern analysis: {structure_info['pattern_counts']}")
            logger.info(f"Available seats for packing: {len(seats)}")
            
            # Generate packs using structure-aware method
            if structure_info['strategy'] == 'mixed':
                # For mixed patterns, generate packs section by section
                packs = self._generate_mixed_pattern_packs(seats, sections, performance, structure_info)
            else:
                # Use the enhanced common generator with detected strategy
                packs = self._generate_standard_packs(seats, sections, performance, structure_info['strategy'])
            
            logger.info(f"=== PACK GENERATION COMPLETE: {len(packs)} total packs created ===")
            
            # ENHANCED FALLBACK: If 0 packs generated, use our proven algorithm
            if len(packs) == 0:
                logger.info("🔧 ZERO PACKS DETECTED: Switching to enhanced processor fallback method")
                try:
                    from .processor import BroadwaySFProcessor
                    processor = BroadwaySFProcessor()
                    enhanced_packs = processor._fallback_seat_pack_generation(seats, sections, performance)
                    logger.info(f"✅ ENHANCED FALLBACK: Generated {len(enhanced_packs)} packs using proven algorithm")
                    return enhanced_packs
                except Exception as e:
                    logger.error(f"Enhanced fallback also failed: {e}")
                    return packs  # Return original 0 packs
            
            return packs
                
        except Exception as e:
            logger.error(f"Enhanced seat pack generation failed: {e}, using fallback")
            return self._generate_fallback_packs(seats, sections, performance)
        finally:
            self._generating_packs = False
    
    def _generate_mixed_pattern_packs(self, seats: List[Any], sections: List[Any], performance: Any, structure_info: Dict[str, Any]) -> List[Any]:
        """Generate seat packs for venues with mixed consecutive/odd-even patterns."""
        from ...core.seat_pack_generator import generate_seat_packs
        import logging
        logger = logging.getLogger(__name__)
        
        all_packs = []
        venue_prefix_map = {"broadway_sf": "bsf"}
        
        # Group seats by their detected section patterns
        section_patterns = structure_info['sections']
        
        logger.info("--- Generating Mixed Pattern Packs ---")
        
        for level, level_data in section_patterns.items():
            logger.info(f"Processing Level: {level}")
            
            for row, row_data in level_data.items():
                logger.info(f"  Processing Row: {row}")
                
                # Process consecutive sections
                consecutive_seats = row_data.get('consecutive_seats', [])
                if consecutive_seats and len(consecutive_seats) >= 2:
                    section_seats = self._filter_seats_by_raw_data(seats, consecutive_seats)
                    if len(section_seats) >= 2:
                        logger.info(f"    Generating consecutive packs for {len(section_seats)} seats")
                        packs = generate_seat_packs(
                            all_seats=section_seats,
                            all_sections=sections,
                            performance=performance,
                            venue_prefix_map=venue_prefix_map,
                            venue=None,
                            min_pack_size=2,
                            packing_strategy="maximal",
                            seating_strategy="consecutive",
                            scraper_instance=None
                        )
                        all_packs.extend(packs)
                        logger.info(f"    Generated {len(packs)} consecutive packs")
                
                # Process odd sections
                odd_seats = row_data.get('odd_seats', [])
                if odd_seats and len(odd_seats) >= 2:
                    section_seats = self._filter_seats_by_raw_data(seats, odd_seats)
                    if len(section_seats) >= 2:
                        logger.info(f"    Generating odd-numbered packs for {len(section_seats)} seats")
                        packs = generate_seat_packs(
                            all_seats=section_seats,
                            all_sections=sections,
                            performance=performance,
                            venue_prefix_map=venue_prefix_map,
                            venue=None,
                            min_pack_size=2,
                            packing_strategy="maximal",
                            seating_strategy="odd_even",
                            scraper_instance=None
                        )
                        all_packs.extend(packs)
                        logger.info(f"    Generated {len(packs)} odd-numbered packs")
                
                # Process even sections
                even_seats = row_data.get('even_seats', [])
                if even_seats and len(even_seats) >= 2:
                    section_seats = self._filter_seats_by_raw_data(seats, even_seats)
                    if len(section_seats) >= 2:
                        logger.info(f"    Generating even-numbered packs for {len(section_seats)} seats")
                        packs = generate_seat_packs(
                            all_seats=section_seats,
                            all_sections=sections,
                            performance=performance,
                            venue_prefix_map=venue_prefix_map,
                            venue=None,
                            min_pack_size=2,
                            packing_strategy="maximal",
                            seating_strategy="odd_even",
                            scraper_instance=None
                        )
                        all_packs.extend(packs)
                        logger.info(f"    Generated {len(packs)} even-numbered packs")
        
        logger.info(f"--- Mixed Pattern Pack Generation Complete ---")
        logger.info(f"Total generated packs: {len(all_packs)}")
        return all_packs
    
    def _generate_standard_packs(self, seats: List[Any], sections: List[Any], performance: Any, strategy: str) -> List[Any]:
        """Generate seat packs using standard strategy for uniform patterns."""
        from ...core.seat_pack_generator import generate_seat_packs
        import logging
        logger = logging.getLogger(__name__)
        
        venue_prefix_map = {"broadway_sf": "bsf"}
        
        logger.info(f"--- Generating Standard Packs ---")
        logger.info(f"Strategy: {strategy}")
        logger.info(f"Processing {len(seats)} seats across {len(sections)} sections")
        
        packs = generate_seat_packs(
            all_seats=seats,
            all_sections=sections,
            performance=performance,
            venue_prefix_map=venue_prefix_map,
            venue=None,
            min_pack_size=2,
            packing_strategy="maximal",
            seating_strategy=strategy,
            scraper_instance=None
        )
        
        logger.info(f"Generated {len(packs)} packs using {strategy} strategy")
        return packs
    
    def _generate_fallback_packs(self, seats: List[Any], sections: List[Any], performance: Any) -> List[Any]:
        """Fallback pack generation method."""
        from ...core.seat_pack_generator import generate_seat_packs
        
        venue_prefix_map = {"broadway_sf": "bsf"}
        
        return generate_seat_packs(
            all_seats=seats,
            all_sections=sections,
            performance=performance,
            venue_prefix_map=venue_prefix_map,
            venue=None,
            min_pack_size=2,
            packing_strategy="maximal",
            seating_strategy="consecutive",
            scraper_instance=None
        )
    
    def _filter_seats_by_raw_data(self, all_seats: List[Any], raw_section_seats: List[Dict[str, Any]]) -> List[Any]:
        """Filter seat objects based on raw seat data from a section."""
        section_seat_ids = {seat['id'] for seat in raw_section_seats}
        
        filtered_seats = []
        for seat in all_seats:
            # Extract raw seat ID from performance-specific seat ID
            raw_seat_id = getattr(seat, 'seat_id', '').split('_')[-1] if hasattr(seat, 'seat_id') else ''
            if raw_seat_id in section_seat_ids:
                filtered_seats.append(seat)
        
        return filtered_seats
    
    def get_seat_pack_strategy(self) -> str:
        """
        Return the seat pack generation strategy for Broadway SF.
        
        This method is kept for backwards compatibility but the enhanced
        generate_seat_packs method now uses detailed structure analysis.
        """
        return "mixed"  # Indicates that detailed analysis should be used

    async def extract_all_data(self) -> Dict[str, Any]:
        """
        Extract data from all Broadway SF API endpoints.
        
        This method orchestrates the API calls in parallel since performance ID
        is always available in the URL, significantly reducing scrape time.
        
        Returns:
            Combined data from all endpoints
            
        Raises:
            ScrapingException: If critical endpoints fail
        """
        self.logger.info(f"Starting Broadway SF API data extraction for URL: {self.url}")
        
        try:
            # Parse URL to get parameters
            url_params = await self.parse_url_parameters(self.url)
            title_slug = url_params['title_slug']
            venue_slug = url_params['venue_slug']
            performance_id = url_params.get('performance_id')
            
            if not performance_id:
                raise ParseException("Performance ID not found in URL - required for parallel API calls")
            
            # OPTIMIZATION: Make API calls in parallel since we have performance ID
            self.logger.info("Making parallel API calls for optimal performance...")
            
            # Create tasks for parallel execution
            calendar_task = self.extract_from_endpoint(
                'calendar_service',
                variables={
                    'titleSlug': title_slug,
                    'venueSlug': venue_slug,
                    'combined': False,
                    'ruleSetting': {},
                    'sourceId': 'AV_US_WEST'
                }
            )
            
            bolt_task = self.extract_from_endpoint(
                'bolt_api',
                path_values={
                    'title_slug': title_slug,
                    'venue_slug': venue_slug,
                    'performance_id': performance_id
                }
            )
            
            # Execute both API calls in parallel
            self.logger.info("Executing Calendar Service and Bolt API calls in parallel...")
            calendar_result, bolt_result = await asyncio.gather(
                calendar_task, 
                bolt_task,
                return_exceptions=True
            )
            
            # Handle any exceptions from parallel execution
            if isinstance(calendar_result, Exception):
                raise ParseException(f"Calendar Service API failed: {calendar_result}")
            if isinstance(bolt_result, Exception):
                raise ParseException(f"Bolt API failed: {bolt_result}")
            
            # Process responses
            calendar_data = self.process_api_response(calendar_result.data, 'calendar_service')
            seating_data = self.process_api_response(bolt_result.data, 'bolt_api')
            
            # Combine responses
            responses = {
                'calendar_service': calendar_data,
                'bolt_api': seating_data
            }
            
            combined_data = self.combine_responses(responses)
            
            self.logger.info("Successfully extracted all Broadway SF API data with parallel optimization")
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Broadway SF API extraction failed: {e}")
            raise
    
    def _extract_performance_id_from_calendar(self, calendar_data: Dict[str, Any]) -> Optional[str]:
        """Extract performance ID from calendar data."""
        try:
            performances = calendar_data.get('performances', [])
            if performances:
                performance_id = performances[0]['id']
                self.logger.info(f"Using first performance ID from calendar data: {performance_id}")
                return performance_id
            else:
                self.logger.warning("No performances found in calendar data")
        except (KeyError, IndexError, TypeError) as e:
            self.logger.warning(f"Error accessing calendar data structure: {e}")
        
        return None
    
    async def extract_from_endpoint(self, endpoint_name: str, **kwargs) -> Any:
        """
        Override to handle Broadway SF specific endpoint logic.
        
        Args:
            endpoint_name: Name of the endpoint
            **kwargs: Endpoint-specific parameters
            
        Returns:
            RequestResult with response data
        """
        if endpoint_name == "bolt_api":
            # Handle path parameter substitution for Bolt API
            return await self._extract_from_bolt_api(**kwargs)
        elif endpoint_name == "calendar_service":
            # Handle GraphQL request construction for Calendar Service
            return await self._extract_from_calendar_service(**kwargs)
        else:
            # Use base implementation for other endpoints
            return await super().extract_from_endpoint(endpoint_name, **kwargs)
    
    async def _extract_from_bolt_api(self, path_values: Dict[str, str] = None, **kwargs):
        """Extract from Bolt API with path parameter substitution."""
        endpoint_name = "bolt_api"
        if endpoint_name not in self.config.endpoints:
            raise ScrapingException(f"Endpoint '{endpoint_name}' not found in configuration")
        
        endpoint = self.config.endpoints[endpoint_name]
        
        # Extract domain info for dynamic headers and API URL
        domain_info = self._extract_domain_info(self.url)
        base_domain = domain_info['base_domain']
        full_domain = domain_info['full_domain']
        
        # Build URL with path parameters for REST endpoint
        if hasattr(endpoint, 'build_url'):
            url = endpoint.build_url("", path_values)
        else:
            # Fallback: manual path substitution with dynamic domain
            url = endpoint.url
            if path_values:
                for key, value in path_values.items():
                    placeholder = f"{{{key}}}"
                    url = url.replace(placeholder, str(value))
        
        # Replace hardcoded domain with dynamic domain
        url = url.replace('boltapi.broadwaysf.com', f'boltapi.{base_domain}')
        
        http_client = await self._get_http_client()
        
        # Prepare headers with Broadway SF specific headers
        headers = endpoint.headers.copy() if endpoint.headers else {}
        
        # Add Broadway SF specific headers with dynamic domain
        headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': f'{full_domain}/',
            'Origin': full_domain
        })
        
        self.logger.info(f"Calling Bolt API: GET {url}")
        
        # Make GET request to Bolt API
        result = await http_client.get(url=url, headers=headers)
        
        # Validate response
        if not await self.validate_response(result, "bolt_api"):
            raise ParseException("Response validation failed for bolt_api")
        
        # Cache successful response
        self._response_cache["bolt_api"] = result
        
        return result
    
    async def _extract_from_calendar_service(self, **kwargs):
        """Extract from Calendar Service with proper GraphQL request construction."""
        endpoint_name = "calendar_service"
        if endpoint_name not in self.config.endpoints:
            raise ScrapingException(f"Endpoint '{endpoint_name}' not found in configuration")
        
        endpoint = self.config.endpoints[endpoint_name]
        
        # Use GraphQL endpoint's create_request_payload method
        if hasattr(endpoint, 'create_request_payload'):
            # Extract variables from kwargs
            variables = kwargs.get('variables', {})
            payload = endpoint.create_request_payload(variables)
        else:
            # Fallback to direct json_data
            payload = kwargs.get('json_data', {})
        
        http_client = await self._get_http_client()
        
        # Prepare headers with Broadway SF specific headers
        headers = endpoint.headers.copy() if endpoint.headers else {}
        
        # Extract domain info for dynamic headers
        domain_info = self._extract_domain_info(self.url)
        full_domain = domain_info['full_domain']
        
        # Add Broadway SF specific headers with dynamic domain
        headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': f'{full_domain}/',
            'Origin': full_domain
        })
        
        self.logger.info(f"Calling Calendar Service: POST {endpoint.url}")
        self.logger.debug(f"GraphQL payload: {payload}")
        
        # Make POST request to Calendar Service
        result = await http_client.post(url=endpoint.url, headers=headers, json_data=payload)
        
        # Validate response
        if not await self.validate_response(result, endpoint_name):
            raise ParseException("Response validation failed for calendar_service")
        
        # Cache successful response
        self._response_cache[endpoint_name] = result
        
        return result
    
    def cleanup(self):
        """Clean up resources to prevent memory leaks."""
        try:
            # Call base class cleanup first
            super().cleanup()
            
            # Broadway SF specific cleanup
            self._limit_cache_size()
            
            # Clear Broadway SF specific caches
            if hasattr(self, '_http_clients'):
                for client in self._http_clients:
                    try:
                        if hasattr(client, 'close'):
                            client.close()
                    except Exception as e:
                        self.logger.warning(f"Error closing HTTP client: {e}")
                self._http_clients.clear()
            
            self.logger.info("Broadway SF scraper cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def _limit_cache_size(self):
        """Limit cache size to prevent memory leaks."""
        if hasattr(self, '_response_cache') and len(self._response_cache) > self._max_cache_size:
            # Remove oldest entries
            keys_to_remove = list(self._response_cache.keys())[:-self._max_cache_size]
            for key in keys_to_remove:
                del self._response_cache[key]
            self.logger.debug(f"Cleaned cache, removed {len(keys_to_remove)} entries")
    
    async def _get_http_client(self):
        """Get HTTP client with memory management."""
        http_client = await super()._get_http_client()
        
        # Track HTTP clients for cleanup
        if hasattr(self, '_http_clients'):
            if len(self._http_clients) >= self._max_http_clients:
                # Remove oldest client
                old_client = self._http_clients.pop(0)
                try:
                    if hasattr(old_client, 'close'):
                        old_client.close()
                except Exception as e:
                    self.logger.warning(f"Error closing old HTTP client: {e}")
            
            self._http_clients.append(http_client)
        
        return http_client