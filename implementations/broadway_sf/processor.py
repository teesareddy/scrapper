from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal, InvalidOperation
from ...core.data_schemas import ScrapedData, VenueData, EventData, PerformanceData, ZoneData, LevelData, SectionData, SeatData, SeatPackData, ScraperConfigData
from ...core.seat_pack_generator import generate_seat_packs, detect_venue_seat_structure
from ...models import Venue  # Add this import for DB access

# Utility function to fetch seat_structure from DB (async-safe)
def get_venue_seat_structure(source_venue_id: str, source_website: str) -> str:
    try:
        # Use sync_to_async to make Django ORM calls safe in async context
        from asgiref.sync import sync_to_async
        
        @sync_to_async
        def _get_venue():
            return Venue.objects.filter(source_venue_id=source_venue_id, source_website=source_website).first()
        
        # For now, return None to avoid async issues - this will be improved later
        # The dynamic detection will handle this case properly
        return None
    except Exception:
        pass
    return None

class BroadwaySFProcessor:

    def __init__(self):
        pass

    def process(self, calendar_data: Dict[str, Any], seating_data: Dict[str, Any],
                url: str, scrape_job_id: str = None, scraper_instance=None, enriched_data: Dict[str, Any] = None) -> ScrapedData:
        # Note: enriched_data parameter kept for backwards compatibility but no longer used for markup pricing
        
        venue_info = self._process_venue_info(seating_data, calendar_data)
        event_info = self._process_event_info(calendar_data, venue_info.source_venue_id, url)
        performance_info = self._process_performance_info(seating_data, event_info.source_event_id, venue_info.source_venue_id, url)
        
        # Generate internal event and performance IDs for unique component IDs
        from ...core.id_generator import InternalIDGenerator
        internal_event_id = InternalIDGenerator.generate_event_id("bsf", event_info)
        internal_venue_id = InternalIDGenerator.generate_venue_id("bsf", venue_info)
        internal_performance_id = InternalIDGenerator.generate_performance_id("bsf", performance_info, internal_event_id, internal_venue_id)
        
        # Use full internal performance ID as prefix for component uniqueness
        perf_prefix = internal_performance_id
        
        zones = self._process_zones(seating_data, perf_prefix)
        levels = self._process_levels(seating_data, venue_info)
        
        # Create seats first to detect seat structure
        temp_sections = self._process_sections(seating_data, levels, "consecutive", venue_info) # Temporary sections
        temp_seats = self._process_seats(seating_data, zones, levels, temp_sections, "consecutive", perf_prefix)
        
        # Enhanced seating structure detection using new architecture with validation
        seat_structure = get_venue_seat_structure(venue_info.source_venue_id, venue_info.source_website)
        structure_info = None
        
        if not seat_structure:
            # Use enhanced dynamic detection with passed scraper instance or fallback
            if scraper_instance and hasattr(scraper_instance, 'analyze_seating_structure'):
                # Validate seat data before analysis
                seat_data = seating_data.get('seats', [])
                if not seat_data or not isinstance(seat_data, list):
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Invalid or empty seat data for structure detection, using fallback")
                    seat_structure = "consecutive"
                else:
                    try:
                        # Enhanced analysis returns detailed structure info
                        detected_result = scraper_instance.analyze_seating_structure(seat_data)
                        
                        if isinstance(detected_result, dict):
                            # New enhanced analysis returns detailed info
                            structure_info = detected_result
                            seat_structure = detected_result.get('strategy', 'consecutive')
                        else:
                            # Backwards compatibility with old string return
                            seat_structure = detected_result if detected_result not in ["unknown", None] else "consecutive"
                        
                        # Log the detection result
                        import logging
                        logger = logging.getLogger(__name__)
                        if structure_info:
                            logger.info(f"Enhanced seat structure analysis for Broadway SF venue {venue_info.source_venue_id}: "
                                      f"strategy={seat_structure}, patterns={structure_info.get('pattern_counts', {})}")
                        else:
                            logger.info(f"Basic seat structure detection for Broadway SF venue {venue_info.source_venue_id}: {seat_structure}")
                            
                    except Exception as e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.error(f"Failed to detect seat structure: {e}, using fallback")
                        seat_structure = "consecutive"
            else:
                # No scraper instance available, use fallback
                import logging
                logger = logging.getLogger(__name__)
                logger.warning("No scraper instance available for seat structure detection, using fallback")
                seat_structure = "consecutive"
        
        venue_info.seat_structure = seat_structure
        
        # Store structure info for enhanced pack generation
        if structure_info:
            venue_info.enhanced_structure_info = structure_info
        
        # Now recreate sections with the correct seat structure
        sections = self._process_sections(seating_data, levels, seat_structure, venue_info)
        seats = self._process_seats(seating_data, zones, levels, sections, seat_structure, perf_prefix)
        
        # Enhanced filtering to prevent ghost packs
        raw_seats = seating_data.get('seats', [])
        
        # Apply strict filtering to prevent ghost packs
        available_seats = self._filter_genuinely_available_seats(seats, raw_seats)
        
        seat_packs = self._process_seat_packs(available_seats, sections, performance_info, scraper_instance)

        # Link venue-level levels to this performance
        self._link_levels_to_performance(levels, performance_info)

        return ScrapedData(
            source_website="broadway_sf",
            scraped_at=datetime.utcnow(),
            url=url,
            venue_info=venue_info,
            event_info=event_info,
            performance_info=performance_info,
            levels=levels,
            zones=zones,
            sections=sections,
            seats=seats,
            seat_packs=seat_packs,
            scraper_config=ScraperConfigData(
                scraper_name="broadway_sf_scraper_v5"
            ),
            scraper_version="v5",
            scraper_instance=scraper_instance
        )

    def _link_levels_to_performance(self, levels: List[LevelData], performance: PerformanceData):
        """Link venue-level levels to this performance (async-safe)"""
        try:
            # Skip database operations in async context to avoid sync errors
            # This will be handled by the database handler later
            import asyncio
            if asyncio.current_task() is not None:
                return
            
            from scrapers.models import Level, Performance, PerformanceLevel
            
            # Get the performance record
            perf_obj = Performance.objects.get(internal_performance_id=performance.source_performance_id)
            
            # Link each level to this performance
            for level_data in levels:
                level_obj = Level.objects.get(internal_level_id=level_data.level_id)
                
                # Create or get the performance-level link
                PerformanceLevel.objects.get_or_create(
                    performance=perf_obj,
                    level=level_obj,
                    defaults={'display_order': 0}
                )
                
        except (Performance.DoesNotExist, Level.DoesNotExist) as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not link levels to performance: {e}")
        except Exception:
            # Non-critical error, probably async context
            pass

    def _process_venue_info(self, seating_data: Dict[str, Any], calendar_data: Dict[str, Any] = None) -> VenueData:
        """Process venue information from seating data and calendar data"""
        performance = seating_data.get('performance', {})
        venue_name = performance.get('venue', 'Broadway SF Venue')
        venue_id = performance.get('avVenueId', 'unknown')
        
        source_venue_id = f"{venue_id}" if venue_id != 'unknown' else f"{venue_name.lower().replace(' ', '_')}"
        
        # Extract timezone from calendar API response
        venue_timezone = self._extract_venue_timezone(calendar_data)
        
        return VenueData(
            name=venue_name,
            source_venue_id=source_venue_id,
            source_website="broadway_sf",
            city="San Francisco",
            state="CA",
            country="US",
            venue_timezone=venue_timezone
        )

    def _process_event_info(self, calendar_data: Dict[str, Any], venue_source_id: str, url: str = None) -> EventData:
        """Process event information from calendar data"""
        event_name = "Broadway SF Event" # Default fallback
        source_event_id = f"{venue_source_id}" # Default source_event_id

        # Prioritize event_info from extractor
        extracted_event_info = calendar_data.get('event_info', {})
        if extracted_event_info and extracted_event_info.get('title'):
            event_name = extracted_event_info['title']
            # If event_info has a title, we can use it to generate a more specific source_event_id
            source_event_id = f"{venue_source_id}_{event_name.lower().replace(' ', '_')}"
        else:
            # Fallback to show_data from calendar_data network response
            show_data = calendar_data.get('data', {}).get('getShow', {}).get('show', {})
            if show_data and show_data.get('title'):
                event_name = show_data['title']
                event_id = show_data.get('id', 'unknown')
                source_event_id = f"{event_id}" if event_id != 'unknown' else f"{venue_source_id}_{event_name.lower().replace(' ', '_')}"

        return EventData(
            name=event_name,
            source_event_id=source_event_id,
            source_website="broadway_sf",
            url=url,
            currency="USD",
            event_type="theater"
        )

    def _process_performance_info(self, seating_data: Dict[str, Any], event_source_id: str, venue_source_id: str, url: str) -> PerformanceData:
        """Process performance information from seating data"""
        performance = seating_data.get('performance', {})
        
        # Extract performance ID
        import re
        performance_id_match = re.search(r'/tickets/([A-F0-9-]{36})', url, re.IGNORECASE)
        performance_id = performance_id_match.group(1) if performance_id_match else 'unknown'
        
        # Extract performance datetime
        performance_datetime = datetime.utcnow()  # Default fallback
        if performance.get('dateTimeISO'):
            try:
                from dateutil import parser
                performance_datetime = parser.parse(performance['dateTimeISO'])
            except Exception:
                pass
        
        return PerformanceData(
            source_performance_id=performance_id,
            source_website="broadway_sf",
            performance_datetime_utc=performance_datetime,
            event_source_id=event_source_id,
            venue_source_id=venue_source_id,
            seat_map_url=url,
            performance_url=url
        )

    def _process_zones(self, seating_data: Dict[str, Any], perf_prefix: str = None) -> List[ZoneData]:
        """Process Broadway SF zones from seating data"""
        zones = []
        zone_lookup = seating_data.get('zones', {})
        legend_lookup = {
            item['id']: item['description'] 
            for item in seating_data.get('legends', [])
            if isinstance(item, dict) and 'id' in item and 'description' in item
        }

        for zone_id, zone_data in zone_lookup.items():
            # Calculate price range for this zone
            min_price = None
            max_price = None
            
            zone_tickets = zone_data.get('tickets', {})
            for ticket_id, ticket_price in zone_tickets.items():
                try:
                    price = Decimal(str(ticket_price.get('total', 0)))
                    if price > 0:
                        if min_price is None or price < min_price:
                            min_price = price
                        if max_price is None or price > max_price:
                            max_price = price
                except (ValueError, TypeError, InvalidOperation):
                    continue

            # Create performance-specific zone ID to avoid conflicts
            unique_zone_id = f"{perf_prefix}_{zone_id}" if perf_prefix else zone_id
            
            zone = ZoneData(
                zone_id=unique_zone_id,
                source_website="broadway_sf",
                name=legend_lookup.get(zone_id, f"Zone {zone_id}"),
                raw_identifier=zone_id,
                wheelchair_accessible='wheelchair' in zone_data.get('tags', []),
                min_price=min_price,
                max_price=max_price,
                miscellaneous={
                    "available": zone_data.get('available', True),
                    "default_ticket": zone_data.get('defaultTicket'),
                    "tags": zone_data.get('tags', [])
                }
            )
            zones.append(zone)

        return zones

    def _process_levels(self, seating_data: Dict[str, Any], venue_info: VenueData) -> List[LevelData]:
        """Process Broadway SF levels - VENUE LEVEL (created only once) - async-safe"""
        levels_map = {}
        seats = seating_data.get('seats', [])
        
        # Check if we're in async context
        try:
            import asyncio
            if asyncio.current_task() is not None:
                # In async context - create levels without database operations
                return self._create_levels_without_db(seats)
        except RuntimeError:
            # Not in async context, proceed normally
            pass
        
        from scrapers.models import Level, Venue
        
        # Get or create venue record
        venue, created = Venue.objects.get_or_create(
            source_venue_id=venue_info.source_venue_id,
            source_website=venue_info.source_website,
            defaults={
                'name': venue_info.name,
                'address': venue_info.address,
                'city': venue_info.city,
                'state': venue_info.state,
                'country': venue_info.country,
                'venue_timezone': venue_info.venue_timezone
            }
        )
        
        # Extract unique levels from seat data
        unique_levels = set()
        for seat in seats:
            level = seat.get('level')
            if level:
                unique_levels.add(level)
        
        # Get or create levels for this venue
        for level_name in unique_levels:
            # Create venue-level level ID (persistent)
            level_id = f"bsf_venue_{level_name}"
            
            # Check if level already exists for this venue
            existing_level = Level.objects.filter(
                internal_level_id=level_id,
                source_website="broadway_sf"
            ).first()
            
            if existing_level:
                # Use existing level
                levels_map[level_name] = LevelData(
                    level_id=existing_level.internal_level_id,
                    source_website=existing_level.source_website,
                    name=existing_level.name,
                    raw_name=existing_level.raw_name or level_name
                )
            else:
                # Create new level for this venue
                level_obj = Level.objects.create(
                    internal_level_id=level_id,
                    source_website="broadway_sf",
                    name=self._format_level_name(level_name),
                    raw_name=level_name,
                    level_type=self._get_level_type(level_name)
                )
                
                levels_map[level_name] = LevelData(
                    level_id=level_obj.internal_level_id,
                    source_website=level_obj.source_website,
                    name=level_obj.name,
                    raw_name=level_obj.raw_name
                )
        
        return list(levels_map.values())

    def _create_levels_without_db(self, seats) -> List[LevelData]:
        """Create levels without database operations for async context"""
        levels_map = {}
        
        # Extract unique levels from seat data
        unique_levels = set()
        for seat in seats:
            level = seat.get('level')
            if level:
                unique_levels.add(level)
        
        # Create levels without database operations
        for level_name in unique_levels:
            level_id = f"bsf_venue_{level_name}"
            
            levels_map[level_name] = LevelData(
                level_id=level_id,
                source_website="broadway_sf",
                name=self._format_level_name(level_name),
                raw_name=level_name
            )
        
        return list(levels_map.values())

    def _process_sections(self, seating_data: Dict[str, Any], levels: List[LevelData], venue_seat_structure: str, venue_info: VenueData) -> List[SectionData]:
        """Process Broadway SF sections - VENUE LEVEL (created only once) - async-safe"""
        sections = []
        sections_data = seating_data.get('sections', [])
        
        # Check if we're in async context
        try:
            import asyncio
            if asyncio.current_task() is not None:
                # In async context - create sections without database operations
                return self._create_sections_without_db(sections_data, levels, venue_seat_structure)
        except RuntimeError:
            # Not in async context, proceed normally
            pass
        
        from scrapers.models import Section, Level
        
        # Create level lookup
        level_lookup = {level.level_id: level for level in levels}
        
        for section_data in sections_data:
            section_id = section_data.get('id', section_data.get('name', 'unknown'))
            section_name = section_data.get('name', f'Section {section_id}')
            
            # Try to match section to a level
            level_id = None
            for level in levels:
                if level.name.lower() in section_name.lower() or level.level_id == section_id:
                    level_id = level.level_id
                    break
            
            # If no match, use first available level
            if not level_id and levels:
                level_id = levels[0].level_id

            if level_id:
                # Create venue-level section ID (persistent)
                unique_section_id = f"bsf_venue_{section_id}"
                
                # Check if section already exists for this level
                existing_section = Section.objects.filter(
                    internal_section_id=unique_section_id,
                    level_id__internal_level_id=level_id,
                    source_website="broadway_sf"
                ).first()
                
                if existing_section:
                    # Use existing section
                    section = SectionData(
                        section_id=existing_section.internal_section_id,
                        level_id=existing_section.level_id.internal_level_id,
                        source_website=existing_section.source_website,
                        name=existing_section.name,
                        raw_name=existing_section.raw_name or section_name,
                        numbering_scheme=venue_seat_structure
                    )
                else:
                    # Create new section for this level
                    level_obj = Level.objects.get(internal_level_id=level_id)
                    section_obj = Section.objects.create(
                        internal_section_id=unique_section_id,
                        level_id=level_obj,
                        source_website="broadway_sf",
                        name=section_name,
                        raw_name=section_name
                    )
                    
                    section = SectionData(
                        section_id=section_obj.internal_section_id,
                        level_id=section_obj.level_id.internal_level_id,
                        source_website=section_obj.source_website,
                        name=section_obj.name,
                        raw_name=section_obj.raw_name,
                        numbering_scheme=venue_seat_structure
                    )
                
                sections.append(section)

        return sections

    def _create_sections_without_db(self, sections_data, levels: List[LevelData], venue_seat_structure: str) -> List[SectionData]:
        """Create sections without database operations for async context"""
        sections = []
        
        # Create level lookup
        level_lookup = {level.level_id: level for level in levels}
        
        for section_data in sections_data:
            section_id = section_data.get('id', section_data.get('name', 'unknown'))
            section_name = section_data.get('name', f'Section {section_id}')
            
            # Try to match section to a level
            level_id = None
            for level in levels:
                if level.name.lower() in section_name.lower() or level.level_id == section_id:
                    level_id = level.level_id
                    break
            
            # If no match, use first available level
            if not level_id and levels:
                level_id = levels[0].level_id

            if level_id:
                unique_section_id = f"bsf_venue_{section_id}"
                
                section = SectionData(
                    section_id=unique_section_id,
                    level_id=level_id,
                    source_website="broadway_sf",
                    name=section_name,
                    raw_name=section_name,
                    numbering_scheme=venue_seat_structure
                )
                
                sections.append(section)

        return sections

    def _process_seats(self, seating_data: Dict[str, Any], zones: List[ZoneData], 
                      levels: List[LevelData], sections: List[SectionData], venue_seat_structure: str, perf_prefix: str = None) -> List[SeatData]:
        """Process Broadway SF seats from seating data"""
        seats = []
        seats_data = seating_data.get('seats', [])
        
        # Create lookups - need to map raw IDs to venue-level objects
        zone_lookup = {}
        for zone in zones:
            # Extract raw zone ID from performance-specific ID
            raw_zone_id = zone.raw_identifier if hasattr(zone, 'raw_identifier') else zone.zone_id.split('_', -1)[-1]
            zone_lookup[raw_zone_id] = zone
            
        level_lookup = {}
        for level in levels:
            # Extract raw level name from venue-level ID
            raw_level_name = level.raw_name if hasattr(level, 'raw_name') else level.level_id.replace('bsf_venue_', '')
            level_lookup[raw_level_name] = level
            
        section_lookup = {}
        for section in sections:
            # Extract raw section name from venue-level ID
            raw_section_name = section.raw_name if hasattr(section, 'raw_name') else section.section_id.replace('bsf_venue_', '')
            section_lookup[raw_section_name] = section
        
        for seat_data in seats_data:
            raw_seat_id = seat_data.get('id', 'unknown')
            # Create performance-specific seat ID to avoid conflicts
            seat_id = f"{perf_prefix}_{raw_seat_id}" if perf_prefix else raw_seat_id
            zone_id = seat_data.get('zone', 'unknown')
            level_id = seat_data.get('level', 'unknown')
            section_id = seat_data.get('section', level_id)  # Use level as section if not specified
            
            # Find or create section (venue-level)
            if section_id not in section_lookup and level_id in level_lookup:
                # Create a default section for this level with venue-level ID
                level_obj = level_lookup[level_id]
                unique_section_id = f"bsf_venue_{section_id}"
                
                # Check if we're in async context
                is_async_context = False
                try:
                    import asyncio
                    if asyncio.current_task() is not None:
                        is_async_context = True
                except RuntimeError:
                    pass
                
                if is_async_context:
                    # In async context - create section without database operations
                    section = SectionData(
                        section_id=unique_section_id,
                        level_id=level_obj.level_id,
                        source_website="broadway_sf",
                        name=f"Section {section_id}",
                        raw_name=section_id,
                        numbering_scheme=venue_seat_structure
                    )
                else:
                    # Not in async context - proceed with database operations
                    from scrapers.models import Section
                    existing_section = Section.objects.filter(
                        internal_section_id=unique_section_id,
                        level_id__internal_level_id=level_obj.level_id,
                        source_website="broadway_sf"
                    ).first()
                    
                    if existing_section:
                        section = SectionData(
                            section_id=existing_section.internal_section_id,
                            level_id=existing_section.level_id.internal_level_id,
                            source_website="broadway_sf",
                            name=existing_section.name,
                            raw_name=existing_section.raw_name or section_id,
                            numbering_scheme=venue_seat_structure
                        )
                    else:
                        # Create new section
                        section_obj = Section.objects.create(
                            internal_section_id=unique_section_id,
                            level_id=level_obj.level_id,
                            source_website="broadway_sf",
                            name=f"Section {section_id}",
                            raw_name=section_id
                        )
                        section = SectionData(
                            section_id=section_obj.internal_section_id,
                            level_id=section_obj.level_id.internal_level_id,
                            source_website="broadway_sf",
                            name=section_obj.name,
                            raw_name=section_obj.raw_name,
                            numbering_scheme=venue_seat_structure
                        )
                
                sections.append(section)
                section_lookup[section_id] = section
            
            if zone_id in zone_lookup and section_id in section_lookup:
                # Get price from zone data
                price = None
                zone = zone_lookup[zone_id]
                section = section_lookup[section_id]
                if zone.min_price:
                    price = zone.min_price
                
                seat = SeatData(
                    seat_id=seat_id,
                    section_id=section.section_id,  # Use venue-level section ID
                    zone_id=zone.zone_id,  # Use performance-specific zone ID
                    source_website="broadway_sf",
                    row_label=seat_data.get('row', '1'),
                    seat_number=seat_data.get('number', '1'),
                    seat_type="standard",
                    x_coord=Decimal(str(seat_data.get('x', 0))) if seat_data.get('x') else None,
                    y_coord=Decimal(str(seat_data.get('y', 0))) if seat_data.get('y') else None,
                    status="available" if seat_data.get('available', False) else "unavailable",
                    price=price,
                    available=seat_data.get('available', False),
                    level_id=level_lookup[level_id].level_id if level_id in level_lookup else level_id  # Use venue-level level ID
                )
                seats.append(seat)

        return seats

    def _process_seat_packs(self, seats: List[SeatData], sections: List[SectionData], performance: PerformanceData, scraper_instance=None) -> List[SeatPackData]:
        """Process Broadway SF seat packs using the new strategy-aware architecture with validation."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Input validation
        if not seats or not isinstance(seats, list):
            return []
        
        if not sections or not isinstance(sections, list):
            return []
        
        if not performance:
            return []
        
        # Filter to only available seats for pack generation
        available_seats = [seat for seat in seats if getattr(seat, 'available', False)]
        if not available_seats:
            return []
        
        try:
            # TEMPORARY FIX: Force enhanced fallback due to scraper generating 0 packs
            # TODO: Remove this bypass once scraper's generate_seat_packs is fixed
            logger.info(f"🔧 FORCING enhanced fallback method due to 0 pack issue - processing {len(available_seats)} seats")
            return self._fallback_seat_pack_generation(available_seats, sections, performance)
            
            # Original logic (temporarily disabled)
            # if scraper_instance:
            #     # Check for recursion guard
            #     if hasattr(scraper_instance, '_processor_generating_packs') and scraper_instance._processor_generating_packs:
            #         return self._fallback_seat_pack_generation(available_seats, sections, performance)
            #     
            #     try:
            #         # Set recursion guard
            #         scraper_instance._processor_generating_packs = True
            #         
            #         # Use the new architecture with dynamic strategy detection
            #         packs = scraper_instance.generate_seat_packs(available_seats, sections, performance)
            #         
            #         # Basic validation to ensure we have valid packs (without triggering recursion)
            #         if not packs or not isinstance(packs, list):
            #             return self._fallback_seat_pack_generation(available_seats, sections, performance)
            #         
            #         # Basic pack count validation
            #         if len(packs) == 0:
            #             return self._fallback_seat_pack_generation(available_seats, sections, performance)
            #         
            #         return packs
            #     finally:
            #         # Clear recursion guard
            #         scraper_instance._processor_generating_packs = False
            # else:
            #     return self._fallback_seat_pack_generation(available_seats, sections, performance)
                
        except Exception as e:
            logger.error(f"Exception in seat pack processing: {e}")
            return self._fallback_seat_pack_generation(available_seats, sections, performance)
    
    def _fallback_seat_pack_generation(self, seats: List[SeatData], sections: List[SectionData], performance: PerformanceData) -> List[SeatPackData]:
        """Enhanced seat pack generation using physical clustering and pattern detection.
        
        Original fallback logic (preserved as comment):
        # Broadway SF venue prefix mapping
        # venue_prefix_map = {"broadway_sf": "bsf"}
        # venue = self._create_venue_object_from_enriched_data()
        # packs = generate_seat_packs(all_seats=seats, all_sections=sections, performance=performance, 
        #                           venue_prefix_map=venue_prefix_map, venue=venue, min_pack_size=2, packing_strategy="maximal")
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"🚀 ENHANCED FALLBACK: Starting with {len(seats)} seats")
            
            # Convert SeatData objects to dict format for enhanced algorithm
            seats_dict_data = self._convert_seats_to_dict(seats)
            logger.info(f"🔄 ENHANCED FALLBACK: Converted {len(seats_dict_data)} seats to dict format")
            
            # Apply enhanced seat pack algorithm
            enhanced_packs = self._find_enhanced_seating_packs(seats_dict_data)
            logger.info(f"🎯 ENHANCED FALLBACK: Generated {sum(len(rows) for level in enhanced_packs.values() for section in level.values() for rows in section.values())} pack groups")
            
            # Convert results back to SeatPackData objects
            seat_pack_objects = self._convert_packs_to_seat_pack_data(enhanced_packs, seats, sections, performance)
            logger.info(f"✅ ENHANCED FALLBACK: Created {len(seat_pack_objects)} final seat pack objects")
            
            return seat_pack_objects
            
        except Exception as e:
            logger.error(f"Enhanced seat pack generation failed: {e}")
            # Fallback to empty list to prevent crashes
            return []
    
    def _get_venue_from_database(self, performance: PerformanceData):
        """Fetch venue object from database for markup pricing."""
        try:
            # Import Venue model
            from ...models import Venue
            
            # Get venue using source_venue_id and source_website from performance
            venue = Venue.objects.filter(
                source_venue_id=performance.venue_source_id,
                source_website="broadway_sf"
            ).first()
            
            return venue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not fetch venue from database: {e}")
            return None

    def _convert_seats_to_dict(self, seats: List[SeatData]) -> List[Dict]:
        """Convert SeatData objects to dict format for enhanced algorithm."""
        seats_dict = []
        for seat in seats:
            # Extract raw seat ID (remove performance prefix)
            raw_seat_id = seat.seat_id.split('_')[-1] if '_' in seat.seat_id else seat.seat_id
            
            # Create seat dict compatible with enhanced algorithm
            seat_dict = {
                'id': raw_seat_id,
                'number': seat.seat_number,
                'row': seat.row_label,
                'level': seat.level_id.replace('bsf_venue_', '') if seat.level_id.startswith('bsf_venue_') else seat.level_id,
                'section': seat.section_id.replace('bsf_venue_', '') if seat.section_id.startswith('bsf_venue_') else seat.section_id,
                'zone': seat.zone_id.split('_')[-1] if '_' in seat.zone_id else seat.zone_id,  # Extract raw zone ID
                'x': float(seat.x_coord) if seat.x_coord else 0,
                'y': float(seat.y_coord) if seat.y_coord else 0,
                'available': seat.available,
                'label': f"{seat.row_label}{seat.seat_number}",  # Create label for display
                # Store reference to original SeatData for later use
                '_seat_data_ref': seat
            }
            seats_dict.append(seat_dict)
        
        return seats_dict

    def _find_enhanced_seating_packs(self, seats_data: List[Dict]) -> Dict:
        """
        Enhanced seat pack detection using physical clustering and pattern recognition.
        Direct implementation of the proven find_seating_packs() function from braodway-sf-seat-pack.py
        """
        # Create data dict in the format expected by the original function
        data = {'seats': seats_data}
        
        # Apply the exact logic from find_seating_packs()
        if 'seats' not in data:
            return {}

        available_seats = [seat for seat in data.get('seats', []) if seat.get('available', False)]

        # Group seats by their primary identifiers
        grouped_seats = {}
        for seat in available_seats:
            level = seat.get('level', 'N/A')
            section = seat.get('section', 'N/A')
            row = seat.get('row', 'N/A')
            grouped_seats.setdefault(level, {}).setdefault(section, {}).setdefault(row, []).append(seat)

        final_packs = {}
        for level, sections in grouped_seats.items():
            final_packs.setdefault(level, {})
            for section, rows in sections.items():
                final_packs[level].setdefault(section, {})
                for row, seats in rows.items():
                    if len(seats) < 1:
                        continue

                    # 1. Sort all seats in the row by their physical X-coordinate
                    sorted_by_x = sorted(seats, key=lambda s: s['x'])

                    # 2. Identify physical clusters by finding large gaps in X-coordinates
                    clusters = []
                    if sorted_by_x:
                        current_cluster = [sorted_by_x[0]]
                        # A reasonable pixel gap to define a new section/aisle
                        CLUSTER_GAP_THRESHOLD = 50

                        for i in range(1, len(sorted_by_x)):
                            prev_seat = sorted_by_x[i - 1]
                            current_seat = sorted_by_x[i]

                            if current_seat['x'] - prev_seat['x'] > CLUSTER_GAP_THRESHOLD:
                                clusters.append(current_cluster)
                                current_cluster = [current_seat]
                            else:
                                current_cluster.append(current_seat)
                        clusters.append(current_cluster)

                    # 3. Process each cluster independently to find packs
                    all_packs_for_row = []
                    for cluster in clusters:
                        sorted_cluster = sorted(cluster, key=lambda s: int(s['number']))

                        if not sorted_cluster: 
                            continue

                        current_pack = [sorted_cluster[0]]
                        pack_step = None  # Will be 1 for consecutive, 2 for odd/even

                        for i in range(1, len(sorted_cluster)):
                            prev_seat = sorted_cluster[i - 1]
                            current_seat = sorted_cluster[i]

                            # Always break a pack if the zone changes
                            if current_seat.get('zone') != prev_seat.get('zone'):
                                all_packs_for_row.append(current_pack)
                                current_pack = [current_seat]
                                pack_step = None
                                continue

                            diff = int(current_seat['number']) - int(prev_seat['number'])

                            if len(current_pack) == 1:
                                # This is the second seat; establish the pattern for this pack
                                if diff in [1, 2]:
                                    pack_step = diff
                                    current_pack.append(current_seat)
                                else:
                                    # The second seat doesn't form a sequence, so the first was a pack of 1
                                    all_packs_for_row.append(current_pack)
                                    current_pack = [current_seat]
                                    pack_step = None
                            elif diff == pack_step:
                                # The pattern continues, add the seat
                                current_pack.append(current_seat)
                            else:
                                # The pattern is broken, end the old pack and start a new one
                                all_packs_for_row.append(current_pack)
                                current_pack = [current_seat]
                                pack_step = None  # Reset pattern for the new pack

                        all_packs_for_row.append(current_pack)

                    if all_packs_for_row:
                        final_packs[level][section][row] = sorted(all_packs_for_row, key=lambda p: int(p[0]['number']))

        return final_packs

    def _convert_packs_to_seat_pack_data(self, enhanced_packs: Dict, original_seats: List[SeatData], 
                                       sections: List[SectionData], performance: PerformanceData) -> List[SeatPackData]:
        """Convert enhanced pack results back to SeatPackData objects with proper IDs."""
        from ...core.data_schemas import SeatPackData
        from decimal import Decimal
        import uuid
        
        seat_pack_objects = []
        seat_lookup = {seat.seat_id: seat for seat in original_seats}
        
        # Fetch venue object from database for markup pricing
        venue = self._get_venue_from_database(performance)
        
        pack_counter = 1
        for level, sections_data in enhanced_packs.items():
            for section, rows_data in sections_data.items():
                for row, packs_list in rows_data.items():
                    for pack_seats in packs_list:
                        if len(pack_seats) <= 1:  # Skip packs with size 1 or empty
                            continue
                        
                        # Get the original SeatData objects for this pack
                        pack_seat_objects = []
                        for seat_dict in pack_seats:
                            seat_data_ref = seat_dict.get('_seat_data_ref')
                            if seat_data_ref and seat_data_ref.seat_id in seat_lookup:
                                pack_seat_objects.append(seat_data_ref)
                        
                        if not pack_seat_objects:
                            continue
                        
                        # Calculate pack pricing
                        total_face_value = sum(seat.price for seat in pack_seat_objects if seat.price)
                        
                        # Apply venue markup if available
                        total_price_with_markup = total_face_value
                        if venue and hasattr(venue, 'price_markup_type') and hasattr(venue, 'price_markup_value'):
                            if venue.price_markup_type == 'percentage':
                                markup_amount = total_face_value * (venue.price_markup_value / Decimal('100'))
                                total_price_with_markup = total_face_value + markup_amount
                            elif venue.price_markup_type == 'dollar':
                                total_price_with_markup = total_face_value + venue.price_markup_value
                        
                        # Create unique pack ID using performance and counter
                        pack_id = f"{performance.source_performance_id}_pack_{pack_counter}"
                        
                        # Create SeatPackData object with correct schema
                        seat_pack = SeatPackData(
                            pack_id=pack_id,
                            zone_id=pack_seat_objects[0].zone_id,  # Use the first seat's zone
                            source_website="broadway_sf",
                            row_label=pack_seat_objects[0].row_label,
                            start_seat_number=pack_seat_objects[0].seat_number,
                            end_seat_number=pack_seat_objects[-1].seat_number,
                            pack_size=len(pack_seat_objects),
                            pack_price=total_face_value,
                            total_price=total_price_with_markup,
                            seat_ids=[seat.seat_id for seat in pack_seat_objects],
                            row=pack_seat_objects[0].row_label,
                            start_seat=pack_seat_objects[0].seat_number,
                            end_seat=pack_seat_objects[-1].seat_number,
                            performance=performance,  # Pass the performance object
                            level_id=pack_seat_objects[0].level_id
                        )
                        
                        seat_pack_objects.append(seat_pack)
                        pack_counter += 1
        
        return seat_pack_objects

    def _filter_genuinely_available_seats(self, processed_seats: List[SeatData], raw_seats: List[Dict]) -> List[SeatData]:
        """
        Enhanced filtering to prevent ghost seat packs by ensuring only genuinely available seats are processed.
        
        Args:
            processed_seats: List of processed SeatData objects
            raw_seats: List of raw seat dictionaries from API
            
        Returns:
            List of genuinely available SeatData objects
        """
        # Create mapping from raw seat data for validation
        raw_seat_lookup = {}
        for raw_seat in raw_seats:
            raw_seat_id = raw_seat.get('id', 'unknown')
            raw_seat_lookup[raw_seat_id] = raw_seat
        
        genuinely_available_seats = []
        
        for seat in processed_seats:
            # Extract raw seat ID (remove performance prefix)
            raw_seat_id = seat.seat_id.split('_')[-1] if '_' in seat.seat_id else seat.seat_id
            raw_seat_data = raw_seat_lookup.get(raw_seat_id, {})
            
            # Multi-level validation to prevent ghost packs
            is_genuinely_available = True
            
            # 1. Check basic availability flag
            if not seat.available:
                is_genuinely_available = False
            
            # 2. Validate against raw data availability
            raw_available = raw_seat_data.get('available', False)
            if not raw_available:
                is_genuinely_available = False
            
            # 3. Check if seat has valid pricing
            if not seat.price or seat.price <= 0:
                is_genuinely_available = False
            
            # 4. Validate seat position (must have valid row and seat number)
            if not seat.row_label or not seat.seat_number:
                is_genuinely_available = False
            
            # 5. Check status consistency
            if seat.status != "available":
                is_genuinely_available = False
            
            # 6. Special validation for balcony seats (common ghost pack source)
            if 'balc' in seat.section_id.lower():
                # Extra validation for balcony seats
                if not raw_seat_data.get('zone') or not raw_seat_data.get('level'):
                    is_genuinely_available = False
            
            if is_genuinely_available:
                genuinely_available_seats.append(seat)
        
        return genuinely_available_seats

    def _extract_venue_timezone(self, calendar_data: Dict[str, Any] = None) -> str:
        """
        Extract venue timezone from calendar API response with validation.
        
        Args:
            calendar_data: Calendar API response data
            
        Returns:
            Valid timezone string, defaults to "America/Los_Angeles" if not found or invalid
        """
        default_timezone = "America/Los_Angeles"
        
        if not calendar_data:
            return default_timezone
        
        try:
            # Extract timezone from calendar API response structure
            # Path: calendar_data -> dates -> timeZone
            dates_info = calendar_data.get('dates', {})
            api_timezone = dates_info.get('timeZone')
            
            if not api_timezone:
                # Try alternative path: show_info -> dates -> timeZone (for processed calendar data)
                show_info = calendar_data.get('show_info', {})
                if show_info:
                    dates_info = show_info.get('dates', {})
                    api_timezone = dates_info.get('timeZone')
            
            if not api_timezone:
                # Try raw data path: raw_data -> data -> getShow -> show -> dates -> timeZone
                raw_data = calendar_data.get('raw_data', {})
                if raw_data:
                    show_data = raw_data.get('data', {}).get('getShow', {}).get('show', {})
                    dates_info = show_data.get('dates', {})
                    api_timezone = dates_info.get('timeZone')
            
            if api_timezone and self._is_valid_timezone(api_timezone):
                import logging
                logger = logging.getLogger(__name__)
                logger.info(f"Extracted timezone from Broadway SF API: {api_timezone}")
                return api_timezone
            elif api_timezone:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Invalid timezone from API: {api_timezone}, using default: {default_timezone}")
                
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Error extracting timezone from calendar data: {e}, using default: {default_timezone}")
        
        return default_timezone
    
    def _is_valid_timezone(self, timezone_str: str) -> bool:
        """
        Validate if a timezone string is valid.
        
        Args:
            timezone_str: Timezone string to validate
            
        Returns:
            True if valid timezone, False otherwise
        """
        if not timezone_str or not isinstance(timezone_str, str):
            return False
        
        try:
            import pytz
            pytz.timezone(timezone_str)
            return True
        except (pytz.exceptions.UnknownTimeZoneError, AttributeError):
            # Fallback validation for common timezone formats
            valid_patterns = [
                'America/',
                'Europe/',
                'Asia/',
                'Africa/',
                'Australia/',
                'Pacific/',
                'UTC',
                'GMT'
            ]
            return any(timezone_str.startswith(pattern) for pattern in valid_patterns)

    def _format_level_name(self, level: str) -> str:
        """Format level name for display"""
        if not level:
            return "Unknown Level"
        
        level_lower = level.lower()
        if 'orchestra' in level_lower:
            return "Orchestra"
        elif 'mezzanine' in level_lower:
            return "Mezzanine"
        elif 'balcony' in level_lower:
            return "Balcony"
        elif 'box' in level_lower:
            return "Box Seats"
        else:
            return level.title()

    def _get_level_type(self, level_name: str) -> str:
        """Determine level type based on name"""
        level_lower = level_name.lower()
        if 'orchestra' in level_lower:
            return 'orchestra'
        elif 'mezzanine' in level_lower:
            return 'mezzanine'
        elif 'balcony' in level_lower:
            return 'balcony'
        elif 'box' in level_lower:
            return 'box'
        else:
            return 'other'