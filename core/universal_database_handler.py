import datetime  # Added for timezone.utc
import logging
from decimal import Decimal
from typing import Dict, Any, Optional, List

from django.core.exceptions import ValidationError
from django.db import transaction, IntegrityError
from django.utils import timezone

logger = logging.getLogger(__name__)

from scrapers.models import (
    Venue, Event, EventVenue, Performance, Level, Zone, Section, Seat,
    ScrapeJob, SeatSnapshot, LevelPriceSnapshot, ZonePriceSnapshot,
    SectionPriceSnapshot, PerformanceLevel
)
from .data_schemas import ScrapedData
from .id_generator import InternalIDGenerator


class UniversalDatabaseHandler:
    """Universal database handler for all scrapers"""

    def __init__(self, source_website: str, prefix: str):
        self.source_website = source_website
        self.prefix = prefix
        self.id_generator = InternalIDGenerator()

    @transaction.atomic
    def save_scraped_data(self, scraped_data: ScrapedData, scrape_job_id: Optional[str] = None,
                          enriched_data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, str]]:
        """
        Save scraped data to database with full transaction support.
        
        Args:
            scraped_data: ScrapedData object containing all scraped information
            scrape_job_id: External scrape job ID for tracking (optional)
            enriched_data: RabbitMQ message data containing POS configuration (optional)
            
        Returns:
            Dictionary containing performance_id, event_id, and venue_id if successful, None if failed
        """
        try:
            # Validate required data
            self._validate_scraped_data(scraped_data)

            # Generate internal IDs first
            venue_internal_id = self.id_generator.generate_venue_id(self.prefix, scraped_data.venue_info)
            event_internal_id = self.id_generator.generate_event_id(self.prefix, scraped_data.event_info)
            performance_internal_id = self.id_generator.generate_performance_id(
                self.prefix, scraped_data.performance_info, event_internal_id, venue_internal_id
            )

            # Create or get core entities with generated internal IDs
            venue = self._create_or_get_venue(scraped_data.venue_info, venue_internal_id)
            event = self._create_or_get_event(scraped_data.event_info, event_internal_id)
            self._create_or_get_event_venue(event, venue)
            performance = self._create_or_get_performance(scraped_data.performance_info, event, venue,
                                                          performance_internal_id)

            # Create scrape job for tracking
            scrape_job = self._create_scrape_job(performance, scraped_data, scrape_job_id)

            # Create seating structure with generated internal IDs
            levels_map = self._create_or_get_levels(performance, scraped_data.levels, performance_internal_id)
            zones_map = self._create_or_get_zones(performance, scraped_data.zones, performance_internal_id)
            sections_map = self._create_or_get_sections(performance, levels_map, scraped_data.sections,
                                                        performance_internal_id)

            seats_map = self._create_or_get_seats(sections_map, zones_map, scraped_data.seats)

            # Create snapshots and pricing data
            self._create_price_snapshots(scrape_job, levels_map, zones_map, sections_map, scraped_data)
            self._create_seat_snapshots(scrape_job, seats_map, scraped_data.seats)

            # New strategy-aware seat pack generation with comprehensive validation
            seat_packs_to_create = scraped_data.seat_packs

            # If scraper instance is available and no seat packs were pre-generated, generate them now
            if scraped_data.scraper_instance and (not seat_packs_to_create or len(seat_packs_to_create) == 0):
                try:
                    # Validate inputs before generation
                    if not scraped_data.seats:
                        seat_packs_to_create = []
                    elif not scraped_data.sections:
                        seat_packs_to_create = []
                    else:
                        # Generate seat packs using scraper strategy
                        seat_packs_to_create = scraped_data.scraper_instance.generate_seat_packs(
                            scraped_data.seats, scraped_data.sections, scraped_data.performance_info
                        )

                        # Validate generated seat packs
                        if hasattr(scraped_data.scraper_instance, '_validate_seat_pack_integrity'):
                            if not scraped_data.scraper_instance._validate_seat_pack_integrity(seat_packs_to_create):
                                seat_packs_to_create = scraped_data.seat_packs  # Fallback to original
                            else:
                                pass
                        else:
                            pass

                except Exception as e:
                    logger.warning(f"Seat pack generation failed, using fallback: {str(e)}", 
                                  extra={'scrape_job_id': scrape_job_id, 'source_website': self.source_website})
                    seat_packs_to_create = scraped_data.seat_packs  # Fallback to original

            try:
                self._create_seat_packs(scrape_job, zones_map, seat_packs_to_create, performance, event, enriched_data)
            except Exception as e:
                # Log seat pack creation error but don't fail the entire transaction
                logger.error(f"Seat pack creation failed but continuing with other data: {str(e)}", 
                           extra={
                               'scrape_job_id': scrape_job_id,
                               'source_website': self.source_website,
                               'venue_name': scraped_data.venue_info.name if scraped_data.venue_info else 'Unknown',
                               'event_name': scraped_data.event_info.name if scraped_data.event_info else 'Unknown',
                               'seat_packs_count': len(seat_packs_to_create) if seat_packs_to_create else 0
                           })
                # Continue without seat packs rather than failing entirely

            # Update current seat status
            self._update_current_seat_status(seats_map, scrape_job, scraped_data.seats)

            return {
                'performance_id': str(performance.internal_performance_id),
                'event_id': str(event.internal_event_id),
                'venue_id': str(venue.internal_venue_id)
            }

        except IntegrityError as e:
            # Log the specific integrity error details
            logger.error(f"Database integrity error in save_scraped_data: {str(e)}", 
                        extra={
                            'scrape_job_id': scrape_job_id,
                            'source_website': self.source_website,
                            'venue_name': scraped_data.venue_info.name if scraped_data.venue_info else 'Unknown',
                            'event_name': scraped_data.event_info.name if scraped_data.event_info else 'Unknown',
                            'error_type': 'IntegrityError'
                        })
            return None
        except Exception as e:
            # Log the general exception details
            logger.error(f"Unexpected error in save_scraped_data: {str(e)}", 
                        extra={
                            'scrape_job_id': scrape_job_id,
                            'source_website': self.source_website,
                            'venue_name': scraped_data.venue_info.name if scraped_data.venue_info else 'Unknown',
                            'event_name': scraped_data.event_info.name if scraped_data.event_info else 'Unknown',
                            'error_type': type(e).__name__
                        })
            return None

    def _validate_scraped_data(self, data: ScrapedData):
        """Validate scraped data has required fields"""
        if not data.venue_info or not data.venue_info.name:
            raise ValidationError("Venue name is required")
        if not data.event_info or not data.event_info.name:
            raise ValidationError("Event name is required")
        if not data.performance_info or not data.performance_info.performance_datetime_utc:
            raise ValidationError("Performance datetime is required")
        
        # Validate venue info has minimum required fields
        if not data.venue_info.source_venue_id:
            raise ValidationError("Venue source ID is required")
        if not data.venue_info.source_website:
            raise ValidationError("Venue source website is required")
            
        # Validate event info has minimum required fields
        if not data.event_info.source_event_id:
            raise ValidationError("Event source ID is required")
        if not data.event_info.source_website:
            raise ValidationError("Event source website is required")
            
        # Validate performance info has minimum required fields
        if not data.performance_info.source_website:
            raise ValidationError("Performance source website is required")

    def _create_or_get_venue(self, venue_data, internal_venue_id: str) -> Venue:
        """Create or get venue from VenueData with internal ID"""
        venue_defaults = {
            'source_venue_id': venue_data.source_venue_id,
            'source_website': venue_data.source_website,
            'internal_venue_id': internal_venue_id,
            'name': venue_data.name,
            'address': venue_data.address or f"{venue_data.city}, {venue_data.state}",
            'city': venue_data.city,
            'state': venue_data.state,
            'country': venue_data.country,
            'postal_code': venue_data.postal_code,
            'venue_timezone': venue_data.venue_timezone,
            'url': venue_data.url,
            'seat_structure': venue_data.seat_structure
        }

        venue, created = Venue.objects.get_or_create(
            source_venue_id=venue_data.source_venue_id,
            source_website=venue_data.source_website,
            defaults=venue_defaults
        )

        if created:
            pass
        else:
            pass

        return venue

    def _create_or_get_event(self, event_data, internal_event_id: str) -> Event:
        """Create or get event from EventData with internal ID"""
        event_defaults = {
            'source_event_id': event_data.source_event_id,
            'source_website': event_data.source_website,
            'internal_event_id': internal_event_id,
            'name': event_data.name,
            'url': event_data.url,
            'currency': event_data.currency,
            'event_type': event_data.event_type
        }

        event, created = Event.objects.get_or_create(
            source_event_id=event_data.source_event_id,
            source_website=event_data.source_website,
            defaults=event_defaults
        )

        if created:
            pass

        return event

    def _create_or_get_event_venue(self, event: Event, venue: Venue) -> EventVenue:
        """Create or get event-venue relationship"""
        event_venue, created = EventVenue.objects.get_or_create(
            event_id=event,
            venue_id=venue,
            defaults={
                'source_website': self.source_website,
                'is_active': True
            }
        )

        if created:
            pass

        return event_venue

    def _create_or_get_performance(self, performance_data, event: Event, venue: Venue,
                                   internal_performance_id: str) -> Performance:
        """Create or get performance from PerformanceData with internal ID and POS logic"""
        # Determine POS status - if parameter is False, fall back to venue POS setting
        pos_enabled = self._determine_pos_status(getattr(performance_data, 'pos_enabled', None), venue)

        performance_defaults = {
            'event_id': event,
            'venue_id': venue,
            'source_performance_id': performance_data.source_performance_id,
            'source_website': performance_data.source_website,
            'internal_performance_id': internal_performance_id,
            'performance_datetime_utc': performance_data.performance_datetime_utc,
            'seat_map_url': performance_data.seat_map_url,
            'map_width': performance_data.map_width,
            'map_height': performance_data.map_height,
            'pos_enabled': pos_enabled,
            'pos_enabled_at': timezone.now() if pos_enabled else None
        }

        performance, created = Performance.objects.get_or_create(
            event_id=event,
            venue_id=venue,
            performance_datetime_utc=performance_data.performance_datetime_utc,
            defaults=performance_defaults
        )

        # Update existing performance if POS status changed
        if not created:
            current_pos_status = performance.pos_enabled
            if current_pos_status != pos_enabled:
                performance.pos_enabled = pos_enabled
                if pos_enabled:
                    performance.pos_enabled_at = timezone.now()
                else:
                    performance.pos_disabled_at = timezone.now()
                performance.save(update_fields=['pos_enabled', 'pos_enabled_at', 'pos_disabled_at'])

        if created:
            pass

        return performance

    def _create_scrape_job(self, performance: Performance, data: ScrapedData,
                           external_job_id: Optional[str]) -> ScrapeJob:
        """Create scrape job for tracking"""
        # Ensure scraped_at is timezone-aware
        scraped_at = data.scraped_at
        if scraped_at.tzinfo is None:
            scraped_at = timezone.make_aware(scraped_at, datetime.timezone.utc)

        return ScrapeJob.objects.create(
            performance_id=performance,
            scraper_name=f'{self.source_website}_scraper',
            source_website=self.source_website,
            scraper_version=data.scraper_version,
            scraped_at_utc=scraped_at,
            scrape_success=data.success,
            http_status=data.http_status or 200,
            error_message=data.error_message,
            raw_payload=data.raw_data,
            scraper_config=data.scraper_config.__dict__ if data.scraper_config else {}
        )

    def _create_or_get_levels(self, performance: Performance, levels_data: List, performance_internal_id: str) -> Dict[
        str, Level]:
        """Create or get levels from LevelData list with internal ID generation"""
        levels_map = {}

        for level_data in levels_data:
            # Generate internal level ID
            internal_level_id = self.id_generator.generate_level_id(
                self.prefix, level_data, performance_internal_id
            )

            level_defaults = {
                'source_level_id': level_data.level_id,
                'source_website': level_data.source_website,
                'internal_level_id': internal_level_id,
                'venue_id': performance.venue_id,  # Add venue relationship
                'name': level_data.name,
                'alias': level_data.name,  # Set alias to name by default
                'raw_name': level_data.raw_name,
                'level_number': level_data.level_number,
                'display_order': level_data.display_order,
                'level_type': level_data.level_type
            }

            # Try to get existing level first, then create with unique internal_level_id
            try:
                level, created = Level.objects.get_or_create(
                    venue_id=performance.venue_id,
                    source_level_id=level_data.level_id,
                    source_website=level_data.source_website,
                    name=level_data.name,
                    defaults=level_defaults
                )
            except IntegrityError:
                # Handle case where internal_level_id already exists
                # Try to get existing level with same internal_level_id
                try:
                    level = Level.objects.get(internal_level_id=internal_level_id)
                    created = False
                except Level.DoesNotExist:
                    # Generate a new unique internal_level_id
                    counter = 1
                    while True:
                        try:
                            new_internal_id = f"{internal_level_id}_{counter}"
                            level_defaults['internal_level_id'] = new_internal_id
                            level, created = Level.objects.get_or_create(
                                venue_id=performance.venue_id,
                                source_level_id=level_data.level_id,
                                source_website=level_data.source_website,
                                name=level_data.name,
                                defaults=level_defaults
                            )
                            break
                        except IntegrityError:
                            counter += 1
                            if counter > 100:  # Prevent infinite loop
                                raise ValueError(f"Could not generate unique internal_level_id for {internal_level_id}")
                            continue

            # Update internal_level_id if it was created without one
            if not level.internal_level_id:
                level.internal_level_id = internal_level_id
                level.save(update_fields=['internal_level_id'])

            # Create or get the PerformanceLevel relationship
            PerformanceLevel.objects.get_or_create(
                performance=performance,
                level=level,
                defaults={'display_order': level_data.display_order}
            )

            # Ensure consistent string key format
            level_key = str(level_data.level_id)
            levels_map[level_key] = level

        return levels_map

    def _create_or_get_zones(self, performance: Performance, zones_data: List, performance_internal_id: str) -> Dict[
        str, Zone]:
        """Create or get zones from ZoneData list with internal ID generation"""
        zones_map = {}

        for zone_data in zones_data:
            # Generate internal zone ID
            internal_zone_id = self.id_generator.generate_zone_id(
                self.prefix, zone_data, performance_internal_id
            )

            zone_defaults = {
                'performance_id': performance,
                'source_zone_id': zone_data.zone_id,
                'source_website': zone_data.source_website,
                'internal_zone_id': internal_zone_id,
                'name': zone_data.name,
                'raw_identifier': zone_data.raw_identifier,
                'zone_type': zone_data.zone_type,
                'color_code': zone_data.color_code,
                'view_type': zone_data.view_type,
                'wheelchair_accessible': zone_data.wheelchair_accessible,
                'display_order': zone_data.display_order,
                'miscellaneous': zone_data.miscellaneous
            }

            try:
                zone, created = Zone.objects.get_or_create(
                    performance_id=performance,
                    source_zone_id=zone_data.zone_id,
                    source_website=zone_data.source_website,
                    defaults=zone_defaults
                )
            except IntegrityError:
                # Handle case where internal_zone_id already exists
                # Try to get existing zone with same internal_zone_id
                try:
                    zone = Zone.objects.get(internal_zone_id=internal_zone_id)
                    created = False
                except Zone.DoesNotExist:
                    # Generate a new unique internal_zone_id
                    counter = 1
                    while True:
                        try:
                            new_internal_id = f"{internal_zone_id}_{counter}"
                            zone_defaults['internal_zone_id'] = new_internal_id
                            zone, created = Zone.objects.get_or_create(
                                performance_id=performance,
                                source_zone_id=zone_data.zone_id,
                                source_website=zone_data.source_website,
                                defaults=zone_defaults
                            )
                            break
                        except IntegrityError:
                            counter += 1
                            if counter > 100:  # Prevent infinite loop
                                raise ValueError(f"Could not generate unique internal_zone_id for {internal_zone_id}")
                            continue

            # Update internal_zone_id if it was created without one
            if not zone.internal_zone_id:
                zone.internal_zone_id = internal_zone_id
                zone.save(update_fields=['internal_zone_id'])

            zones_map[zone_data.zone_id] = zone

        return zones_map

    def _create_or_get_sections(self, performance: Performance, levels_map: Dict[str, Level],
                                sections_data: List, performance_internal_id: str) -> Dict[str, Section]:
        """Create or get sections from SectionData list with internal ID generation"""
        sections_map = {}

        for section_data in sections_data:
            # Find the appropriate level - ensure string conversion for consistent lookup
            level_key = str(section_data.level_id)
            level = levels_map.get(level_key)
            if not level:
                # If no specific level found, try to match by name or use first available
                for level_obj in levels_map.values():
                    if level_obj.name.lower() in section_data.name.lower():
                        level = level_obj
                        break

                # If still no match, use first available level
                if not level and levels_map:
                    level = list(levels_map.values())[0]

            if level:
                # Generate internal section ID
                internal_section_id = self.id_generator.generate_section_id(
                    self.prefix, section_data, level.internal_level_id, performance_internal_id
                )

                section_defaults = {
                    'level_id': level,
                    'source_section_id': section_data.section_id,
                    'source_website': section_data.source_website,
                    'internal_section_id': internal_section_id,
                    'name': section_data.name,
                    'alias': section_data.name,  # Set alias to name by default
                    'raw_name': section_data.raw_name,
                    'section_type': section_data.section_type,
                    'display_order': section_data.display_order
                }

                try:
                    section, created = Section.objects.get_or_create(
                        level_id=level,
                        source_section_id=section_data.section_id,
                        source_website=section_data.source_website,
                        defaults=section_defaults
                    )
                except IntegrityError:
                    # Handle case where internal_section_id already exists
                    # Try to get existing section with same internal_section_id
                    try:
                        section = Section.objects.get(internal_section_id=internal_section_id)
                        created = False
                    except Section.DoesNotExist:
                        # Generate a new unique internal_section_id
                        counter = 1
                        while True:
                            try:
                                new_internal_id = f"{internal_section_id}_{counter}"
                                section_defaults['internal_section_id'] = new_internal_id
                                section, created = Section.objects.get_or_create(
                                    level_id=level,
                                    source_section_id=section_data.section_id,
                                    source_website=section_data.source_website,
                                    defaults=section_defaults
                                )
                                break
                            except IntegrityError:
                                counter += 1
                                if counter > 100:  # Prevent infinite loop
                                    raise ValueError(
                                        f"Could not generate unique internal_section_id for {internal_section_id}")
                                continue

                # Update internal_section_id if it was created without one
                if not section.internal_section_id:
                    section.internal_section_id = internal_section_id
                    section.save(update_fields=['internal_section_id'])

                sections_map[section_data.section_id] = section

        return sections_map

    def _create_or_get_seats(self, sections_map: Dict[str, Section], zones_map: Dict[str, Zone],
                             seats_data: List) -> Dict[str, Seat]:
        """Create or get seats from SeatData list with internal ID generation"""
        seats_map = {}
        
        logger.info(f"🪑 Starting seat creation with {len(seats_data)} seat records")
        seats_created = 0
        seats_skipped = 0
        
        for seat_data in seats_data:
            section = sections_map.get(seat_data.section_id)
            zone = zones_map.get(seat_data.zone_id)

            if section and zone:
                seats_created += 1
                # Generate internal seat ID
                internal_seat_id = self.id_generator.generate_seat_id(
                    self.prefix, seat_data, section.internal_section_id, zone.internal_zone_id
                )

                seat_defaults = {
                    'section_id': section,
                    'zone_id': zone,
                    'source_seat_id': seat_data.seat_id,
                    'source_website': seat_data.source_website,
                    'internal_seat_id': internal_seat_id,
                    'row_label': seat_data.row_label,
                    'seat_number': seat_data.seat_number,
                    'seat_type': seat_data.seat_type,
                    'x_coord': seat_data.x_coord,
                    'y_coord': seat_data.y_coord
                }

                try:
                    seat, created = Seat.objects.get_or_create(
                        section_id=section,
                        row_label=seat_data.row_label,
                        seat_number=seat_data.seat_number,
                        defaults=seat_defaults
                    )
                except IntegrityError:
                    # Handle duplicate internal_seat_id by trying to get existing seat
                    try:
                        seat = Seat.objects.get(internal_seat_id=internal_seat_id)
                        created = False
                    except Seat.DoesNotExist:
                        # If seat doesn't exist, try again with a unique internal_seat_id
                        import uuid
                        unique_suffix = str(uuid.uuid4())[:8]
                        internal_seat_id = f"{internal_seat_id}_{unique_suffix}"
                        seat_defaults['internal_seat_id'] = internal_seat_id
                        seat, created = Seat.objects.get_or_create(
                            section_id=section,
                            row_label=seat_data.row_label,
                            seat_number=seat_data.seat_number,
                            defaults=seat_defaults
                        )

                # Update internal_seat_id if it was created without one
                if not seat.internal_seat_id:
                    seat.internal_seat_id = internal_seat_id
                    seat.save(update_fields=['internal_seat_id'])

                seats_map[seat_data.seat_id] = seat
            else:
                seats_skipped += 1
                if seats_skipped <= 5:  # Log first 5 skipped seats
                    logger.warning(f"⚠️ Skipping seat: section_id='{seat_data.section_id}', zone_id='{seat_data.zone_id}'")
                    logger.warning(f"   - Section found: {section is not None}")
                    logger.warning(f"   - Zone found: {zone is not None}")
                    if seats_skipped == 1:
                        logger.info(f"   - Available sections: {list(sections_map.keys())[:10]}")
                        logger.info(f"   - Available zones: {list(zones_map.keys())[:10]}")

        logger.info(f"🎯 Seat creation summary:")
        logger.info(f"   - Seats processed: {len(seats_data)}")
        logger.info(f"   - Seats created: {seats_created}")
        logger.info(f"   - Seats skipped: {seats_skipped}")
        
        if seats_skipped > 0:
            logger.warning(f"⚠️ {seats_skipped} seats were skipped due to missing section/zone mappings")

        return seats_map

    def _create_price_snapshots(self, scrape_job: ScrapeJob, levels_map: Dict[str, Level],
                                zones_map: Dict[str, Zone], sections_map: Dict[str, Section],
                                data: ScrapedData):
        """Create price snapshots for levels, zones, and sections with venue markup applied"""

        # Get venue for markup calculation
        venue = None
        if zones_map:
            first_zone = next(iter(zones_map.values()), None)
            if first_zone and hasattr(first_zone, 'performance_id'):
                venue = first_zone.performance_id.venue_id

        # Level price snapshots
        for level_data in data.levels:
            level = levels_map.get(level_data.level_id)
            if level:
                # Calculate aggregated pricing for this level with markup applied
                level_seats = [s for s in data.seats if s.level_id == level_data.level_id]
                available_count = sum(1 for s in level_seats if s.available)
                prices = []
                for s in level_seats:
                    if s.price and s.price > 0:
                        marked_up_price = self._apply_venue_markup(s.price, venue) if venue else s.price
                        prices.append(marked_up_price)

                min_price = min(prices) if prices else None
                max_price = max(prices) if prices else None

                LevelPriceSnapshot.objects.create(
                    scrape_job_key=scrape_job,
                    level_id=level,
                    min_price=min_price,
                    max_price=max_price,
                    available_seats=available_count,
                    raw_price_text=f"Level {level_data.name}: ${min_price if min_price else 0}-${max_price if max_price else 0}",
                    raw_availability_text=f"{available_count} seats available"
                )

        # Zone price snapshots
        for zone_data in data.zones:
            zone = zones_map.get(zone_data.zone_id)
            if zone:
                zone_seats = [s for s in data.seats if s.zone_id == zone_data.zone_id]
                available_count = sum(1 for s in zone_seats if s.available)
                prices = []
                for s in zone_seats:
                    if s.price and s.price > 0:
                        marked_up_price = self._apply_venue_markup(s.price, venue) if venue else s.price
                        prices.append(marked_up_price)

                # Apply markup to zone data min/max prices as fallback
                fallback_min = self._apply_venue_markup(zone_data.min_price,
                                                        venue) if venue and zone_data.min_price else zone_data.min_price
                fallback_max = self._apply_venue_markup(zone_data.max_price,
                                                        venue) if venue and zone_data.max_price else zone_data.max_price

                ZonePriceSnapshot.objects.create(
                    scrape_job_key=scrape_job,
                    zone_id=zone,
                    min_price=min(prices) if prices else fallback_min,
                    max_price=max(prices) if prices else fallback_max,
                    available_seats=available_count
                )

        # Section price snapshots
        for section_data in data.sections:
            section = sections_map.get(section_data.section_id)
            if section:
                section_seats = [s for s in data.seats if s.section_id == section_data.section_id]
                available_count = sum(1 for s in section_seats if s.available)
                prices = []
                for s in section_seats:
                    if s.price and s.price > 0:
                        marked_up_price = self._apply_venue_markup(s.price, venue) if venue else s.price
                        prices.append(marked_up_price)

                SectionPriceSnapshot.objects.create(
                    scrape_job_key=scrape_job,
                    section_id=section,
                    min_price=min(prices) if prices else None,
                    max_price=max(prices) if prices else None,
                    available_seats=available_count
                )

    def _create_seat_snapshots(self, scrape_job: ScrapeJob, seats_map: Dict[str, Seat], seats_data: List):
        """Create individual seat snapshots with venue markup applied"""
        # Get venue for markup calculation
        venue = None
        if seats_data and seats_map:
            first_seat_data = seats_data[0]
            first_seat = seats_map.get(first_seat_data.seat_id)
            if first_seat and hasattr(first_seat, 'zone_id') and hasattr(first_seat.zone_id, 'performance_id'):
                venue = first_seat.zone_id.performance_id.venue_id

        for seat_data in seats_data:
            seat = seats_map.get(seat_data.seat_id)
            if seat:
                # Apply venue markup to price before saving snapshot
                marked_up_price = seat_data.price
                if marked_up_price and venue:
                    marked_up_price = self._apply_venue_markup(marked_up_price, venue)

                SeatSnapshot.objects.create(
                    scrape_job_key=scrape_job,
                    seat_id=seat,
                    status=seat_data.status,
                    price=marked_up_price,
                    fees=seat_data.fees,
                    raw_status_text=seat_data.status,
                    raw_price_text=str(marked_up_price) if marked_up_price else None,
                    raw_fees_text=str(seat_data.fees) if seat_data.fees else None
                )

    def _create_seat_packs(self, scrape_job: ScrapeJob, zones_map: Dict[str, Zone], seat_packs_data: List,
                           performance: Performance, event: Event, enriched_data: Optional[Dict[str, Any]] = None):
        """
        Create/update seat packs using Phase 2 synchronization algorithm.
        
        This method uses the intelligent diffing algorithm to compare existing
        packs with newly generated ones, producing a minimal set of database operations.
        
        Focus: Database preparation only - no POS synchronization.
        External POS sync service will handle synchronization based on pack states.
        """

        if not seat_packs_data:
            return None

        # Get performance ID for context
        first_zone = next(iter(zones_map.values()), None)
        if not first_zone:
            return None

        performance_id = performance.internal_performance_id

        try:
            # Import sync modules (avoid circular imports)
            from .seat_pack_sync import get_active_seat_packs_for_performance
            from .sync_plan_executor import execute_seat_pack_synchronization

            # Get existing active seat packs for this performance
            existing_packs = get_active_seat_packs_for_performance(performance_id)

            # Apply venue markup to seat pack prices
            venue = performance.venue_id
            if venue and hasattr(venue, 'price_markup_value') and venue.price_markup_value:
                for pack_data in seat_packs_data:
                    if hasattr(pack_data, 'pack_price') and pack_data.pack_price:
                        # Apply per-seat markup multiplied by pack size
                        original_pack_price = pack_data.pack_price
                        pack_size = getattr(pack_data, 'pack_size', 1)

                        # Calculate per-seat price, apply markup per seat, then multiply by pack size
                        per_seat_price = original_pack_price / pack_size
                        marked_up_per_seat_price = self._apply_venue_markup(per_seat_price, venue)
                        marked_up_pack_price = marked_up_per_seat_price * pack_size

                        pack_data.pack_price = marked_up_pack_price

                        # Also update total_price if it wasn't already marked up
                        if hasattr(pack_data, 'total_price') and pack_data.total_price == original_pack_price:
                            pack_data.total_price = marked_up_pack_price

            # Execute synchronization workflow (database operations only)
            sync_results = execute_seat_pack_synchronization(
                existing_packs=existing_packs,
                new_packs=seat_packs_data,
                source_website=self.source_website,
                scrape_job=scrape_job,
                zones_map=zones_map,
                performance=performance,
                event=event
            )
            return sync_results

        except ImportError as e:
            # Critical: Do not use legacy method to prevent duplicates
            raise Exception(
                f"Seat pack sync modules unavailable: {str(e)}. Cannot proceed with seat pack creation to prevent duplicates.")
        except Exception as e:
            # Re-raise the exception instead of falling back to legacy method
            # The legacy method is causing constraint violations and duplicates
            raise

    def _apply_venue_markup(self, original_price: Decimal, venue: Venue) -> Decimal:
        """Apply venue markup to seat price based on venue configuration"""
        if not original_price or not venue.price_markup_value:
            return original_price

        if venue.price_markup_type == 'percentage':
            markup_amount = original_price * (venue.price_markup_value / 100)
        else:  # dollar
            markup_amount = venue.price_markup_value

        return original_price + markup_amount

    def _determine_pos_status(self, performance_pos_param: Optional[bool], venue: Venue) -> bool:
        """Determine final POS status based on parameter and venue fallback"""
        try:
            if performance_pos_param is False:
                # Use venue setting as fallback when performance explicitly disables POS
                try:
                    if hasattr(venue, 'pos_enabled'):
                        venue_pos_enabled = venue.pos_enabled
                        if venue_pos_enabled is not None and isinstance(venue_pos_enabled, bool):
                            return venue_pos_enabled
                    return False
                except Exception:
                    return False
            # Return performance setting if explicitly set, otherwise default to False
            return performance_pos_param if performance_pos_param is not None else False
        except Exception as e:
            logger.warning(f"Error determining POS status, defaulting to False: {str(e)}")
            return False

    def _update_current_seat_status(self, seats_map: Dict[str, Seat], scrape_job: ScrapeJob, seats_data: List):
        """Update current status of seats from latest scrape with venue markup applied"""
        # Get venue for markup calculation - use first seat's venue
        venue = None
        if seats_data and seats_map:
            first_seat_data = seats_data[0]
            first_seat = seats_map.get(first_seat_data.seat_id)
            if first_seat and hasattr(first_seat, 'zone_id') and hasattr(first_seat.zone_id, 'performance_id'):
                venue = first_seat.zone_id.performance_id.venue_id

        for seat_data in seats_data:
            seat = seats_map.get(seat_data.seat_id)
            if seat:
                # Apply venue markup to price before saving
                marked_up_price = seat_data.price
                if marked_up_price and venue:
                    marked_up_price = self._apply_venue_markup(marked_up_price, venue)

                seat.current_status = seat_data.status
                seat.current_price = marked_up_price
                seat.current_fees = seat_data.fees
                seat.last_updated = timezone.now()
                seat.last_scrape_job = scrape_job
                seat.save(update_fields=[
                    'current_status', 'current_price', 'current_fees',
                    'last_updated', 'last_scrape_job'
                ])
