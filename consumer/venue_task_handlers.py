# consumer/venue_task_handlers.py
import logging
import traceback # Import traceback module
from typing import Optional, Dict, Any, List, Union

from django.core import serializers
from django.utils import timezone
from django.db.models import Q, QuerySet
from django.core.paginator import Paginator

from scrapers.models import (
    Performance, Level, Zone, Section, Seat,
    ScrapeJob, SeatSnapshot, LevelPriceSnapshot, ZonePriceSnapshot,
    SectionPriceSnapshot, SeatPack, PerformanceLevel, POSListing
)
from django.db import models

logger = logging.getLogger(__name__)

# Task handlers registry
VENUE_TASK_HANDLERS = {}


def create_paginated_response(
    data: Union[List[Dict[str, Any]], Dict[str, Any]],
    pagination_info: Dict[str, Any],
    filters_applied: Optional[Dict[str, Any]] = None,
    sort_applied: Optional[str] = None
) -> Dict[str, Any]:
    """Create standardized paginated success response"""
    response = {
        'success': True,
        'data': data,
        'pagination': pagination_info
    }
    if filters_applied:
        response['filters_applied'] = filters_applied
    if sort_applied:
        response['sort_applied'] = sort_applied
    return response


def create_error_response(
    message: str,
    error_code: Optional[str] = None
) -> Dict[str, Any]:
    """Create standardized error response following CLAUDE.md format"""
    response = {
        'success': False,
        'message': message
    }
    
    if error_code:
        response['error_code'] = error_code
    
    return response


def paginate_queryset_with_filters(
    queryset: QuerySet,
    page: int = 1,
    limit: int = 50,
    filters: Optional[Dict[str, Any]] = None,
    search: Optional[str] = None,
    sort: Optional[str] = None,
    max_limit: int = 100
) -> tuple[QuerySet, Dict[str, Any], Dict[str, Any], Optional[str]]:
    """Handle pagination, filtering, searching, and sorting
    
    Returns:
        tuple: (paginated_queryset, pagination_info, filters_applied, sort_applied)
    """
    # Validate and apply limits
    page = max(1, page)
    limit = min(max(1, limit), max_limit)
    
    # Apply filters
    filters_applied = {}
    if filters:
        for filter_key, filter_value in filters.items():
            if filter_value is not None and filter_value != '':
                filters_applied[filter_key] = filter_value
                
                # Apply specific filters based on the key
                if filter_key == 'venue_id':
                    queryset = queryset.filter(
                        venues__internal_venue_id=filter_value
                    )
                elif filter_key == 'currency':
                    queryset = queryset.filter(currency__iexact=filter_value)
                elif filter_key == 'event_type':
                    queryset = queryset.filter(event_type__iexact=filter_value)
                elif filter_key == 'date_from':
                    queryset = queryset.filter(created_at__gte=filter_value)
                elif filter_key == 'date_to':
                    queryset = queryset.filter(created_at__lte=filter_value)
                elif filter_key == 'city':
                    queryset = queryset.filter(city__icontains=filter_value)
                elif filter_key == 'state':
                    queryset = queryset.filter(state__icontains=filter_value)
                elif filter_key == 'country':
                    queryset = queryset.filter(country__icontains=filter_value)
    
    # Apply search (different for venues vs events)
    if search and search.strip():
        if hasattr(queryset.model, '_meta') and queryset.model._meta.model_name == 'venue':
            # Venue search
            search_query = Q(name__icontains=search.strip())
            search_query |= Q(city__icontains=search.strip())
            search_query |= Q(state__icontains=search.strip())
            queryset = queryset.filter(search_query).distinct()
        else:
            # Event search (existing logic)
            search_query = Q(name__icontains=search.strip())
            search_query |= Q(source_event_id__icontains=search.strip())
            search_query |= Q(venues__name__icontains=search.strip())
            queryset = queryset.filter(search_query).distinct()
    
    # Apply sorting
    sort_applied = None
    if sort and sort.strip():
        sort_fields = []
        for field in sort.split(','):
            field = field.strip()
            if field.startswith('-'):
                field_name = field[1:]
                if field_name in ['name', 'created_at', 'updated_at']:
                    sort_fields.append(field)
                elif field_name == 'venue_name':
                    sort_fields.append('-venues__name')
            else:
                if field in ['name', 'created_at', 'updated_at']:
                    sort_fields.append(field)
                elif field == 'venue_name':
                    sort_fields.append('venues__name')
                elif field == 'total_events' and hasattr(queryset.model, '_meta') and queryset.model._meta.model_name == 'venue':
                    sort_fields.append('total_events')
                elif field == 'city' and hasattr(queryset.model, '_meta') and queryset.model._meta.model_name == 'venue':
                    sort_fields.append('city')
        
        if sort_fields:
            queryset = queryset.order_by(*sort_fields)
            sort_applied = ','.join(sort_fields)
    
    # If no sorting applied, default to creation date
    if not sort_applied:
        queryset = queryset.order_by('-created_at')
        sort_applied = '-created_at'
    
    # Apply pagination
    paginator = Paginator(queryset, limit)
    
    try:
        page_obj = paginator.page(page)
    except Exception:
        # Return empty result for invalid page
        return queryset.none(), {
            'page': page,
            'limit': limit,
            'total_pages': 0,
            'total_count': 0,
            'has_next': False,
            'has_previous': False
        }, filters_applied, sort_applied
    
    pagination_info = {
        'page': page,
        'limit': limit,
        'total_pages': paginator.num_pages,
        'total_count': paginator.count,
        'has_next': page_obj.has_next(),
        'has_previous': page_obj.has_previous()
    }
    
    return page_obj, pagination_info, filters_applied, sort_applied


def register_venue_task(task_type):
    """Decorator to register venue task handlers"""

    def decorator(func):
        VENUE_TASK_HANDLERS[task_type] = func
        return func

    return decorator


@register_venue_task('get_all_events')
def handle_get_all_events(data):
    """Get all events across all venues with enhanced filtering, search, and sorting"""
    try:
        from scrapers.models import Event, Performance
        
        # Extract parameters
        page = data.get('page', 1)
        limit = data.get('limit', 50)
        
        # Extract filters
        filters = {
            'venue_id': data.get('venue_id'),
            'currency': data.get('currency'),
            'event_type': data.get('event_type'),
            'date_from': data.get('date_from'),
            'date_to': data.get('date_to')
        }
        
        # Extract search and sort
        search = data.get('search')
        sort = data.get('sort')
        
        # Validate page and limit
        try:
            page = int(page) if page else 1
            limit = int(limit) if limit else 50
        except (ValueError, TypeError):
            return create_error_response(
                'Invalid page or limit parameter',
                'INVALID_PAGINATION_PARAMS'
            )
        
        if page < 1:
            return create_error_response(
                'Page number must be greater than 0',
                'INVALID_PAGE_NUMBER'
            )
        
        # Build base query with optimizations
        events_query = Event.objects.filter(is_active=True).prefetch_related('venues').annotate(
            performance_count=models.Count('performances', filter=models.Q(performances__is_active=True))
        )
        
        # Apply pagination with filters, search, and sort
        events_page, pagination_info, filters_applied, sort_applied = paginate_queryset_with_filters(
            events_query, page, limit, filters, search, sort
        )
        
        # Check if page is invalid
        if pagination_info['total_count'] == 0 and page > 1:
            return create_error_response(
                f'Page {page} does not exist',
                'INVALID_PAGE_NUMBER'
            )
        
        # Build events data
        events_data = []
        for event in events_page:
            # Build venues array with full information
            venues_data = []
            for venue in event.venues.all():
                venues_data.append({
                    'id': venue.internal_venue_id,
                    'venue_key': getattr(venue, 'venue_key', None),
                    'name': venue.name,
                    'location': f"{venue.city}, {venue.state}" if venue.city and venue.state else 'Location TBD',
                    'city': venue.city,
                    'state': venue.state,
                    'country': venue.country,
                    'seat_structure': venue.seat_structure
                })
            
            events_data.append({
                'id': event.internal_event_id,
                'event_key': getattr(event, 'event_key', None),
                'title': event.name,  # Frontend expects 'title' as primary field
                'name': event.name,   # Keep for backward compatibility
                'source_event_id': event.source_event_id,
                'url': event.url,
                'currency': event.currency,
                'event_type': getattr(event, 'event_type', None),
                'venues': venues_data,
                'performance_count': getattr(event, 'performance_count', 0),
                'created_at': event.created_at.isoformat(),
                'updated_at': event.updated_at.isoformat() if event.updated_at else None,
            })

        return create_paginated_response(
            events_data,
            pagination_info,
            filters_applied,
            sort_applied
        )

    except Exception as e:
        logger.error(f"Error getting all events: {e}")
        return create_error_response(
            f'Failed to retrieve events: {str(e)}',
            'EVENTS_FETCH_ERROR'
        )

@register_venue_task('get_venues')
def handle_get_venues(data):
    """Get all venues with enhanced filtering, search, and sorting"""
    try:
        from scrapers.models import Venue, Event
        from django.db.models import Count
        
        # Extract parameters
        page = data.get('page', 1)
        limit = data.get('limit', 50)
        
        # Extract filters
        filters = {
            'city': data.get('city'),
            'state': data.get('state'),
            'country': data.get('country')
        }
        
        # Extract search and sort
        search = data.get('search')
        sort = data.get('sort')
        
        # Validate page and limit
        try:
            page = int(page) if page else 1
            limit = int(limit) if limit else 50
        except (ValueError, TypeError):
            return create_error_response(
                'Invalid page or limit parameter',
                'INVALID_PAGINATION_PARAMS'
            )
        
        if page < 1:
            return create_error_response(
                'Page number must be greater than 0',
                'INVALID_PAGE_NUMBER'
            )
        
        # Build base query with optimizations
        venues_query = Venue.objects.filter(is_active=True).annotate(
            total_events=Count('events', filter=models.Q(events__is_active=True))
        )
        
        # Apply pagination with filters, search, and sort
        venues_page, pagination_info, filters_applied, sort_applied = paginate_queryset_with_filters(
            venues_query, page, limit, filters, search, sort
        )
        
        # Check if page is invalid
        if pagination_info['total_count'] == 0 and page > 1:
            return create_error_response(
                f'Page {page} does not exist',
                'INVALID_PAGE_NUMBER'
            )
        
        # Build venues data
        venues_data = []
        for venue in venues_page:
            venues_data.append({
                'id': venue.internal_venue_id,
                'venue_key': getattr(venue, 'venue_key', None),
                'name': venue.name,
                'location': f"{venue.city}, {venue.state}" if venue.city and venue.state else 'Location TBD',
                'address': venue.address,
                'city': venue.city,
                'state': venue.state,
                'country': venue.country,
                'postal_code': venue.postal_code,
                'venue_timezone': venue.venue_timezone,
                'source_venue_id': venue.source_venue_id,
                'source_website': venue.source_website,
                'seat_structure': venue.seat_structure,
                'total_events': getattr(venue, 'total_events', 0),
                'created_at': venue.created_at.isoformat(),
                'updated_at': venue.updated_at.isoformat() if venue.updated_at else None,
            })

        return create_paginated_response(
            venues_data,
            pagination_info,
            filters_applied,
            sort_applied
        )

    except Exception as e:
        logger.error(f"Error getting venues: {e}")
        return create_error_response(
            f'Failed to retrieve venues: {str(e)}',
            'VENUES_FETCH_ERROR'
        )

@register_venue_task('get_venue_events')
def handle_get_venue_events(data):
    """Get all events for a specific venue - reuses get_all_events logic"""
    try:
        # Get venue_id from the data (passed from URL parameter)
        venue_id = data.get('venue_id')
        if not venue_id:
            return create_error_response(
                'venue_id is required',
                'VENUE_ID_REQUIRED'
            )

        # Add venue_id to the filter and call existing get_all_events handler
        data_with_venue_filter = data.copy()
        data_with_venue_filter['venue_id'] = venue_id
        
        return handle_get_all_events(data_with_venue_filter)

    except Exception as e:
        logger.error(f"Error getting venue events: {e}")
        return create_error_response(
            f'Failed to retrieve venue events: {str(e)}',
            'VENUE_EVENTS_FETCH_ERROR'
        )

@register_venue_task('get_venue_details')
def handle_get_venue_details(data):
    """Get detailed information about a specific venue"""
    try:
        from scrapers.models import Venue, Event, Performance
        from django.db.models import Count
        
        venue_id = data.get('venue_id')
        if not venue_id:
            return create_error_response(
                'venue_id is required',
                'VENUE_ID_REQUIRED'
            )

        # Try to find venue by internal_venue_id first, then by venue_key
        try:
            venue = Venue.objects.get(internal_venue_id=venue_id, is_active=True)
        except Venue.DoesNotExist:
            try:
                venue = Venue.objects.get(venue_key=venue_id, is_active=True)
            except Venue.DoesNotExist:
                return create_error_response(
                    f'Venue with ID {venue_id} not found',
                    'VENUE_NOT_FOUND'
                )

        # Get event and performance counts efficiently
        total_events = Event.objects.filter(venues=venue, is_active=True).count()
        total_performances = Performance.objects.filter(
            event_id__venues=venue, 
            is_active=True
        ).count()

        # Get levels for this venue with aliases and their sections with aliases
        levels_data = []
        levels = Level.objects.filter(
            venue_id=venue,
            is_active=True
        ).prefetch_related(
            'sections'
        ).order_by('display_order', 'name')
        
        for level in levels:
            # Get sections for this level with aliases
            sections_data = []
            sections = level.sections.filter(is_active=True).order_by('display_order', 'name')
            
            for section in sections:
                sections_data.append({
                    'id': section.internal_section_id,
                    'name': section.name,
                    'alias': section.alias,
                    'section_type': section.section_type,
                    'display_order': section.display_order
                })
            
            levels_data.append({
                'id': level.internal_level_id,
                'name': level.name,
                'alias': level.alias,
                'level_type': level.level_type,
                'level_number': level.level_number,
                'display_order': level.display_order,
                'sections': sections_data
            })

        venue_data = {
            'id': venue.internal_venue_id,
            'venue_key': getattr(venue, 'venue_key', None),
            'name': venue.name,
            'location': f"{venue.city}, {venue.state}" if venue.city and venue.state else 'Location TBD',
            'address': venue.address,
            'city': venue.city,
            'state': venue.state,
            'country': venue.country,
            'postal_code': venue.postal_code,
            'venue_timezone': venue.venue_timezone,
            'source_venue_id': venue.source_venue_id,
            'source_website': venue.source_website,
            'total_events': total_events,
            'total_performances': total_performances,
            'levels': levels_data,
            'created_at': venue.created_at.isoformat(),
            'updated_at': venue.updated_at.isoformat() if venue.updated_at else None,
            'seat_structure': venue.seat_structure,
            'previous_seat_structure': venue.previous_seat_structure,
        }

        return {
            'success': True,
            'data': venue_data
        }

    except Exception as e:
        logger.error(f"Error getting venue details: {e}")
        return create_error_response(
            f'Failed to retrieve venue details: {str(e)}',
            'VENUE_DETAILS_FETCH_ERROR'
        )

@register_venue_task('get_event_details')
def handle_get_event_details(data):
    """Get detailed information about a specific event"""
    try:
        from scrapers.models import Event, Performance
        
        event_id = data.get('event_id')
        if not event_id:
            return create_error_response(
                'event_id is required',
                'EVENT_ID_REQUIRED'
            )

        # Try to find event by internal_event_id first, then by event_key
        try:
            event = Event.objects.prefetch_related('venues').get(internal_event_id=event_id, is_active=True)
        except Event.DoesNotExist:
            try:
                event = Event.objects.prefetch_related('venues').get(event_key=event_id, is_active=True)
            except Event.DoesNotExist:
                return create_error_response(
                    f'Event with ID {event_id} not found',
                    'EVENT_NOT_FOUND'
                )

        # Get performance count
        performance_count = Performance.objects.filter(event_id=event, is_active=True).count()

        # Build venues array with full information (consistent with get_all_events)
        venues_data = []
        for venue in event.venues.all():
            venues_data.append({
                'id': venue.internal_venue_id,
                'venue_key': getattr(venue, 'venue_key', None),
                'name': venue.name,
                'location': f"{venue.city}, {venue.state}" if venue.city and venue.state else 'Location TBD',
                'address': venue.address,
                'city': venue.city,
                'state': venue.state,
                'country': venue.country,
                'postal_code': venue.postal_code,
                'venue_timezone': venue.venue_timezone,
                'source_venue_id': venue.source_venue_id,
                'source_website': venue.source_website,
                'seat_structure': venue.seat_structure,
                'created_at': venue.created_at.isoformat(),
                'updated_at': venue.updated_at.isoformat() if venue.updated_at else None,
            })

        event_data = {
            'id': event.internal_event_id,
            'event_key': getattr(event, 'event_key', None),
            'title': event.name,  # Frontend expects 'title' as primary field
            'name': event.name,   # Keep for backward compatibility
            'source_event_id': event.source_event_id,
            'source_website': event.source_website,
            'url': event.url,
            'currency': event.currency,
            'event_type': getattr(event, 'event_type', None),
            'venues': venues_data,
            'performance_count': performance_count,
            'created_at': event.created_at.isoformat(),
            'updated_at': event.updated_at.isoformat() if event.updated_at else None,
        }

        return {
            'success': True,
            'data': event_data
        }

    except Exception as e:
        logger.error(f"Error getting event details: {e}")
        return create_error_response(
            f'Failed to retrieve event details: {str(e)}',
            'EVENT_DETAILS_FETCH_ERROR'
        )

@register_venue_task('get_event_performances')
def handle_get_event_performances(data):
    """Get all performances for a specific event, optionally filtered by venue"""
    try:
        from scrapers.models import Event, Venue
        
        event_id = data.get('event_id')
        venue_id = data.get('venue_id')  # Optional venue filter
        
        if not event_id:
            return create_error_response(
                'event_id is required',
                'EVENT_ID_REQUIRED'
            )

        # Try to find event by internal_event_id first, then by event_key
        try:
            event = Event.objects.get(internal_event_id=event_id, is_active=True)
        except Event.DoesNotExist:
            try:
                event = Event.objects.get(event_key=event_id, is_active=True)
            except Event.DoesNotExist:
                return create_error_response(
                    f'Event with ID {event_id} not found',
                    'EVENT_NOT_FOUND'
                )

        # Build base query for performances
        performances_query = Performance.objects.filter(
            event_id=event,
            is_active=True
        ).select_related('event_id', 'venue_id').order_by('performance_datetime_utc')
        
        # Apply venue filter if provided
        venue = None
        if venue_id:
            try:
                venue = Venue.objects.get(internal_venue_id=venue_id, is_active=True)
                performances_query = performances_query.filter(venue_id=venue)
            except Venue.DoesNotExist:
                try:
                    venue = Venue.objects.get(venue_key=venue_id, is_active=True)
                    performances_query = performances_query.filter(venue_id=venue)
                except Venue.DoesNotExist:
                    return create_error_response(
                        f'Venue with ID {venue_id} not found',
                        'VENUE_NOT_FOUND'
                    )
        
        # Check if any performances exist
        if not performances_query.exists():
            error_msg = f'No performances found for event ID {event_id}'
            if venue_id:
                error_msg += f' and venue ID {venue_id}'
            return create_error_response(
                error_msg,
                'NO_PERFORMANCES_FOUND'
            )

        # Get all performances (no pagination)
        performances_data = []
        for performance in performances_query:
            # Get counts for this performance
            level_count = Level.objects.filter(performancelevel__performance=performance, is_active=True).count()
            zone_count = Zone.objects.filter(performance_id=performance, is_active=True).count()
            section_count = Section.objects.filter(
                level_id__performancelevel__performance=performance,
                is_active=True
            ).count()
            seat_count = Seat.objects.filter(
                section_id__level_id__performancelevel__performance=performance,
                is_active=True
            ).count()

            performances_data.append({
                'id': performance.internal_performance_id,
                'source_performance_id': getattr(performance, 'source_performance_id', None),
                'event_id': event.internal_event_id,
                'venue_id': performance.venue_id.internal_venue_id,
                'datetime': performance.performance_datetime_utc.isoformat(),
                'seat_map_url': getattr(performance, 'seat_map_url', None),
                'map_width': getattr(performance, 'map_width', None),
                'map_height': getattr(performance, 'map_height', None),
                'level_count': level_count,
                'zone_count': zone_count,
                'section_count': section_count,
                'seat_count': seat_count,
                'created_at': performance.created_at.isoformat(),
                'timezone': venue.venue_timezone,
                'updated_at': performance.updated_at.isoformat() if performance.updated_at else None,
            })

        response_data = {
            'event_id': event.internal_event_id,
            'event_name': event.name,
            'data': performances_data,
            'total_count': len(performances_data)
        }
        
        # Add venue info if filtering by venue
        if venue:
            response_data['venue_id'] = venue.internal_venue_id
            response_data['venue_name'] = venue.name
        print(data)
        return {
            'success': True,
            'data': response_data
        }

    except Exception as e:
        logger.error(f"Error getting event performances: {e}")
        return create_error_response(
            f'Failed to retrieve event performances: {str(e)}',
            'EVENT_PERFORMANCES_FETCH_ERROR'
        )


@register_venue_task('get_performance_details')
def handle_get_performance_details(data):
    """Get complete performance details from database with latest scrape data"""
    try:
        performance_id = data.get('performance_id')
        if not performance_id:
            return {
                'success': False,
                'error': 'performance_id is required',
                'performance': None
            }

        # Try to find performance by internal_performance_id first, then by source_performance_id
        try:
            performance = Performance.objects.select_related(
                'event_id', 'venue_id'
            ).get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            try:
                performance = Performance.objects.select_related(
                    'event_id', 'venue_id'
                ).get(source_performance_id=performance_id, is_active=True)
            except Performance.DoesNotExist:
                return {
                    'success': False,
                    'error': f'Performance with ID {performance_id} not found',
                    'performance': None
                }

        # Get latest successful scrape
        latest_scrape = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        ).order_by('-scraped_at_utc').first()

        # Get basic performance data
        performance_data = {
            'id': performance.internal_performance_id,
            # 'source_performance_id': performance.source_performance_id,
            'event_id': performance.event_id.internal_event_id,
            'venue_id': performance.venue_id.internal_venue_id,
            'datetime': performance.performance_datetime_utc.isoformat(),
            'seat_map_url': getattr(performance, 'seat_map_url', None),
            'map_width': getattr(performance, 'map_width', None),
            'map_height': getattr(performance, 'map_height', None),
            'created_at': performance.created_at.isoformat(),
            'updated_at': performance.updated_at.isoformat() if performance.updated_at else None,
        }

        # Add scrape information if available
        if latest_scrape:
            performance_data['latest_scrape'] = {
                'scrape_job_id': latest_scrape.scrape_job_key,
                'scraped_at': latest_scrape.scraped_at_utc.isoformat(),
                'scraper_name': latest_scrape.scraper_name,
                'source_website': latest_scrape.source_website
            }

        return {
            'success': True,
            'performance': performance_data
        }

    except Exception as e:
        logger.error(f"Error getting performance details: {e}")
        return {
            'success': False,
            'error': str(e),
            'performance': None
        }

def create_seat_filters_queryset(queryset, filters, search, sort, is_seat_pack=False):
    """Apply filters, search, and sort for seat/seat pack data"""
    filters_applied = {}
    
    # Apply filters
    if filters:
        for filter_key, filter_value in filters.items():
            if filter_value is not None and filter_value != '':
                filters_applied[filter_key] = filter_value
                
                if is_seat_pack:
                    # Seat pack specific filters
                    if filter_key == 'zone_id':
                        queryset = queryset.filter(
                            Q(zone_id__internal_zone_id=filter_value) |
                            Q(zone_id__zone_key=filter_value)
                        )
                    elif filter_key == 'level_id':
                        queryset = queryset.filter(
                            Q(level__internal_level_id=filter_value) |
                            Q(level__level_key=filter_value)
                        )
                    elif filter_key == 'row_label':
                        queryset = queryset.filter(row_label__icontains=filter_value)
                    elif filter_key == 'pack_size_min':
                        queryset = queryset.filter(pack_size__gte=filter_value)
                    elif filter_key == 'pack_size_max':
                        queryset = queryset.filter(pack_size__lte=filter_value)
                    elif filter_key == 'price_min':
                        queryset = queryset.filter(pack_price__gte=filter_value)
                    elif filter_key == 'price_max':
                        queryset = queryset.filter(pack_price__lte=filter_value)
                else:
                    # Seat specific filters
                    if filter_key == 'zone_id':
                        queryset = queryset.filter(
                            Q(seat_id__zone_id__internal_zone_id=filter_value) |
                            Q(seat_id__zone_id__zone_key=filter_value)
                        )
                    elif filter_key == 'section_id':
                        queryset = queryset.filter(
                            Q(seat_id__section_id__internal_section_id=filter_value) |
                            Q(seat_id__section_id__section_key=filter_value)
                        )
                    elif filter_key == 'level_id':
                        queryset = queryset.filter(
                            Q(seat_id__section_id__level_id__internal_level_id=filter_value) |
                            Q(seat_id__section_id__level_id__level_key=filter_value)
                        )
                    elif filter_key == 'row_label':
                        queryset = queryset.filter(seat_id__row_label__icontains=filter_value)
                    elif filter_key == 'seat_type':
                        queryset = queryset.filter(seat_id__seat_type__icontains=filter_value)
                    elif filter_key == 'status':
                        queryset = queryset.filter(status__icontains=filter_value)
                    elif filter_key == 'price_min':
                        queryset = queryset.filter(price__gte=filter_value)
                    elif filter_key == 'price_max':
                        queryset = queryset.filter(price__lte=filter_value)
    
    # Apply search
    if search and search.strip():
        if is_seat_pack:
            search_query = Q(row_label__icontains=search.strip())
            search_query |= Q(zone_id__name__icontains=search.strip())
            search_query |= Q(source_pack_id__icontains=search.strip())
        else:
            search_query = Q(seat_id__row_label__icontains=search.strip())
            search_query |= Q(seat_id__seat_number__icontains=search.strip())
            search_query |= Q(seat_id__zone_id__name__icontains=search.strip())
            search_query |= Q(seat_id__section_id__name__icontains=search.strip())
        
        queryset = queryset.filter(search_query).distinct()
    
    # Apply sorting
    sort_applied = None
    if sort and sort.strip():
        sort_fields = []
        for field in sort.split(','):
            field = field.strip()
            if is_seat_pack:
                # Seat pack sorting
                if field.startswith('-'):
                    field_name = field[1:]
                    if field_name in ['row_label', 'pack_size', 'pack_price', 'created_at']:
                        sort_fields.append(field)
                    elif field_name == 'zone_name':
                        sort_fields.append('-zone_id__name')
                else:
                    if field in ['row_label', 'pack_size', 'pack_price', 'created_at']:
                        sort_fields.append(field)
                    elif field == 'zone_name':
                        sort_fields.append('zone_id__name')
            else:
                # Seat sorting
                if field.startswith('-'):
                    field_name = field[1:]
                    if field_name in ['price', 'status', 'created_at']:
                        sort_fields.append(field)
                    elif field_name == 'row_label':
                        sort_fields.append('-seat_id__row_label')
                    elif field_name == 'seat_number':
                        sort_fields.append('-seat_id__seat_number')
                    elif field_name == 'zone_name':
                        sort_fields.append('-seat_id__zone_id__name')
                    elif field_name == 'section_name':
                        sort_fields.append('-seat_id__section_id__name')
                else:
                    if field in ['price', 'status', 'created_at']:
                        sort_fields.append(field)
                    elif field == 'row_label':
                        sort_fields.append('seat_id__row_label')
                    elif field == 'seat_number':
                        sort_fields.append('seat_id__seat_number')
                    elif field == 'zone_name':
                        sort_fields.append('seat_id__zone_id__name')
                    elif field == 'section_name':
                        sort_fields.append('seat_id__section_id__name')
        
        if sort_fields:
            queryset = queryset.order_by(*sort_fields)
            sort_applied = ','.join(sort_fields)
    
    # Default sorting if none applied
    if not sort_applied:
        if is_seat_pack:
            queryset = queryset.order_by('zone_id__display_order', 'row_label')
            sort_applied = 'zone_id__display_order,row_label'
        else:
            queryset = queryset.order_by('seat_id__row_label', 'seat_id__seat_number')
            sort_applied = 'seat_id__row_label,seat_id__seat_number'
    
    return queryset, filters_applied, sort_applied


@register_venue_task('get_performance_data')
def handle_get_performance_data(data):
    """Get seat or seat pack data for a performance with filtering, search, and sorting"""
    try:
        from scrapers.models import Performance, Event, Venue
        
        # Extract required parameters
        performance_id = data.get('performance_id')
        mode = data.get('mode', 'seats')  # 'seats' or 'seat_packs'
        
        # Extract pagination parameters
        page = data.get('page', 1)
        limit = data.get('limit', 50)
        
        # Extract filters
        filters = {
            'zone_id': data.get('zone_id'),
            'section_id': data.get('section_id'),
            'level_id': data.get('level_id'),
            'row_label': data.get('row_label'),
            'seat_type': data.get('seat_type'),
            'status': data.get('status'),
            'price_min': data.get('price_min'),
            'price_max': data.get('price_max'),
            'pack_size_min': data.get('pack_size_min'),
            'pack_size_max': data.get('pack_size_max')
        }
        
        # Extract search and sort
        search = data.get('search')
        sort = data.get('sort')
        
        # Validate parameters
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'PERFORMANCE_ID_REQUIRED'
            )
        
        if mode not in ['seats', 'seat_packs']:
            return create_error_response(
                'mode must be either "seats" or "seat_packs"',
                'INVALID_MODE'
            )
        
        try:
            page = int(page) if page else 1
            limit = int(limit) if limit else 50
        except (ValueError, TypeError):
            return create_error_response(
                'Invalid page or limit parameter',
                'INVALID_PAGINATION_PARAMS'
            )
        
        if page < 1:
            return create_error_response(
                'Page number must be greater than 0',
                'INVALID_PAGE_NUMBER'
            )
        
        # Find performance
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            try:
                performance = Performance.objects.get(source_performance_id=performance_id, is_active=True)
            except Performance.DoesNotExist:
                return create_error_response(
                    f'Performance with ID {performance_id} not found',
                    'PERFORMANCE_NOT_FOUND'
                )
        
        # Find latest successful scrape for this performance
        latest_scrape = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        ).order_by('-scraped_at_utc').first()
        
        if not latest_scrape:
            return create_error_response(
                f'No successful scrape jobs found for performance {performance_id}',
                'NO_SCRAPE_DATA_FOUND'
            )
        
        # Build query based on mode
        if mode == 'seats':
            base_query = SeatSnapshot.objects.filter(
                scrape_job_key=latest_scrape
            ).select_related(
                'seat_id__section_id__level_id',
                'seat_id__zone_id'
            )
        else:  # seat_packs
            base_query = SeatPack.objects.filter(
                scrape_job_key=latest_scrape
            ).select_related('zone_id')
        
        # Apply filters, search, and sort
        filtered_query, filters_applied, sort_applied = create_seat_filters_queryset(
            base_query, filters, search, sort, is_seat_pack=(mode == 'seat_packs')
        )
        
        # Apply pagination
        paginator = Paginator(filtered_query, min(limit, 100))
        try:
            page_obj = paginator.page(page)
        except Exception:
            return create_error_response(
                f'Page {page} does not exist',
                'INVALID_PAGE_NUMBER'
            )
        
        # Build response data
        if mode == 'seats':
            data_list = []
            for snapshot in page_obj:
                seat = snapshot.seat_id
                data_list.append({
                    'id': seat.internal_seat_id,
                    'seat_key': seat.seat_key,
                    'row_label': seat.row_label,
                    'seat_number': seat.seat_number,
                    'seat_type': seat.seat_type,
                    'x_coord': seat.x_coord,
                    'y_coord': seat.y_coord,
                    'status': snapshot.status,
                    'price': snapshot.price,
                    'fees': snapshot.fees,
                    'snapshot_time': snapshot.snapshot_time.isoformat(),
                    'section': {
                        'id': seat.section_id.internal_section_id,
                        'section_key': seat.section_id.section_key,
                        'name': seat.section_id.name,
                        'section_type': seat.section_id.section_type,
                        'level': {
                            'id': seat.section_id.level_id.internal_level_id,
                            'level_key': seat.section_id.level_id.level_key,
                            'name': seat.section_id.level_id.name,
                            'level_number': seat.section_id.level_id.level_number,
                            'level_type': seat.section_id.level_id.level_type
                        }
                    },
                    'zone': {
                        'id': seat.zone_id.internal_zone_id,
                        'zone_key': seat.zone_id.zone_key,
                        'name': seat.zone_id.name,
                        'zone_type': seat.zone_id.zone_type,
                        'color_code': seat.zone_id.color_code,
                        'view_type': seat.zone_id.view_type,
                        'wheelchair_accessible': seat.zone_id.wheelchair_accessible
                    } if seat.zone_id else None,
                    'level': {
                        'id': seat.section_id.level_id.internal_level_id,
                        'level_key': seat.section_id.level_id.level_key,
                        'name': seat.section_id.level_id.name,
                        'level_number': seat.section_id.level_id.level_number,
                        'level_type': seat.section_id.level_id.level_type
                    } if seat.section_id and seat.section_id.level_id else None
                })
        else:  # seat_packs
            data_list = []
            for pack in page_obj:
                # Get seat details for this pack
                pack_seats = Seat.objects.filter(
                    source_seat_id__in=pack.seat_ids,
                    is_active=True
                ).select_related('section_id__level_id').order_by('row_label', 'seat_number')
                
                data_list.append({
                    'id': pack.internal_pack_id,
                    'pack_key': pack.pack_key,
                    'source_pack_id': pack.source_pack_id,
                    'row_label': pack.row_label,
                    'start_seat_number': pack.start_seat_number,
                    'end_seat_number': pack.end_seat_number,
                    'pack_size': pack.pack_size,
                    'pack_price': pack.pack_price,
                    'zone': {
                        'id': pack.zone_id.internal_zone_id,
                        'zone_key': pack.zone_id.zone_key,
                        'name': pack.zone_id.name,
                        'zone_type': pack.zone_id.zone_type,
                        'color_code': pack.zone_id.color_code,
                        'view_type': pack.zone_id.view_type,
                        'wheelchair_accessible': pack.zone_id.wheelchair_accessible
                    },
                    'seats': [{
                        'id': seat.internal_seat_id,
                        'seat_key': seat.seat_key,
                        'row_label': seat.row_label,
                        'seat_number': seat.seat_number,
                        'seat_type': seat.seat_type,
                        'x_coord': seat.x_coord,
                        'y_coord': seat.y_coord,
                        'section': {
                            'id': seat.section_id.internal_section_id,
                            'section_key': seat.section_id.section_key,
                            'name': seat.section_id.name,
                            'level': {
                                'id': seat.section_id.level_id.internal_level_id,
                                'level_key': seat.section_id.level_id.level_key,
                                'name': seat.section_id.level_id.name,
                                'level_number': seat.section_id.level_id.level_number
                            }
                        }
                    } for seat in pack_seats],
                    'created_at': pack.created_at.isoformat()
                })
        
        pagination_info = {
            'page': page,
            'limit': min(limit, 100),
            'total_pages': paginator.num_pages,
            'total_count': paginator.count,
            'has_next': page_obj.has_next(),
            'has_previous': page_obj.has_previous()
        }
        
        # Note: Changed response structure to use 'data' instead of separate response_data
        return create_paginated_response(
            {
                'performance_id': performance.internal_performance_id,
                'event_id': performance.event_id.internal_event_id,
                'venue_id': performance.venue_id.internal_venue_id,
                'mode': mode,
                mode: data_list,  # 'seats' or 'seat_packs'
                'scrape_info': {
                    'scrape_job_id': latest_scrape.scrape_job_key,
                    'scraped_at': latest_scrape.scraped_at_utc.isoformat(),
                    'scraper_name': latest_scrape.scraper_name,
                    'source_website': latest_scrape.source_website
                }
            },
            pagination_info,
            filters_applied,
            sort_applied
        )
        
    except Exception as e:
        logger.error(f"Error getting performance data: {e}")
        return create_error_response(
            f'Failed to retrieve performance data: {str(e)}',
            'PERFORMANCE_DATA_FETCH_ERROR'
        )


@register_venue_task('get_performance_structure')
def handle_get_performance_structure(data):
    """Get performance seating structure (levels, zones, sections) without seat data"""
    try:
        from django.db.models import Count, Min, Max
        
        performance_id = data.get('performance_id')
        
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'PERFORMANCE_ID_REQUIRED'
            )

        # Find performance by internal_performance_id first, then by source_performance_id
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            try:
                performance = Performance.objects.get(source_performance_id=performance_id, is_active=True)
            except Performance.DoesNotExist:
                return create_error_response(
                    f'Performance with ID {performance_id} not found',
                    'PERFORMANCE_NOT_FOUND'
                )

        # Find latest successful scrape for pricing and seat count data
        latest_scrape = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        ).order_by('-scraped_at_utc').first()

        # Get seat statistics by level from latest scrape
        level_stats = {}
        if latest_scrape:
            level_seat_stats = SeatSnapshot.objects.filter(
                scrape_job_key=latest_scrape,
                status="available"
            ).values(
                'seat_id__section_id__level_id__internal_level_id'
            ).annotate(
                seat_count=Count('seat_id'),
                min_price=Min('price'),
                max_price=Max('price')
            )
            
            level_stats = {
                stat['seat_id__section_id__level_id__internal_level_id']: stat 
                for stat in level_seat_stats
            }

        # Get all levels for this performance with their sections
        levels_query = Level.objects.filter(
            performancelevel__performance=performance,
            is_active=True
        ).prefetch_related('sections').order_by('display_order', 'name')

        levels_data = []
        total_sections = 0
        for level in levels_query:
            sections_data = []
            # Use prefetched sections
            for section in level.sections.filter(is_active=True).order_by('display_order', 'name'):
                sections_data.append({
                    'id': section.internal_section_id,
                    'section_key': getattr(section, 'section_key', section.source_section_id),
                    'name': section.name,
                    'section_type': section.section_type,
                    'display_order': section.display_order
                })
            
            # Get seat statistics for this level
            stats = level_stats.get(level.internal_level_id, {})
            
            levels_data.append({
                'id': level.internal_level_id,
                'level_key': getattr(level, 'level_key', level.source_level_id),
                'name': level.alias,
                'level_number': level.level_number,
                'level_type': level.level_type,
                'display_order': level.display_order,
                'seat_count': stats.get('seat_count', 0),
                'min_price': float(stats.get('min_price', 0)) if stats.get('min_price') is not None else 0.0,
                'max_price': float(stats.get('max_price', 0)) if stats.get('max_price') is not None else 0.0,
                'sections': sections_data
            })
            total_sections += len(sections_data)

        # Get all zones for this performance
        zones_query = Zone.objects.filter(
            performance_id=performance,
            is_active=True
        ).order_by('display_order')
        
        zones_data = []
        for zone in zones_query:
            zones_data.append({
                'id': zone.internal_zone_id,
                'zone_key': getattr(zone, 'zone_key', zone.source_zone_id),
                'name': zone.name,
                'zone_type': zone.zone_type,
                'color_code': zone.color_code,
                'view_type': zone.view_type,
                'wheelchair_accessible': zone.wheelchair_accessible,
                'display_order': zone.display_order,
            })

        # Build response data
        structure_data = {
            'performance_id': performance.internal_performance_id,
            'event_id': performance.event_id.internal_event_id,
            'venue_id': performance.venue_id.internal_venue_id,
            'datetime': performance.performance_datetime_utc.isoformat(),
            'levels': levels_data,
            'zones': zones_data,
            'counts': {
                'levels': len(levels_data),
                'zones': len(zones_data),
                'sections': total_sections
            }
        }

        return {
            'success': True,
            'data': structure_data
        }

    except Exception as e:
        logger.error(f"Error getting performance structure: {e}")
        return create_error_response(
            f'Failed to retrieve performance structure: {str(e)}',
            'PERFORMANCE_STRUCTURE_FETCH_ERROR'
        )


@register_venue_task('get_performance_seats')
def handle_get_performance_seats(data):
    """Get seats for a performance with pagination - return clean seat info only"""
    try:
        from scrapers.models import Performance, SeatSnapshot
        
        # Extract required parameters
        performance_id = data.get('performance_id')
        
        # Extract pagination parameters
        page = data.get('page', 1)
        limit = data.get('limit', 50)
        
        # Extract filters
        filters = {
            'zone_id': data.get('zone_id'),
            'section_id': data.get('section_id'),
            'level_id': data.get('level_id'),
            'min_price': data.get('min_price'),
            'max_price': data.get('max_price'),
            'status': data.get('status'),
            'available_only': data.get('available_only'),
        }
        
        # Extract search and sort
        search = data.get('search')
        sort = data.get('sort', 'price')
        
        # Validate parameters
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'PERFORMANCE_ID_REQUIRED'
            )
        
        try:
            page = int(page) if page else 1
            limit = int(limit) if limit else 50
        except (ValueError, TypeError):
            return create_error_response(
                'Invalid page or limit parameter',
                'INVALID_PAGINATION_PARAMS'
            )
        
        if page < 1:
            return create_error_response(
                'Page number must be greater than 0',
                'INVALID_PAGE_NUMBER'
            )
        
        # Find performance
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            try:
                performance = Performance.objects.get(source_performance_id=performance_id, is_active=True)
            except Performance.DoesNotExist:
                return create_error_response(
                    f'Performance with ID {performance_id} not found',
                    'PERFORMANCE_NOT_FOUND'
                )
        
        # Find latest successful scrape for this performance
        latest_scrape = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        ).order_by('-scraped_at_utc').first()
        
        if not latest_scrape:
            return create_error_response(
                f'No successful scrape jobs found for performance {performance_id}',
                'NO_SCRAPE_DATA_FOUND'
            )
        
        # Build base query
        base_query = SeatSnapshot.objects.filter(
            scrape_job_key=latest_scrape
        ).select_related(
            'seat_id__section_id__level_id',
            'seat_id__zone_id'
        )
        
        # Apply filters
        filters_applied = {}
        if filters:
            for filter_key, filter_value in filters.items():
                if filter_value is not None and filter_value != '':
                    filters_applied[filter_key] = filter_value
                    
                    if filter_key == 'zone_id':
                        base_query = base_query.filter(seat_id__zone_id__internal_zone_id=filter_value)
                    elif filter_key == 'section_id':
                        base_query = base_query.filter(seat_id__section_id__internal_section_id=filter_value)
                    elif filter_key == 'level_id':
                        base_query = base_query.filter(seat_id__section_id__level_id__internal_level_id=filter_value)
                    elif filter_key == 'min_price':
                        base_query = base_query.filter(price__gte=filter_value)
                    elif filter_key == 'max_price':
                        base_query = base_query.filter(price__lte=filter_value)
                    elif filter_key == 'status':
                        base_query = base_query.filter(status__iexact=filter_value)
                    elif filter_key == 'available_only' and filter_value:
                        base_query = base_query.filter(status='available')
        
        # Apply search
        if search and search.strip():
            search_query = Q(seat_id__row_label__icontains=search.strip())
            search_query |= Q(seat_id__seat_number__icontains=search.strip())
            search_query |= Q(seat_id__zone_id__name__icontains=search.strip())
            base_query = base_query.filter(search_query).distinct()
        
        # Apply sorting
        sort_applied = None
        if sort:
            if sort in ['price', '-price']:
                base_query = base_query.order_by(sort, 'seat_id__row_label', 'seat_id__seat_number')
                sort_applied = sort
            elif sort in ['row_label', '-row_label']:
                base_query = base_query.order_by(f'seat_id__{sort}', 'seat_id__seat_number')
                sort_applied = sort
            elif sort in ['seat_number', '-seat_number']:
                base_query = base_query.order_by('seat_id__row_label', f'seat_id__{sort}')
                sort_applied = sort
            elif sort in ['status', '-status']:
                base_query = base_query.order_by(sort, 'price')
                sort_applied = sort
        
        if not sort_applied:
            base_query = base_query.order_by('price', 'seat_id__row_label', 'seat_id__seat_number')
            sort_applied = 'price'
        
        # Apply pagination
        paginator = Paginator(base_query, min(limit, 100))
        try:
            page_obj = paginator.page(page)
        except Exception:
            return create_error_response(
                f'Page {page} does not exist',
                'INVALID_PAGE_NUMBER'
            )
        
        # Build clean seat data with section info
        seats_data = []
        for snapshot in page_obj:
            seat = snapshot.seat_id
            
            seat_data = {
                'id': seat.internal_seat_id,
                'row_label': seat.row_label,
                'seat_number': seat.seat_number,
                'seat_type': seat.seat_type,
                'price': float(snapshot.price) if snapshot.price is not None else 0.0,
                'fees': float(snapshot.fees) if snapshot.fees is not None else 0.0,
                'status': snapshot.status,
                'zone_id': seat.zone_id.internal_zone_id if seat.zone_id else None,
                'section_id': seat.section_id.internal_section_id,
                'level_id': seat.section_id.level_id.internal_level_id,
                'x_coord': float(seat.x_coord) if seat.x_coord is not None else None,
                'y_coord': float(seat.y_coord) if seat.y_coord is not None else None,
                'last_updated': snapshot.snapshot_time.isoformat(),
                # Add section info with alias
                'section': {
                    'id': seat.section_id.internal_section_id,
                    'name': seat.section_id.name,
                    'alias': seat.section_id.alias or seat.section_id.name,  # Use alias if available, fallback to name
                    'section_type': seat.section_id.section_type,
                }
            }
            seats_data.append(seat_data)
        
        pagination_info = {
            'page': page,
            'limit': min(limit, 100),
            'total': paginator.count,
            'total_pages': paginator.num_pages,
            'has_next': page_obj.has_next(),
            'has_previous': page_obj.has_previous()
        }
        
        return create_paginated_response(
            seats_data,
            pagination_info,
            filters_applied,
            sort_applied
        )
        
    except Exception as e:
        logger.error(f"Error getting performance seats: {e}")
        return create_error_response(
            f'Failed to retrieve performance seats: {str(e)}',
            'PERFORMANCE_SEATS_FETCH_ERROR'
        )


@register_venue_task('get_performance_seat_packs')
def handle_get_performance_seat_packs(data):
    """Get all seat packs for a performance with rich POS listing data and enhanced hierarchy"""
    try:
        # Extract required parameters
        performance_id = data.get('performance_id')
        
        # Extract pagination parameters
        page = data.get('page', 1)
        limit = data.get('limit', 20)
        
        # Extract enhanced filters including POS and lifecycle data
        filters = {
            'zone_id': data.get('zone_id'),
            'level_id': data.get('level_id'),
            'min_pack_size': data.get('min_pack_size'),
            'max_pack_size': data.get('max_pack_size'),
            'min_price': data.get('min_price'),
            'max_price': data.get('max_price'),
            'creation_event': data.get('creation_event'),
            'has_pos_listing': data.get('has_pos_listing'),
            'admin_hold_status': data.get('admin_hold_status'),
            'pos_status': data.get('pos_status'),
            # 'available_only': data.get('available_only'),
        }
        
        # Extract sort
        sort = data.get('sort', 'pack_price')
        
        # Validate parameters
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'PERFORMANCE_ID_REQUIRED'
            )
        
        try:
            page = int(page) if page else 1
            limit = int(limit) if limit else 20
        except (ValueError, TypeError):
            return create_error_response(
                'Invalid page or limit parameter',
                'INVALID_PAGINATION_PARAMS'
            )
        
        if page < 1:
            return create_error_response(
                'Page number must be greater than 0',
                'INVALID_PAGE_NUMBER'
            )
        
        # Find performance
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id)
        except Performance.DoesNotExist:
            return create_error_response(
                f'Performance with ID {performance_id} not found',
                'PERFORMANCE_NOT_FOUND'
            )
        
        # Find successful scrape jobs for this performance
        successful_scrapes = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        ).order_by('-scraped_at_utc')
        
        if not successful_scrapes.exists():
            return create_error_response(
                f'No successful scrape jobs found for performance {performance_id}',
                'NO_SCRAPE_DATA_FOUND'
            )
        
        # Build base query with enhanced select_related for rich data
        base_query = SeatPack.objects.filter(
            scrape_job_key__in=successful_scrapes
        ).select_related(
            'zone_id', 
            'scrape_job_key',
            'pos_listing',
            'level',  # Include level for hierarchy
            'performance', 
            'event'
        )
        
        # Check if any seat packs exist
        if not base_query.exists():
            return create_error_response(
                f'No seat packs found for performance {performance_id}',
                'NO_SEAT_PACKS_FOUND'
            )
        
        # Apply enhanced filters including POS and lifecycle data
        filters_applied = {}
        if filters:
            for filter_key, filter_value in filters.items():
                if filter_value is not None and filter_value != '':
                    filters_applied[filter_key] = filter_value
                    
                    # Existing filters
                    if filter_key == 'zone_id':
                        base_query = base_query.filter(zone_id__internal_zone_id=filter_value)
                    elif filter_key == 'level_id':
                        base_query = base_query.filter(level__internal_level_id=filter_value)
                    elif filter_key == 'min_pack_size':
                        base_query = base_query.filter(pack_size__gte=filter_value)
                    elif filter_key == 'max_pack_size':
                        base_query = base_query.filter(pack_size__lte=filter_value)
                    elif filter_key == 'min_price':
                        base_query = base_query.filter(pack_price__gte=filter_value)
                    elif filter_key == 'max_price':
                        base_query = base_query.filter(pack_price__lte=filter_value)
                    
                    # New lifecycle filters (updated for new model)
                    elif filter_key == 'is_active' and isinstance(filter_value, bool):
                        # Map is_active to pack_status: True -> 'active', False -> 'inactive'
                        pack_status = 'active' if filter_value else 'inactive'
                        base_query = base_query.filter(pack_status=pack_status)
                    elif filter_key == 'creation_event':
                        # Map creation_event to pack_state
                        base_query = base_query.filter(pack_state=filter_value)
                    
                    # New POS filters
                    elif filter_key == 'has_pos_listing' and isinstance(filter_value, bool):
                        if filter_value:
                            base_query = base_query.filter(pos_listing__isnull=False)
                        else:
                            base_query = base_query.filter(pos_listing__isnull=True)
                    elif filter_key == 'admin_hold_status' and isinstance(filter_value, bool):
                        base_query = base_query.filter(pos_listing__admin_hold_applied=filter_value)
                    elif filter_key == 'pos_status':
                        base_query = base_query.filter(pos_listing__status=filter_value)
        
        # Apply enhanced sorting with new options
        sort_applied = None
        if sort:
            if sort in ['pack_price', '-pack_price']:
                base_query = base_query.order_by(sort, '-scrape_job_key__scraped_at_utc', 'zone_id__display_order')
                sort_applied = sort
            elif sort in ['pack_size', '-pack_size']:
                base_query = base_query.order_by(sort, '-scrape_job_key__scraped_at_utc', 'pack_price')
                sort_applied = sort
            elif sort in ['row_label', '-row_label']:
                base_query = base_query.order_by(sort, '-scrape_job_key__scraped_at_utc', 'start_seat_number')
                sort_applied = sort
            elif sort in ['scraped_at', '-scraped_at']:
                scrape_sort = '-scrape_job_key__scraped_at_utc' if sort == '-scraped_at' else 'scrape_job_key__scraped_at_utc'
                base_query = base_query.order_by(scrape_sort, 'pack_price')
                sort_applied = sort
            elif sort in ['level_name', '-level_name']:
                level_sort = '-level__name' if sort == '-level_name' else 'level__name'
                base_query = base_query.order_by(level_sort, 'zone_id__display_order', 'pack_price')
                sort_applied = sort
            elif sort in ['creation_event', '-creation_event']:
                # Map creation_event sorting to pack_state
                pack_state_sort = '-pack_state' if sort == '-creation_event' else 'pack_state'
                base_query = base_query.order_by(pack_state_sort, '-created_at')
                sort_applied = sort
            elif sort in ['pos_status', '-pos_status']:
                pos_sort = '-pos_listing__status' if sort == '-pos_status' else 'pos_listing__status'
                base_query = base_query.order_by(pos_sort, 'pack_price')
                sort_applied = sort
        
        if not sort_applied:
            base_query = base_query.order_by('-scrape_job_key__scraped_at_utc', 'pack_price', 'zone_id__display_order')
            sort_applied = '-scraped_at,pack_price'
        
        # Apply pagination
        paginator = Paginator(base_query, min(limit, 50))
        try:
            page_obj = paginator.page(page)
        except Exception:
            return create_error_response(
                f'Page {page} does not exist',
                'INVALID_PAGE_NUMBER'
            )
        
        # Build enhanced seat pack data with POS listing and hierarchy information
        seat_packs_data = []
        
        # Try both approaches: seat_keys if available, otherwise find seats by zone/row/seat range
        seat_details_by_pack = {}
        
        for pack in page_obj:
            pack_seats = []
            
            # Method 1: Try using seat_keys if available
            if pack.seat_keys:
                logger.debug(f"Pack {pack.internal_pack_id} has seat_keys: {pack.seat_keys}")
                seats = Seat.objects.filter(
                    internal_seat_id__in=pack.seat_keys
                ).select_related('zone_id', 'section_id__level_id')
                
                for seat in seats:
                    pack_seats.append({
                        'id': seat.internal_seat_id,
                        'row_label': seat.row_label,
                        'seat_number': seat.seat_number,
                        'seat_type': seat.seat_type,
                        'x_coord': float(seat.x_coord) if seat.x_coord is not None else None,
                        'y_coord': float(seat.y_coord) if seat.y_coord is not None else None,
                        'section': {
                            'id': seat.section_id.internal_section_id if seat.section_id else None,
                            'name': seat.section_id.name if seat.section_id else None,
                            'alias': seat.section_id.alias or seat.section_id.name if seat.section_id else None,
                            'section_type': seat.section_id.section_type if seat.section_id else None,
                        }
                        # Note: Level info moved to top-level pack data for cleaner hierarchy
                    })
            
            # Method 2: If no seat_keys or no seats found, try to find seats by zone/row/seat range
            if not pack_seats:
                logger.debug(f"Trying to find seats for pack {pack.internal_pack_id} by zone/row/seat range")
                try:
                    # Parse seat numbers to find the range
                    start_num = int(pack.start_seat_number) if pack.start_seat_number.isdigit() else None
                    end_num = int(pack.end_seat_number) if pack.end_seat_number.isdigit() else None
                    
                    if start_num is not None and end_num is not None:
                        # Find seats in the zone and row that fall within the seat number range
                        seats = Seat.objects.filter(
                            zone_id=pack.zone_id,
                            row_label=pack.row_label,
                            is_active=True
                        ).select_related('zone_id', 'section_id__level_id')
                        
                        # Filter by seat number range
                        for seat in seats:
                            if seat.seat_number.isdigit():
                                seat_num = int(seat.seat_number)
                                if start_num <= seat_num <= end_num:
                                    pack_seats.append({
                                        'id': seat.internal_seat_id,
                                        'row_label': seat.row_label,
                                        'seat_number': seat.seat_number,
                                        'seat_type': seat.seat_type,
                                        'x_coord': float(seat.x_coord) if seat.x_coord is not None else None,
                                        'y_coord': float(seat.y_coord) if seat.y_coord is not None else None,
                                        'section': {
                                            'id': seat.section_id.internal_section_id if seat.section_id else None,
                                            'name': seat.section_id.name if seat.section_id else None,
                                            'alias': seat.section_id.alias or seat.section_id.name if seat.section_id else None,
                                            'section_type': seat.section_id.section_type if seat.section_id else None,
                                        }
                                        # Note: Level info moved to top-level pack data for cleaner hierarchy
                                    })
                except Exception as e:
                    logger.warning(f"Failed to parse seat range for pack {pack.internal_pack_id}: {e}")
            
            seat_details_by_pack[pack.internal_pack_id] = pack_seats
            logger.debug(f"Pack {pack.internal_pack_id} resolved {len(pack_seats)} seats")
        
        # Build enhanced pack data with rich POS and lifecycle information
        for pack in page_obj:
            pack_seats = seat_details_by_pack.get(pack.internal_pack_id, [])
            
            # Extract level information from pack.level (from select_related) or first seat
            level_data = None
            if pack.level:
                level_data = {
                    'id': pack.level.internal_level_id,
                    'name': pack.level.alias,
                    'level_number': pack.level.level_number,
                    'level_type': pack.level.level_type,
                }
            elif pack_seats and pack_seats[0].get('section'):
                # Fallback: get level info from first seat if pack.level is not available
                first_seat = pack_seats[0]
                # Would need to fetch this from seat section relationships
                pass
            
            # Extract section information from first seat for pack-level section
            section_data = None
            if pack_seats and pack_seats[0].get('section'):
                first_seat_section = pack_seats[0]['section']
                section_data = {
                    'id': first_seat_section['id'],
                    'name': first_seat_section['name'],
                    'alias': first_seat_section['alias'],
                    'section_type': first_seat_section['section_type'],
                }
            
            # Build POS listing data if available
            pos_listing_data = None
            if pack.pos_listing:
                pos_listing_data = {
                    'pos_inventory_id': pack.pos_listing.pos_inventory_id,
                    'stubhub_inventory_id': pack.pos_listing.stubhub_inventory_id,
                    'status': pack.pos_listing.status,
                    'admin_hold_applied': pack.pos_listing.admin_hold_applied,
                    'admin_hold_date': pack.pos_listing.admin_hold_date.isoformat() if pack.pos_listing.admin_hold_date else None,
                    'admin_hold_reason': pack.pos_listing.admin_hold_reason,
                    'created_at': pack.pos_listing.created_at.isoformat(),
                    'updated_at': pack.pos_listing.updated_at.isoformat(),
                }
            
            # Build lifecycle data (updated for new model)
            lifecycle_data = {
                'is_active': pack.pack_status == 'active',  # Map pack_status to is_active for backward compatibility
                'manually_delisted': pack.manually_delisted,
                'creation_event': pack.pack_state,  # Map pack_state to creation_event for backward compatibility
                'source_pack_ids': pack.source_pack_ids or [],
                'delist_reason': pack.delist_reason,
                # New fields from four-dimensional model
                'pack_status': pack.pack_status,
                'pos_status': pack.pos_status,
                'pack_state': pack.pack_state,
            }
            
            pack_data = {
                'id': pack.internal_pack_id,
                'source_pack_id': pack.source_pack_id,
                'row_label': pack.row_label,
                'start_seat_number': pack.start_seat_number,
                'end_seat_number': pack.end_seat_number,
                'pack_size': pack.pack_size,
                'pack_price': float(pack.pack_price) if pack.pack_price is not None else 0.0,
                'total_price': float(pack.total_price) if pack.total_price is not None else 0.0,
                
                # Enhanced hierarchy with level and section at top level
                'level': level_data,
                'section': section_data,
                'zone': {
                    'id': pack.zone_id.internal_zone_id,
                    'name': pack.zone_id.name,
                    'zone_type': pack.zone_id.zone_type,
                    'color_code': pack.zone_id.color_code,
                    'view_type': pack.zone_id.view_type,
                    'wheelchair_accessible': pack.zone_id.wheelchair_accessible,
                    'display_order': pack.zone_id.display_order,
                },
                
                # Enhanced POS integration
                'pos_listing': pos_listing_data,
                
                # Enhanced lifecycle tracking
                'lifecycle': lifecycle_data,
                
                # Clean seat data (without redundant level info)
                'seats': pack_seats,
                
                # Enhanced metadata
                'created_at': pack.created_at.isoformat(),
                'updated_at': pack.updated_at.isoformat(),
                'scrape_info': {
                    'scrape_job_key': pack.scrape_job_key.scrape_job_key,
                    'scraped_at': pack.scrape_job_key.scraped_at_utc.isoformat(),
                    'scraper_name': pack.scrape_job_key.scraper_name,
                    'source_website': pack.scrape_job_key.source_website,
                },
                
                # Backward compatibility fields
                'zone_id': pack.zone_id.internal_zone_id,
                'level_id': level_data['id'] if level_data else None,
                'section_id': section_data['id'] if section_data else None,
            }
            seat_packs_data.append(pack_data)
        
        pagination_info = {
            'page': page,
            'limit': min(limit, 50),
            'total': paginator.count,
            'total_pages': paginator.num_pages,
            'has_next': page_obj.has_next(),
            'has_previous': page_obj.has_previous()
        }
        
        return create_paginated_response(
            seat_packs_data,
            pagination_info,
            filters_applied,
            sort_applied
        )
        
    except Exception as e:
        logger.error(f"Error getting performance seat packs: {e}")
        return create_error_response(
            f'Failed to retrieve performance seat packs: {str(e)}',
            'PERFORMANCE_SEAT_PACKS_FETCH_ERROR'
        )


@register_venue_task('get_performance_seat_pack_counts')
def handle_get_performance_seat_pack_counts(data):
    """Efficiently get counts of total and listed seat packs for a performance."""
    try:
        performance_id = data.get('performance_id')
        if not performance_id:
            return create_error_response('performance_id is required', 'PERFORMANCE_ID_REQUIRED')

        # Find performance
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id)
        except Performance.DoesNotExist:
            return create_error_response(f'Performance with ID {performance_id} not found', 'PERFORMANCE_NOT_FOUND')

        # Find successful scrape jobs for this performance
        successful_scrapes = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        )
        if not successful_scrapes.exists():
            return create_error_response(f'No successful scrape jobs found for performance {performance_id}', 'NO_SCRAPE_DATA_FOUND')

        # Build base query
        base_query = SeatPack.objects.filter(scrape_job_key__in=successful_scrapes)

        # Apply the same filters as the main seat pack fetcher
        filters = {
            'zone_id': data.get('zone_id'),
            'level_id': data.get('level_id'),
            'min_pack_size': data.get('min_pack_size'),
            'max_pack_size': data.get('max_pack_size'),
            'min_price': data.get('min_price'),
            'max_price': data.get('max_price'),
            'creation_event': data.get('creation_event'),
            'has_pos_listing': data.get('has_pos_listing'),
            'admin_hold_status': data.get('admin_hold_status'),
            'pos_status': data.get('pos_status'),
        }
        
        if filters:
            for filter_key, filter_value in filters.items():
                if filter_value is not None and filter_value != '':
                    if filter_key == 'zone_id':
                        base_query = base_query.filter(zone_id__internal_zone_id=filter_value)
                    elif filter_key == 'level_id':
                        base_query = base_query.filter(level__internal_level_id=filter_value)
                    # ... (add all other filters from the original function)

        # Get total count for the given filters
        total_count = base_query.count()

        # Get listed (active) count for the same filters
        listed_count = base_query.filter(pack_status='active').count()

        return {
            'success': True,
            'data': {
                'totalSeatPacks': total_count,
                'listedSeatPacks': listed_count,
            }
        }

    except Exception as e:
        logger.error(f"Error getting performance seat pack counts: {e}")
        return create_error_response(
            f'Failed to retrieve performance seat pack counts: {str(e)}',
            'PERFORMANCE_SEAT_PACK_COUNTS_FETCH_ERROR'
        )


@register_venue_task('update_level_aliases')
def handle_update_level_aliases(data):
    """Update level aliases for a venue"""
    try:
        from scrapers.models import Level, Venue
        
        # Extract required parameters
        venue_id = data.get('venue_id')
        level_aliases = data.get('level_aliases', [])
        
        # Validate required parameters
        if not venue_id:
            return create_error_response(
                'venue_id is required',
                'VENUE_ID_REQUIRED'
            )
        
        if not level_aliases or not isinstance(level_aliases, list) or len(level_aliases) == 0:
            return create_error_response(
                'level_aliases array is required and must not be empty',
                'LEVEL_ALIASES_REQUIRED'
            )
        
        # Validate venue exists
        try:
            venue = Venue.objects.get(internal_venue_id=venue_id, is_active=True)
        except Venue.DoesNotExist:
            return create_error_response(
                f'Venue with ID {venue_id} not found',
                'VENUE_NOT_FOUND'
            )
        
        # Validate all level aliases
        level_ids = [alias.get('level_id') for alias in level_aliases]
        if not all(level_ids):
            return create_error_response(
                'All level aliases must have a level_id',
                'INVALID_LEVEL_ALIAS_DATA'
            )
        
        # Check that all levels belong to the specified venue
        levels = Level.objects.filter(
            internal_level_id__in=level_ids,
            venue_id=venue,
            is_active=True
        )
        
        if levels.count() != len(level_ids):
            found_level_ids = set(levels.values_list('internal_level_id', flat=True))
            missing_level_ids = set(level_ids) - found_level_ids
            return create_error_response(
                f'Level(s) not found or do not belong to venue {venue_id}: {list(missing_level_ids)}',
                'INVALID_LEVEL_IDS'
            )
        
        # Update level aliases
        updated_levels = []
        
        for alias_data in level_aliases:
            level_id = alias_data.get('level_id')
            new_alias = alias_data.get('alias', '').strip()
            
            # Validate alias length
            if not new_alias or len(new_alias) > 255:
                return create_error_response(
                    f'Alias for level {level_id} must be between 1 and 255 characters',
                    'INVALID_ALIAS_LENGTH'
                )
            
            # Update the level
            level = levels.get(internal_level_id=level_id)
            level.alias = new_alias
            level.save(update_fields=['alias', 'updated_at'])
            
            updated_levels.append({
                'level_id': level.internal_level_id,
                'name': level.name,
                'alias': level.alias,
                'level_type': level.level_type
            })
        
        logger.info(f"Successfully updated {len(updated_levels)} level aliases for venue {venue_id}")
        
        return {
            'success': True,
            'data': {
                'venue_id': venue_id,
                'updated_levels': updated_levels
            }
        }
        
    except Exception as e:
        logger.error(f"Error updating level aliases: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return create_error_response(
            f'Failed to update level aliases: {str(e)}',
            'LEVEL_ALIASES_UPDATE_ERROR'
        )


@register_venue_task('update_section_aliases')
def handle_update_section_aliases(data):
    """Update section aliases for a venue"""
    try:
        from scrapers.models import Section, Level, Venue
        
        # Extract required parameters
        venue_id = data.get('venue_id')
        section_aliases = data.get('section_aliases', [])
        
        # Validate required parameters
        if not venue_id:
            return create_error_response(
                'venue_id is required',
                'VENUE_ID_REQUIRED'
            )
        
        if not section_aliases or not isinstance(section_aliases, list) or len(section_aliases) == 0:
            return create_error_response(
                'section_aliases array is required and must not be empty',
                'SECTION_ALIASES_REQUIRED'
            )
        
        # Validate venue exists
        try:
            venue = Venue.objects.get(internal_venue_id=venue_id, is_active=True)
        except Venue.DoesNotExist:
            return create_error_response(
                f'Venue with ID {venue_id} not found',
                'VENUE_NOT_FOUND'
            )
        
        # Validate all section aliases
        section_ids = [alias.get('section_id') for alias in section_aliases]
        if not all(section_ids):
            return create_error_response(
                'All section aliases must have a section_id',
                'INVALID_SECTION_ALIAS_DATA'
            )
        
        # Check that all sections belong to the specified venue (through their levels)
        sections = Section.objects.filter(
            internal_section_id__in=section_ids,
            level_id__venue_id=venue,
            is_active=True
        ).select_related('level_id')
        
        if sections.count() != len(section_ids):
            found_section_ids = set(sections.values_list('internal_section_id', flat=True))
            missing_section_ids = set(section_ids) - found_section_ids
            return create_error_response(
                f'Section(s) not found or do not belong to venue {venue_id}: {list(missing_section_ids)}',
                'INVALID_SECTION_IDS'
            )
        
        # Update section aliases
        updated_sections = []
        
        for alias_data in section_aliases:
            section_id = alias_data.get('section_id')
            new_alias = alias_data.get('alias', '').strip()
            
            # Validate alias length
            if not new_alias or len(new_alias) > 255:
                return create_error_response(
                    f'Alias for section {section_id} must be between 1 and 255 characters',
                    'INVALID_ALIAS_LENGTH'
                )
            
            # Update the section
            section = sections.get(internal_section_id=section_id)
            section.alias = new_alias
            section.save(update_fields=['alias', 'updated_at'])
            
            updated_sections.append({
                'section_id': section.internal_section_id,
                'name': section.name,
                'alias': section.alias,
                'section_type': section.section_type,
                'level_id': section.level_id.internal_level_id,
                'level_name': section.level_id.name
            })
        
        logger.info(f"Successfully updated {len(updated_sections)} section aliases for venue {venue_id}")
        
        return {
            'success': True,
            'data': {
                'venue_id': venue_id,
                'updated_sections': updated_sections
            }
        }
        
    except Exception as e:
        logger.error(f"Error updating section aliases: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return create_error_response(
            f'Failed to update section aliases: {str(e)}',
            'SECTION_ALIASES_UPDATE_ERROR'
        )


@register_venue_task('get_seat_summary')
def handle_get_seat_summary(data):
    """Get seat summary statistics by zone for a performance"""
    try:
        from scrapers.models import Performance, SeatSnapshot, SeatPack, Zone
        from django.db.models import Count, Min, Max, Avg
        
        # Extract required parameters
        performance_id = data.get('performance_id')
        
        # Validate parameters
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'PERFORMANCE_ID_REQUIRED'
            )
        
        # Find performance
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            try:
                performance = Performance.objects.get(source_performance_id=performance_id, is_active=True)
            except Performance.DoesNotExist:
                return create_error_response(
                    f'Performance with ID {performance_id} not found',
                    'PERFORMANCE_NOT_FOUND'
                )
        
        # Find latest successful scrape for this performance
        latest_scrape = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        ).order_by('-scraped_at_utc').first()
        
        if not latest_scrape:
            return create_error_response(
                f'No successful scrape jobs found for performance {performance_id}',
                'NO_SCRAPE_DATA_FOUND'
            )
        
        # Get all zones for this performance
        zones = Zone.objects.filter(
            performance=performance,
            is_active=True
        ).order_by('display_order')
        
        # Get seat counts and pricing by zone
        seat_stats = SeatSnapshot.objects.filter(
            scrape_job_key=latest_scrape
        ).values(
            'seat_id__zone_id__internal_zone_id',
            'seat_id__zone_id__name',
            'seat_id__zone_id__color_code',
            'seat_id__zone_id__view_type',
            'seat_id__zone_id__wheelchair_accessible'
        ).annotate(
            total_seats=Count('snapshot_id'),
            available_seats=Count('snapshot_id', filter=Q(status='available')),
            sold_seats=Count('snapshot_id', filter=Q(status='sold')),
            reserved_seats=Count('snapshot_id', filter=Q(status='reserved')),
            min_price=Min('price'),
            max_price=Max('price'),
            avg_price=Avg('price')
        )
        
        # Get seat pack counts by zone
        pack_stats = SeatPack.objects.filter(
            scrape_job_key=latest_scrape
        ).values(
            'zone_id__internal_zone_id'
        ).annotate(
            total_packs=Count('internal_pack_id'),
            min_pack_size=Min('pack_size'),
            max_pack_size=Max('pack_size')
        )
        
        # Create lookup for pack stats
        pack_stats_lookup = {stat['zone_id__internal_zone_id']: stat for stat in pack_stats}
        
        # Build zones summary
        zones_summary = []
        total_seats = 0
        total_available = 0
        total_sold = 0
        total_reserved = 0
        
        for seat_stat in seat_stats:
            zone_id = seat_stat['seat_id__zone_id__internal_zone_id']
            pack_stat = pack_stats_lookup.get(zone_id, {})
            
            zone_summary = {
                'zone_id': zone_id,
                'zone_key': zone_id,
                'name': seat_stat['seat_id__zone_id__name'],
                'color_code': seat_stat['seat_id__zone_id__color_code'],
                'view_type': seat_stat['seat_id__zone_id__view_type'],
                'wheelchair_accessible': seat_stat['seat_id__zone_id__wheelchair_accessible'],
                
                'seat_counts': {
                    'total': seat_stat['total_seats'],
                    'available': seat_stat['available_seats'],
                    'sold': seat_stat['sold_seats'],
                    'reserved': seat_stat['reserved_seats'],
                },
                
                'price_range': {
                    'min': float(seat_stat['min_price']) if seat_stat['min_price'] is not None else 0.0,
                    'max': float(seat_stat['max_price']) if seat_stat['max_price'] is not None else 0.0,
                    'average': float(seat_stat['avg_price']) if seat_stat['avg_price'] is not None else 0.0,
                },
                
                'pack_counts': {
                    'total_packs': pack_stat.get('total_packs', 0),
                    'available_packs': pack_stat.get('total_packs', 0),  # Assume all packs are available for now
                    'min_pack_size': pack_stat.get('min_pack_size', 0),
                    'max_pack_size': pack_stat.get('max_pack_size', 0),
                },
            }
            
            zones_summary.append(zone_summary)
            
            # Add to totals
            total_seats += seat_stat['total_seats']
            total_available += seat_stat['available_seats']
            total_sold += seat_stat['sold_seats']
            total_reserved += seat_stat['reserved_seats']
        
        # Build performance info
        performance_info = {
            'performance_id': performance.internal_performance_id,
            'event_name': performance.event_id.name,
            'venue_name': performance.venue_id.name,
            'datetime': performance.performance_datetime_utc.isoformat(),
        }
        
        # Build summary data
        summary_data = {
            'performance_id': performance.internal_performance_id,
            'total_seats': total_seats,
            'available_seats': total_available,
            'sold_seats': total_sold,
            'reserved_seats': total_reserved,
            'zones': zones_summary,
            'last_updated': latest_scrape.scraped_at_utc.isoformat(),
            'scrape_info': {
                'scrape_job_id': latest_scrape.scrape_job_key,
                'scraped_at': latest_scrape.scraped_at_utc.isoformat(),
                'scraper_name': latest_scrape.scraper_name,
            },
        }
        
        return {
            'success': True,
            'data': summary_data,
            'performance_info': performance_info,
        }
        
    except Exception as e:
        logger.error(f"Error getting seat summary: {e}")
        return create_error_response(
            f'Failed to retrieve seat summary: {str(e)}',
            'SEAT_SUMMARY_FETCH_ERROR'
        )


@register_venue_task('get_performance_scrape_status')
def handle_get_performance_scrape_status(data):
    """Get scrape status information for a specific performance"""
    try:
        from scrapers.models import Performance, ScrapeJob
        from django.db.models import Count, Q
        
        # Extract parameters
        performance_id = data.get('performance_id')
        
        # Validate parameters
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'PERFORMANCE_ID_REQUIRED'
            )
        
        # Find performance by internal_performance_id first, then by source_performance_id
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            try:
                performance = Performance.objects.get(source_performance_id=performance_id, is_active=True)
            except Performance.DoesNotExist:
                return create_error_response(
                    f'Performance with ID {performance_id} not found',
                    'PERFORMANCE_NOT_FOUND'
                )
        
        # Get scrape job statistics for this performance
        scrape_jobs = ScrapeJob.objects.filter(
            performance_id=performance
        ).order_by('-scraped_at_utc')
        # Get the most recent scrape job
        last_scrape_job = scrape_jobs.first()

        # Count total and successful scrapes
        total_scrapes = scrape_jobs.count()
        successful_scrapes = scrape_jobs.filter(scrape_success=True).count()
        failed_scrapes = scrape_jobs.filter(scrape_success=False).count()

        # Map scrape_success to our API format
        def map_scrape_status(scrape_job):
            if scrape_job is None:
                return None

            if scrape_job.scrape_success:
                return 'success'
            else:
                return 'failed'

        # Build response data
        scrape_status_data = {
            'last_scrape_time': last_scrape_job.scraped_at_utc.isoformat() if last_scrape_job else None,
            'last_scrape_status': map_scrape_status(last_scrape_job),
            'last_scrape_error': last_scrape_job.error_message if last_scrape_job else None,
            'total_scrapes': total_scrapes,
            'successful_scrapes': successful_scrapes,
            'failed_scrapes': failed_scrapes,
        }

        return {
            'success': True,
            'data': scrape_status_data
        }
    except Exception as e:
        logger.error(f"Error getting performance scrape status: {e}")
        return create_error_response(
            f'Failed to retrieve performance scrape status: {str(e)}',
            'PERFORMANCE_SCRAPE_STATUS_FETCH_ERROR'
        )

@register_venue_task('delist_performance_packs')
def handle_delist_performance_packs(data):
    """
    Manually delists all active seat packs for a given performance by calling
    the StubHub inventory creator to handle both database updates and API calls.
    """
    try:
        from scrapers.core.stubhub_inventory_creator import StubHubInventoryCreator
        
        performance_id = data.get('performance_id')
        user_id = data.get('user_id')
        
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'PERFORMANCE_ID_REQUIRED'
            )
        
        if not user_id:
            return create_error_response(
                'user_id is required for auditing',
                'USER_ID_REQUIRED'
            )
        
        # Validate user exists
        from django.contrib.auth.models import User
        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return create_error_response(
                f'User with ID {user_id} not found',
                'USER_NOT_FOUND'
            )
        
        # Get pack IDs that need delisting
        pack_ids = list(SeatPack.objects.filter(
            performance=performance_id,
            pack_status='active'
        ).values_list('internal_pack_id', flat=True))
        
        if not pack_ids:
            return {
                'success': True,
                'message': f'No active seat packs found for performance {performance_id} to delist.',
                'packs_delisted': 0
            }
        
        # Use StubHub's method to mark packs for deletion with audit info
        creator = StubHubInventoryCreator(pos_enabled=True)
        mark_result = creator.mark_multiple_packs_for_pos_deletion(pack_ids, 'manual_delist')
        creator.sync_pending_packs(performance_id)
        # Update audit trail manually since mark_multiple_packs_for_pos_deletion doesn't handle it
        SeatPack.objects.filter(internal_pack_id__in=pack_ids).update(
            manually_delisted_at=timezone.now(),
            manually_delisted_by=user
        )
        
        # Now sync the packs immediately for this performance
        sync_result = creator.sync_pending_packs(performance_id=performance_id)
        
        # Map sync results to NestJS expected format
        pos_sync_attempted = True
        pos_sync_success = sync_result.get('failed', 0) == 0
        pos_sync_results = {
            'packs_deleted': sync_result.get('deleted', 0),
            'packs_failed': sync_result.get('failed', 0),
            'total_packs': sync_result.get('created', 0) + sync_result.get('deleted', 0) + sync_result.get('failed', 0)
        }
        
        return {
            'success': True,
            'message': f'Successfully delisted {len(pack_ids)} seat packs for performance {performance_id}',
            'updated_count': len(pack_ids),  # NestJS expects this field name
            'packs_delisted': len(pack_ids),  # Keep for backward compatibility
            'pos_sync_attempted': pos_sync_attempted,  # NestJS expects this
            'pos_sync_success': pos_sync_success,      # NestJS expects this
            'pos_sync_results': pos_sync_results,      # NestJS expects this
            'sync_results': sync_result                # Keep original for debugging
        }
        
    except Exception as e:
        logger.error(f"Error delisting performance packs: {e}", exc_info=True)
        return create_error_response(
            f'Failed to delist performance packs: {str(e)}',
            'DELIST_PERFORMANCE_PACKS_ERROR'
        )
        
        # Get the most recent scrape job
        last_scrape_job = scrape_jobs.first()
        
        # Count total and successful scrapes
        total_scrapes = scrape_jobs.count()
        successful_scrapes = scrape_jobs.filter(scrape_success=True).count()
        failed_scrapes = scrape_jobs.filter(scrape_success=False).count()
        
        # Map scrape_success to our API format
        def map_scrape_status(scrape_job):
            if scrape_job is None:
                return None
            
            if scrape_job.scrape_success:
                return 'success'
            else:
                return 'failed'
        
        # Build response data
        scrape_status_data = {
            'last_scrape_time': last_scrape_job.scraped_at_utc.isoformat() if last_scrape_job else None,
            'last_scrape_status': map_scrape_status(last_scrape_job),
            'last_scrape_error': last_scrape_job.error_message if last_scrape_job else None,
            'total_scrapes': total_scrapes,
            'successful_scrapes': successful_scrapes,
            'failed_scrapes': failed_scrapes,
        }
        
        return {
            'success': True,
            'data': scrape_status_data
        }
        
    except Exception as e:
        logger.error(f"Error getting performance scrape status: {e}")
        return create_error_response(
            f'Failed to retrieve performance scrape status: {str(e)}',
            'PERFORMANCE_SCRAPE_STATUS_FETCH_ERROR'
        )


@register_venue_task('get_performance_names')
def handle_get_performance_names(data):
    """
    Get performance names and POS-related data for a list of performance IDs.
    This is designed to enrich data fetched by the NestJS POS overview endpoint.
    """
    try:
        from scrapers.models import Performance, Event, Venue, SeatPack
        from django.db.models import Count, Q

        # Extract parameters
        performance_ids = data.get('performance_ids', [])

        # Validate parameters
        if not performance_ids:
            return create_error_response(
                'performance_ids array is required',
                'MISSING_PERFORMANCE_IDS'
            )

        if not isinstance(performance_ids, list):
            return create_error_response(
                'performance_ids must be an array',
                'INVALID_PERFORMANCE_IDS_FORMAT'
            )

        logger.info(f"Fetching performance names and pack counts for {len(performance_ids)} performance IDs")

        # Query performances with related data and seat pack counts
        performances = Performance.objects.filter(
            internal_performance_id__in=performance_ids,
            is_active=True
        ).select_related(
            'event_id',
            'venue_id'
        ).annotate(
            totalSeatPacks=Count('seat_packs'),
            listedSeatPacks=Count('seat_packs', filter=Q(seat_packs__pack_status='active'))
        ).order_by('performance_datetime_utc')

        if not performances.exists():
            return create_error_response(
                'No performances found for the provided IDs',
                'PERFORMANCES_NOT_FOUND'
            )

        # Build response data
        performance_data = []
        for performance in performances:
            performance_data.append({
                'id': performance.internal_performance_id,
                'name': performance.event_id.name,
                'eventName': performance.event_id.name,
                'eventId': performance.event_id.internal_event_id,
                'venueName': performance.venue_id.name,
                'venueId': performance.venue_id.internal_venue_id,
                'venueLocation': f"{performance.venue_id.city}, {performance.venue_id.state}" if performance.venue_id.city and performance.venue_id.state else 'Location TBD',
                'performanceDateTime': performance.performance_datetime_utc.isoformat(),
                'totalSeatPacks': performance.totalSeatPacks,
                'listedSeatPacks': performance.listedSeatPacks,
                'createdAt': performance.created_at.isoformat(),
                'updatedAt': performance.updated_at.isoformat() if performance.updated_at else None,
            })

        logger.info(f"Retrieved data for {len(performance_data)} performances")

        return {
            'success': True,
            'data': performance_data,
            'message': f'Retrieved data for {len(performance_data)} performances',
            'total_requested': len(performance_ids),
            'total_found': len(performance_data)
        }

    except Exception as e:
        logger.error(f"Error getting performance names: {e}\{traceback.format_exc()}")
        return create_error_response(
            f'Failed to retrieve performance names: {str(e)}',
            'PERFORMANCE_NAMES_FETCH_ERROR'
        )


@register_venue_task('enable_pos_scraping')
def handle_enable_pos_scraping(data):
    """Enable POS scraping - create scrape job with pos: true"""
    try:
        from scrapers.models import Performance, ScrapeJob
        from django.utils import timezone
        import uuid
        
        # Extract parameters
        performance_id = data.get('performance_id')
        
        # Validate parameters
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'MISSING_PERFORMANCE_ID'
            )
        
        # Find performance by internal_performance_id
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            return create_error_response(
                f'Performance with ID {performance_id} not found',
                'PERFORMANCE_NOT_FOUND'
            )
        
        # Check if performance hasn't expired
        if performance.performance_datetime_utc < timezone.now():
            return create_error_response(
                f'Performance {performance_id} has already expired',
                'PERFORMANCE_EXPIRED'
            )
        
        # Get performance URL from event
        event = performance.event_id
        if not event.url:
            return create_error_response(
                f'No URL found for event {event.internal_event_id}',
                'EVENT_URL_NOT_FOUND'
            )
        
        # Create scrape job with POS enabled
        scrape_job = ScrapeJob.objects.create(
            scrape_job_key=str(uuid.uuid4()),
            performance_id=performance,
            url=event.url,
            scraper_name='pos_scraper',
            source_website=event.source_website or 'unknown',
            pos_enabled=True,
            scraped_at_utc=timezone.now(),
            scrape_success=False  # Will be updated when scraping completes
        )
        
        logger.info(f"Created POS scrape job {scrape_job.scrape_job_key} for performance {performance_id}")
        
        return {
            'success': True,
            'data': {
                'scrape_job_id': scrape_job.scrape_job_key,
                'performance_id': performance_id,
                'url': event.url,
                'pos_enabled': True,
                'created_at': scrape_job.scraped_at_utc.isoformat()
            },
            'message': f'POS scraping enabled for performance {performance_id}'
        }
        
    except Exception as e:
        logger.error(f"Error enabling POS scraping: {e}")
        return create_error_response(
            f'Failed to enable POS scraping: {str(e)}',
            'POS_SCRAPING_ENABLE_ERROR'
        )


@register_venue_task('disable_pos_scraping')
def handle_disable_pos_scraping(data):
    """Disable POS scraping - delist all seat packs"""
    try:
        from scrapers.models import Performance, SeatPack, Event
        from django.utils import timezone
        
        # Extract parameters
        performance_id = data.get('performance_id')
        event_id = data.get('event_id')
        
        # Validate parameters - need either performance_id or event_id
        if not performance_id and not event_id:
            return create_error_response(
                'Either performance_id or event_id is required',
                'MISSING_ID_PARAMETER'
            )
        
        performances_to_process = []
        
        if performance_id:
            # Process single performance
            try:
                performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
                performances_to_process = [performance]
            except Performance.DoesNotExist:
                return create_error_response(
                    f'Performance with ID {performance_id} not found',
                    'PERFORMANCE_NOT_FOUND'
                )
        
        elif event_id:
            # Process all performances for the event
            try:
                event = Event.objects.get(internal_event_id=event_id, is_active=True)
                performances_to_process = Performance.objects.filter(event_id=event, is_active=True)
            except Event.DoesNotExist:
                return create_error_response(
                    f'Event with ID {event_id} not found',
                    'EVENT_NOT_FOUND'
                )
        
        if not performances_to_process:
            return create_error_response(
                'No performances found to process',
                'NO_PERFORMANCES_FOUND'
            )
        
        # Enhanced delist logic with actual POS API calls
        total_delisted = 0
        performances_affected = 0
        total_errors = 0
        error_details = []
        
        for performance in performances_to_process:
            try:
                # Find all active seat packs for this performance
                active_seat_packs = SeatPack.objects.filter(
                    scrape_job_key__performance_id=performance,
                    is_active=True
                ).select_related('scrape_job_key')
                
                active_seat_packs_list = list(active_seat_packs)
                
                if not active_seat_packs_list:
                    logger.info(f"No active seat packs found for performance {performance.internal_performance_id}")
                    continue
                
                # Use POS sync service to properly delist from external APIs
                from ..scrapers.core.pos_sync_service import POSSyncService
                from ..scrapers.core.pos_config_handler import POSConfiguration, POSConfigurationHandler
                
                try:
                    # Get POS configuration for this performance
                    config_handler = POSConfigurationHandler(
                        performance.internal_performance_id, 
                        performance.source_website or 'unknown'
                    )
                    pos_config = config_handler.get_default_configuration()
                    pos_config.delete_enabled = True  # Enable delisting for disable operation
                    
                    # Create POS sync service and perform delist
                    pos_sync_service = POSSyncService(pos_config)
                    delist_result = pos_sync_service.delist_seat_packs(active_seat_packs_list)
                    
                    # Process results
                    api_delisted_count = delist_result.delisted_count
                    api_failed_count = delist_result.failed_count
                    
                    logger.info(f"POS API delist result for performance {performance.internal_performance_id}: "
                              f"{api_delisted_count} successful, {api_failed_count} failed")
                    
                    # Mark all seat packs as inactive regardless of API result
                    # (UI shows them as delisted even if API fails)
                    db_delisted_count = active_seat_packs.update(
                        is_active=False,
                        manually_delisted=True,
                        delist_reason='POS_DISABLED',
                        updated_at=timezone.now()
                    )
                    
                    if delist_result.errors:
                        error_details.extend([f"Performance {performance.internal_performance_id}: {error}" 
                                            for error in delist_result.errors])
                        total_errors += api_failed_count
                    
                except Exception as pos_error:
                    logger.error(f"POS API delist failed for performance {performance.internal_performance_id}: {pos_error}")
                    error_details.append(f"Performance {performance.internal_performance_id}: POS API error - {pos_error}")
                    
                    # Still mark as inactive in database even if POS API fails
                    db_delisted_count = active_seat_packs.update(
                        is_active=False,
                        manually_delisted=True,
                        delist_reason='POS_DISABLED_API_FAILED',
                        updated_at=timezone.now()
                    )
                    total_errors += len(active_seat_packs_list)
                
                if db_delisted_count > 0:
                    performances_affected += 1
                    total_delisted += db_delisted_count
                    logger.info(f"Database: Delisted {db_delisted_count} seat packs for performance {performance.internal_performance_id}")
                
            except Exception as e:
                logger.error(f"Error processing performance {performance.internal_performance_id}: {e}")
                error_details.append(f"Performance {performance.internal_performance_id}: Processing error - {e}")
                total_errors += 1
        
        # Determine overall success
        overall_success = total_errors == 0
        
        return {
            'success': overall_success,
            'data': {
                'performances_affected': performances_affected,
                'total_seat_packs_delisted': total_delisted,
                'total_errors': total_errors,
                'error_details': error_details,
                'event_id': event_id,
                'performance_id': performance_id
            },
            'message': f'POS scraping disabled. Delisted {total_delisted} seat packs across {performances_affected} performances' + 
                      (f' ({total_errors} errors)' if total_errors > 0 else '')
        }
        
    except Exception as e:
        logger.error(f"Error disabling POS scraping: {e}")
        return create_error_response(
            f'Failed to disable POS scraping: {str(e)}',
            'POS_SCRAPING_DISABLE_ERROR'
        )


@register_venue_task('update_venue_seat_structure')
def handle_update_venue_seat_structure(data):
    """Update venue seat structure (consecutive or odd_even)"""
    logger.info(f"handle_update_venue_seat_structure called with data: {data}")
    try:
        from scrapers.models import Venue
        
        # Extract parameters
        venue_id = data.get('venue_id')
        seat_structure = data.get('seat_structure')
        
        # Validate parameters
        if not venue_id:
            return create_error_response(
                'venue_id is required',
                'MISSING_VENUE_ID'
            )
        
        if not seat_structure:
            return create_error_response(
                'seat_structure is required',
                'MISSING_SEAT_STRUCTURE'
            )
        
        if seat_structure not in ['consecutive', 'odd_even']:
            return create_error_response(
                'seat_structure must be either "consecutive" or "odd_even"',
                'INVALID_SEAT_STRUCTURE'
            )
        
        # Try to find venue by internal_venue_id or venue_key
        try:
            venue = Venue.objects.get(internal_venue_id=venue_id)
        except Venue.DoesNotExist:
            try:
                # Fallback to venue_key if available
                venue = Venue.objects.get(venue_key=venue_id)
            except (Venue.DoesNotExist, AttributeError):
                return create_error_response(
                    f'Venue with ID {venue_id} not found',
                    'VENUE_NOT_FOUND'
                )
        
        # Store previous structure before updating for change detection
        # This enables automatic detection of structure changes that require seat pack synchronization
        venue.previous_seat_structure = venue.seat_structure
        venue.seat_structure = seat_structure
        venue.save(update_fields=['seat_structure', 'previous_seat_structure', 'updated_at'])
        
        # Handle seat pack synchronization if structure changed
        # When seat structure changes from odd_even ↔ consecutive, all active seat packs
        # for this venue must be delisted and regenerated with new structure logic
        from scrapers.core.seat_pack_structure_handler import SeatPackStructureHandler
        
        structure_handler = SeatPackStructureHandler()
        sync_result = structure_handler.handle_venue_structure_change(venue)
        
        logger.info(f"Updated seat structure for venue {venue_id} to {seat_structure}")
        
        # Return updated venue data with synchronization results
        venue_data = {
            'id': venue.internal_venue_id,
            'venue_key': getattr(venue, 'venue_key', None),
            'name': venue.name,
            'location': f"{venue.city}, {venue.state}" if venue.city and venue.state else 'Location TBD',
            'address': venue.address,
            'city': venue.city,
            'state': venue.state,
            'country': venue.country,
            'postal_code': venue.postal_code,
            'venue_timezone': venue.venue_timezone,
            'source_venue_id': venue.source_venue_id,
            'source_website': venue.source_website,
            'seat_structure': venue.seat_structure,
            'previous_seat_structure': venue.previous_seat_structure,
            'total_events': venue.events.filter(is_active=True).count(),
            'created_at': venue.created_at.isoformat(),
            'updated_at': venue.updated_at.isoformat() if venue.updated_at else None,
        }
        
        # Include synchronization results in response
        response_message = f'Venue seat structure updated to {seat_structure}'
        if sync_result['change_detected'] and sync_result['success']:
            response_message += f". Delisted {sync_result['packs_delisted']} seat packs across {sync_result['performances_affected']} performances."
        elif sync_result['change_detected'] and not sync_result['success']:
            response_message += f". Warning: Seat pack synchronization failed - {sync_result.get('error_message', 'Unknown error')}"
        
        return {
            'success': True,
            'data': venue_data,
            'message': response_message,
            'sync_result': sync_result  # Include full sync results for debugging/audit
        }
        
    except Exception as e:
        logger.error(f"Error updating venue seat structure: {e}\n{traceback.format_exc()}")
        return create_error_response(
            f'Failed to update venue seat structure: {str(e)}',
            'VENUE_UPDATE_ERROR'
        )


@register_venue_task('update_venue_seating_config')
def handle_update_venue_seating_config(data):
    """Update venue seating configuration (structure + markup)"""
    logger.info(f"handle_update_venue_seating_config called with data: {data}")
    try:
        from scrapers.models import Venue
        from django.utils import timezone
        
        # Extract parameters
        venue_id = data.get('venue_id')
        seat_structure = data.get('seat_structure')
        markup_type = data.get('markup_type')
        markup_value = data.get('markup_value')
        pos_enabled = data.get('pos_enabled')
        
        # Validate parameters
        if not venue_id:
            return create_error_response(
                'venue_id is required',
                'MISSING_VENUE_ID'
            )
        
        if not seat_structure:
            return create_error_response(
                'seat_structure is required',
                'MISSING_SEAT_STRUCTURE'
            )
        
        if seat_structure not in ['consecutive', 'odd_even']:
            return create_error_response(
                'seat_structure must be either "consecutive" or "odd_even"',
                'INVALID_SEAT_STRUCTURE'
            )
        
        if not markup_type:
            return create_error_response(
                'markup_type is required',
                'MISSING_MARKUP_TYPE'
            )
        
        if markup_type not in ['dollar', 'percentage']:
            return create_error_response(
                'markup_type must be either "dollar" or "percentage"',
                'INVALID_MARKUP_TYPE'
            )
        
        if markup_value is None:
            return create_error_response(
                'markup_value is required',
                'MISSING_MARKUP_VALUE'
            )
        
        # Validate markup value
        try:
            markup_value = float(markup_value)
            if markup_value < 0:
                return create_error_response(
                    'markup_value cannot be negative',
                    'INVALID_MARKUP_VALUE'
                )
        except (ValueError, TypeError):
            return create_error_response(
                'markup_value must be a valid number',
                'INVALID_MARKUP_VALUE'
            )
        
        # Validate pos_enabled parameter (optional)
        if pos_enabled is not None:
            if not isinstance(pos_enabled, bool):
                try:
                    # Try to convert string boolean values
                    if isinstance(pos_enabled, str):
                        pos_enabled = pos_enabled.lower() in ('true', '1', 'yes', 'on')
                    else:
                        pos_enabled = bool(pos_enabled)
                except (ValueError, TypeError):
                    return create_error_response(
                        'pos_enabled must be a boolean value',
                        'INVALID_POS_ENABLED'
                    )
        
        # Try to find venue by internal_venue_id or venue_key
        try:
            venue = Venue.objects.get(internal_venue_id=venue_id)
        except Venue.DoesNotExist:
            try:
                # Fallback to venue_key if available
                venue = Venue.objects.get(venue_key=venue_id)
            except (Venue.DoesNotExist, AttributeError):
                return create_error_response(
                    f'Venue with ID {venue_id} not found',
                    'VENUE_NOT_FOUND'
                )
        
        # Store previous structure before updating for change detection
        venue.previous_seat_structure = venue.seat_structure
        venue.seat_structure = seat_structure
        venue.price_markup_type = markup_type
        venue.price_markup_value = markup_value
        venue.price_markup_updated_at = timezone.now()
        
        # Handle POS configuration if provided
        pos_status_changed = False
        if pos_enabled is not None:
            old_pos_enabled = venue.pos_enabled
            venue.pos_enabled = pos_enabled
            if pos_enabled != old_pos_enabled:
                venue.pos_enabled_at = timezone.now()
                pos_status_changed = True
                logger.info(f"POS status changed for venue {venue_id}: {old_pos_enabled} -> {pos_enabled}")
        
        # Save venue with all updates
        update_fields = [
            'seat_structure', 
            'previous_seat_structure', 
            'price_markup_type',
            'price_markup_value',
            'price_markup_updated_at',
            'updated_at'
        ]
        
        if pos_enabled is not None:
            update_fields.extend(['pos_enabled', 'pos_enabled_at'])
            
        venue.save(update_fields=update_fields)
        
        # Handle seat pack synchronization if structure changed
        structure_changed = venue.previous_seat_structure != seat_structure
        sync_result = {'change_detected': False, 'success': True}
        
        if structure_changed:
            from scrapers.core.seat_pack_structure_handler import SeatPackStructureHandler
            structure_handler = SeatPackStructureHandler()
            sync_result = structure_handler.handle_venue_structure_change(venue)
        
        # Handle POS performance enablement/disablement if POS status changed
        pos_result = {'enabled': False, 'performances_enabled': 0, 'scrape_jobs_created': 0, 'errors': []}
        if pos_status_changed and pos_enabled:
            try:
                pos_result = enable_venue_pos_performances(venue, data.get('user_id', 1))
                logger.info(f"POS enabled for venue {venue_id}: {pos_result['performances_enabled']} performances, {pos_result['scrape_jobs_created']} scrape jobs")
            except Exception as e:
                error_msg = f"Failed to enable POS for venue performances: {str(e)}"
                logger.error(error_msg)
                pos_result['errors'].append(error_msg)
        elif pos_status_changed and not pos_enabled:
            try:
                pos_result = disable_venue_pos_performances(venue, data.get('user_id', 1))
                logger.info(f"POS disabled for venue {venue_id}: {pos_result['performances_disabled']} performances disabled, {pos_result['performances_delisted']} performances delisted")
            except Exception as e:
                error_msg = f"Failed to disable POS for venue performances: {str(e)}"
                logger.error(error_msg)
                pos_result['errors'].append(error_msg)
        
        logger.info(f"Updated seating configuration for venue {venue_id}")
        
        # Return updated venue configuration data
        config_data = {
            'venue_id': venue.internal_venue_id,
            'seat_structure': venue.seat_structure,
            'markup_type': venue.price_markup_type,
            'markup_value': float(venue.price_markup_value) if venue.price_markup_value else None,
            'pos_enabled': venue.pos_enabled,
            'pos_enabled_at': venue.pos_enabled_at.isoformat() if venue.pos_enabled_at else None,
            'updated_at': venue.price_markup_updated_at.isoformat() if venue.price_markup_updated_at else None,
        }
        
        # Include synchronization and POS results in response
        response_message = f'Venue seating configuration updated successfully'
        if sync_result['change_detected'] and sync_result['success']:
            response_message += f". Delisted {sync_result['packs_delisted']} seat packs across {sync_result['performances_affected']} performances due to structure change."
        elif sync_result['change_detected'] and not sync_result['success']:
            response_message += f". Warning: Structure changed but seat pack synchronization failed: {sync_result.get('error', 'Unknown error')}"
        
        # Add POS results to response message
        if pos_result.get('enabled') and pos_result.get('performances_enabled', 0) > 0:
            response_message += f". POS enabled for {pos_result['performances_enabled']} performances with {pos_result['scrape_jobs_created']} scrape jobs created."
        elif pos_result.get('disabled') and pos_result.get('performances_disabled', 0) > 0:
            response_message += f". POS disabled for {pos_result['performances_disabled']} performances with seat packs delisted for {pos_result['performances_delisted']} performances."
        if pos_result.get('errors'):
            operation_type = "enablement" if pos_result.get('enabled') else "disablement"
            response_message += f" POS {operation_type} warnings: {'; '.join(pos_result['errors'])}"
        
        # Ensure sync_result is JSON serializable
        serializable_sync_result = sync_result.copy() if sync_result else {}
        for key, value in serializable_sync_result.items():
            if hasattr(value, 'isoformat'):  # datetime object
                serializable_sync_result[key] = value.isoformat()
        
        return {
            'success': True,
            'data': config_data,
            'message': response_message,
            'sync_result': serializable_sync_result,
            'pos_result': pos_result
        }
        
    except Exception as e:
        logger.error(f"Error updating venue seating configuration: {e}\n{traceback.format_exc()}")
        return create_error_response(
            f'Failed to update venue seating configuration: {str(e)}',
            'VENUE_SEATING_CONFIG_UPDATE_ERROR'
        )


# Helper functions for POS performance enablement
def get_venue_non_expired_performances(venue):
    """Get all non-expired performances for a venue"""
    from scrapers.models import Performance
    from django.utils import timezone
    
    return Performance.objects.filter(
        venue_id=venue,
        performance_datetime_utc__gt=timezone.now(),
        is_active=True
    ).select_related('event_id')


def enable_venue_pos_performances(venue, user_id=1):
    """Enable POS for all venue performances and trigger scraping"""
    from scrapers.models import ScrapeJob
    from django.utils import timezone
    import uuid
    
    result = {
        'enabled': True,
        'performances_enabled': 0,
        'scrape_jobs_created': 0,
        'errors': []
    }
    
    try:
        # Get all non-expired performances for this venue
        performances = get_venue_non_expired_performances(venue)
        logger.info(f"Found {performances.count()} non-expired performances for venue {venue.internal_venue_id}")
        
        if performances.count() == 0:
            result['errors'].append("No non-expired performances found for this venue")
            return result
        
        # Enable POS for all performances
        updated_count = performances.update(
            pos_enabled=True,
            pos_enabled_at=timezone.now(),
            pos_disabled_at=None,
            updated_at=timezone.now()
        )
        result['performances_enabled'] = updated_count
        
        # Create scrape jobs for each performance
        scrape_jobs_created = create_performance_scrape_jobs(performances, user_id)
        result['scrape_jobs_created'] = scrape_jobs_created
        
        logger.info(f"Successfully enabled POS for {updated_count} performances and created {scrape_jobs_created} scrape jobs")
        
    except Exception as e:
        error_msg = f"Error enabling POS for venue performances: {str(e)}"
        logger.error(error_msg)
        result['errors'].append(error_msg)
    
    return result


def disable_venue_pos_performances(venue, user_id=1):
    """Disable POS for all venue performances and delist seat packs"""
    from django.utils import timezone
    
    result = {
        'disabled': True,
        'performances_disabled': 0,
        'performances_delisted': 0,
        'errors': []
    }
    
    try:
        # Get all performances for this venue (both expired and non-expired)
        # We want to disable POS for all performances, not just non-expired ones
        from scrapers.models import Performance
        performances = Performance.objects.filter(
            venue_id=venue,
            is_active=True
        ).select_related('event_id')
        
        logger.info(f"Found {performances.count()} active performances for venue {venue.internal_venue_id}")
        
        if performances.count() == 0:
            result['errors'].append("No active performances found for this venue")
            return result
        
        # Disable POS for all performances
        updated_count = performances.update(
            pos_enabled=False,
            pos_disabled_at=timezone.now(),
            updated_at=timezone.now()
        )
        result['performances_disabled'] = updated_count
        
        # Delist seat packs for each performance
        delisted_count = 0
        for performance in performances:
            try:
                # Call our handle_delist_performance_packs function for each performance
                delist_result = handle_delist_performance_packs({
                    'performance_id': performance.internal_performance_id,
                    'user_id': user_id
                })
                
                if delist_result.get('success'):
                    delisted_count += 1
                    logger.info(f"Successfully delisted seat packs for performance {performance.internal_performance_id}")
                else:
                    error_msg = f"Failed to delist seat packs for performance {performance.internal_performance_id}: {delist_result.get('message', 'Unknown error')}"
                    logger.warning(error_msg)
                    result['errors'].append(error_msg)
                    
            except Exception as e:
                error_msg = f"Error delisting seat packs for performance {performance.internal_performance_id}: {str(e)}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
        
        result['performances_delisted'] = delisted_count
        
        logger.info(f"Successfully disabled POS for {updated_count} performances and delisted seat packs for {delisted_count} performances")
        
    except Exception as e:
        error_msg = f"Error disabling POS for venue performances: {str(e)}"
        logger.error(error_msg)
        result['errors'].append(error_msg)
    
    return result


def create_performance_scrape_jobs(performances, user_id=1):
    """Create scrape jobs and send to RabbitMQ"""
    from scrapers.models import ScrapeJob
    import uuid
    
    jobs_created = 0
    
    try:
        for performance in performances:
            try:
                # Create scrape job in database using Django ScrapeJob model structure
                scrape_job = ScrapeJob.objects.create(
                    performance_id=performance,
                    scraper_name='venue_pos_enablement',
                    source_website=performance.event_id.source_website,
                    scraper_config={
                        'user_id': user_id,
                        'pos_enabled': True,
                        'triggered_by': 'venue_pos_enablement'
                    }
                )
                
                # Send scrape request via RabbitMQ
                scrape_message = {
                    'action': 'scrape_url',
                    'scrapeJobId': scrape_job.scrape_job_key,
                    'url': performance.event_id.url or f"https://example.com/event/{performance.event_id.internal_event_id}",
                    'userId': user_id,
                    'performanceId': performance.internal_performance_id,
                    'scheduled': True,
                    'pos': True
                }
                
                # Send to RabbitMQ (implement RabbitMQ sender here)
                send_scrape_message_to_rabbitmq(scrape_message)
                
                jobs_created += 1
                logger.info(f"Created scrape job {scrape_job.scrape_job_key} for performance {performance.internal_performance_id}")
                
            except Exception as e:
                logger.error(f"Failed to create scrape job for performance {performance.internal_performance_id}: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Error creating performance scrape jobs: {str(e)}")
    
    return jobs_created


def send_scrape_message_to_rabbitmq(message):
    """Send scrape message to RabbitMQ django_to_nest queue"""
    try:
        from consumer.rabbitmq_producer import producer
        import pika
        
        # Format message for NestJS scrape client pattern
        formatted_message = {
            'pattern': 'scrape_url',
            'data': {
                'scrapeJobId': message.get('scrapeJobId'),
                'url': message.get('url'),
                'userId': message.get('userId'),
                'performanceId': message.get('performanceId'),
                'scheduled': message.get('scheduled', True),
                'pos': message.get('pos', True)
            }
        }
        
        # Ensure RabbitMQ connection is established
        if not producer.connection or producer.connection.is_closed:
            if not producer.connect():
                logger.error("Failed to connect to RabbitMQ")
                return False
        
        # Send using the producer's channel to nest_to_django queue (scrape requests)
        producer.channel.basic_publish(
            exchange='',
            routing_key='nest_to_django',
            body=producer.safe_json_dumps(formatted_message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json'
            )
        )
        
        logger.info(f"Sent scrape request to RabbitMQ for job: {message.get('scrapeJobId')}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send scrape message to RabbitMQ: {str(e)}")
        raise


@register_venue_task('push_performance_to_pos')
def handle_push_performance_to_pos(data):
    """Push seat packs to POS for a performance after initial scrape completion"""
    try:
        from scrapers.models import Performance, SeatPack
        from django.utils import timezone
        
        # Extract parameters
        performance_id = data.get('performance_id')
        scrape_job_id = data.get('scrape_job_id')
        trigger_reason = data.get('trigger_reason', 'unknown')
        
        # Validate parameters
        if not performance_id:
            return create_error_response(
                'performance_id is required',
                'MISSING_PERFORMANCE_ID'
            )
        
        # Find performance
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id, is_active=True)
        except Performance.DoesNotExist:
            return create_error_response(
                f'Performance with ID {performance_id} not found',
                'PERFORMANCE_NOT_FOUND'
            )
        
        # Find active seat packs for this performance 
        active_seat_packs = SeatPack.objects.filter(
            scrape_job_key__performance_id=performance,
            is_active=True
        ).select_related('scrape_job_key')
        
        active_seat_packs_list = list(active_seat_packs)
        
        if not active_seat_packs_list:
            logger.info(f"No active seat packs found for performance {performance_id}")
            return {
                'success': True,
                'data': {
                    'performance_id': performance_id,
                    'total_seat_packs_pushed': 0,
                    'message': 'No seat packs to push'
                },
                'message': f'No seat packs found to push for performance {performance_id}'
            }
        
        # Use POS sync service to push to external APIs
        from ..scrapers.core.pos_sync_service import POSSyncService
        from ..scrapers.core.pos_config_handler import POSConfiguration, POSConfigurationHandler
        
        try:
            # Get POS configuration for this performance
            config_handler = POSConfigurationHandler(
                performance.internal_performance_id, 
                performance.source_website or 'unknown'
            )
            pos_config = config_handler.get_default_configuration()
            pos_config.create_enabled = True  # Enable pushing for this operation
            
            # Create POS sync service and perform push
            pos_sync_service = POSSyncService(pos_config)
            push_result = pos_sync_service.push_seat_packs(active_seat_packs_list)
            
            # Process results
            api_pushed_count = push_result.pushed_count
            api_failed_count = push_result.failed_count
            
            logger.info(f"POS API push result for performance {performance_id}: "
                      f"{api_pushed_count} successful, {api_failed_count} failed. "
                      f"Trigger: {trigger_reason}")
            
            # Return comprehensive results
            success = api_failed_count == 0
            
            return {
                'success': success,
                'data': {
                    'performance_id': performance_id,
                    'scrape_job_id': scrape_job_id,
                    'trigger_reason': trigger_reason,
                    'total_seat_packs_pushed': api_pushed_count,
                    'total_seat_packs_failed': api_failed_count,
                    'total_seat_packs_processed': len(active_seat_packs_list),
                    'push_result': push_result.to_dict(),
                    'timestamp': timezone.now().isoformat()
                },
                'message': f'POS push completed for performance {performance_id}. ' +
                          f'Pushed {api_pushed_count} seat packs' +
                          (f', {api_failed_count} failed' if api_failed_count > 0 else '')
            }
            
        except Exception as push_error:
            logger.error(f"Error during POS push for performance {performance_id}: {push_error}")
            return create_error_response(
                f'POS push failed for performance {performance_id}: {str(push_error)}',
                'POS_PUSH_OPERATION_ERROR'
            )
        
    except Exception as e:
        logger.error(f"Error pushing performance to POS: {e}")
        return create_error_response(
            f'Failed to push performance to POS: {str(e)}',
            'POS_PUSH_ERROR'
        )


@register_venue_task('disable_venue_pos')
def handle_disable_venue_pos(data):
    """Disable POS for all performances in a venue (delist all seat packs)"""
    try:
        from scrapers.models import Venue, Performance, SeatPack
        from django.utils import timezone
        
        # Extract parameters
        venue_id = data.get('venue_id')
        
        # Validate parameters
        if not venue_id:
            return create_error_response(
                'venue_id is required',
                'MISSING_VENUE_ID'
            )
        
        # Find venue
        try:
            venue = Venue.objects.get(internal_venue_id=venue_id, is_active=True)
        except Venue.DoesNotExist:
            return create_error_response(
                f'Venue with ID {venue_id} not found',
                'VENUE_NOT_FOUND'
            )
        
        # Find all performances for this venue
        performances = Performance.objects.filter(
            venue_id=venue,
            is_active=True
        )
        
        if not performances.exists():
            return {
                'success': True,
                'data': {
                    'venue_id': venue_id,
                    'venue_name': venue.name,
                    'performances_affected': 0,
                    'total_seat_packs_delisted': 0
                },
                'message': f'No performances found for venue {venue.name}'
            }
        
        # Process each performance and delist its seat packs
        total_delisted = 0
        performances_affected = 0
        total_errors = 0
        error_details = []
        
        for performance in performances:
            try:
                # Find all active seat packs for this performance
                active_seat_packs = SeatPack.objects.filter(
                    scrape_job_key__performance_id=performance,
                    is_active=True
                ).select_related('scrape_job_key')
                
                active_seat_packs_list = list(active_seat_packs)
                
                if not active_seat_packs_list:
                    logger.info(f"No active seat packs found for performance {performance.internal_performance_id}")
                    continue
                
                # Use POS sync service to properly delist from external APIs
                from ..scrapers.core.pos_sync_service import POSSyncService
                from ..scrapers.core.pos_config_handler import POSConfiguration, POSConfigurationHandler
                
                try:
                    # Get POS configuration for this performance
                    config_handler = POSConfigurationHandler(
                        performance.internal_performance_id, 
                        performance.source_website or 'unknown'
                    )
                    pos_config = config_handler.get_default_configuration()
                    pos_config.delete_enabled = True  # Enable delisting for disable operation
                    
                    # Create POS sync service and perform delist
                    pos_sync_service = POSSyncService(pos_config)
                    delist_result = pos_sync_service.delist_seat_packs(active_seat_packs_list)
                    
                    # Process results
                    api_delisted_count = delist_result.delisted_count
                    api_failed_count = delist_result.failed_count
                    
                    logger.info(f"POS API delist result for performance {performance.internal_performance_id}: "
                              f"{api_delisted_count} successful, {api_failed_count} failed")
                    
                    # Mark all seat packs as inactive regardless of API result
                    # (UI shows them as delisted even if API fails)
                    db_delisted_count = active_seat_packs.update(
                        is_active=False,
                        manually_delisted=True,
                        delist_reason='VENUE_POS_DISABLED',
                        updated_at=timezone.now()
                    )
                    
                    total_delisted += db_delisted_count
                    performances_affected += 1
                    
                    if api_failed_count > 0:
                        total_errors += api_failed_count
                        error_details.append({
                            'performance_id': performance.internal_performance_id,
                            'failed_count': api_failed_count,
                            'errors': delist_result.errors
                        })
                    
                except Exception as api_error:
                    logger.error(f"POS API error for performance {performance.internal_performance_id}: {api_error}")
                    total_errors += 1
                    error_details.append({
                        'performance_id': performance.internal_performance_id,
                        'error': str(api_error)
                    })
                    
                    # Still mark seat packs as inactive in database
                    db_delisted_count = active_seat_packs.update(
                        is_active=False,
                        manually_delisted=True,
                        delist_reason='VENUE_POS_DISABLED',
                        updated_at=timezone.now()
                    )
                    
                    total_delisted += db_delisted_count
                    performances_affected += 1
                
            except Exception as perf_error:
                logger.error(f"Error processing performance {performance.internal_performance_id}: {perf_error}")
                total_errors += 1
                error_details.append({
                    'performance_id': performance.internal_performance_id,
                    'error': str(perf_error)
                })
        
        # Return comprehensive results
        overall_success = total_errors == 0
        
        return {
            'success': overall_success,
            'data': {
                'venue_id': venue_id,
                'venue_name': venue.name,
                'performances_affected': performances_affected,
                'total_seat_packs_delisted': total_delisted,
                'total_errors': total_errors,
                'error_details': error_details,
                'timestamp': timezone.now().isoformat()
            },
            'message': f'Venue POS disabled for {venue.name}. Delisted {total_delisted} seat packs across {performances_affected} performances' + 
                      (f' ({total_errors} errors)' if total_errors > 0 else '')
        }
        
    except Exception as e:
        logger.error(f"Error disabling venue POS: {e}")
        return create_error_response(
            f'Failed to disable venue POS: {str(e)}',
            'VENUE_POS_DISABLE_ERROR'
        )


@register_venue_task('get_scrapers')
def handle_get_scrapers(data):
    """Get all available scrapers with their public information"""
    try:
        from scrapers.models import ScraperDefinition
        
        logger.info("Getting all scrapers from database")
        
        # Get all active scrapers, ordered by priority and name
        scrapers = ScraperDefinition.objects.filter(
            is_enabled=True
        ).order_by('-priority', 'display_name')
        
        # Transform scraper data for frontend consumption
        scraper_list = []
        for scraper in scrapers:
            # Generate features list based on scraper capabilities
            features = []
            if scraper.optimization_enabled:
                features.append('Performance optimization')
            if scraper.captcha_required:
                features.append('Captcha handling')
            if scraper.use_proxy:
                features.append('Proxy rotation')
            if scraper.enable_screenshots:
                features.append('Debug screenshots')
            
            # Add default features if none are specified
            if not features:
                features = [
                    'Event data extraction',
                    'Seat availability monitoring',
                    'Real-time pricing tracking',
                    'Performance scheduling'
                ]
            
            scraper_data = {
                'internal_id': scraper.internal_id,
                'name': scraper.name,
                'display_name': scraper.display_name,
                'description': scraper.description or 'Professional venue scraper for ticket and event data extraction.',
                'target_website': scraper.target_website,
                'target_domains': scraper.target_domains or [scraper.target_website],
                'status': scraper.status,
                'is_enabled': scraper.is_enabled,
                'priority': scraper.priority,
                'features': features,
                'last_run_at': scraper.last_run_at.isoformat() if scraper.last_run_at else None,
                'last_success_at': scraper.last_success_at.isoformat() if scraper.last_success_at else None,
                'success_rate': scraper.success_rate,
                'created_at': scraper.created_at.isoformat(),
                'updated_at': scraper.updated_at.isoformat(),
            }
            scraper_list.append(scraper_data)
        
        logger.info(f"Retrieved {len(scraper_list)} scrapers")
        
        return {
            'success': True,
            'data': scraper_list,
            'message': f'Retrieved {len(scraper_list)} scrapers successfully'
        }
        
    except Exception as e:
        logger.error(f"Error getting scrapers: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return create_error_response(
            f'Failed to retrieve scrapers: {str(e)}',
            'GET_SCRAPERS_ERROR'
        )