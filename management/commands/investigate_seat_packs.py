#!/usr/bin/env python3
"""
Django management command to investigate seat pack data for a specific performance.
Usage: python manage.py investigate_seat_packs --performance-id <performance_id>
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Q, Min, Max, Avg
from scrapers.models import SeatPack, Performance, ScrapeJob, Zone
import json
from datetime import datetime


class Command(BaseCommand):
    help = 'Investigate seat pack data for a specific performance'

    def add_arguments(self, parser):
        parser.add_argument(
            '--performance-id',
            type=str,
            required=True,
            help='Performance ID to investigate (e.g., bsf_perf_825BAA3F-090B-4ECB-BBCC-6426DC8B8168)'
        )
        parser.add_argument(
            '--detailed',
            action='store_true',
            help='Show detailed breakdown of each pack size'
        )

    def handle(self, *args, **options):
        performance_id = options['performance_id']
        detailed = options.get('detailed', False)
        
        self.stdout.write(
            self.style.SUCCESS(f'\n🔍 Investigating Seat Pack Data for Performance: {performance_id}')
        )
        self.stdout.write('=' * 80)
        
        try:
            # Find the performance
            performance = self.find_performance(performance_id)
            if not performance:
                raise CommandError(f'Performance not found: {performance_id}')
            
            # Get all zones for this performance
            zones = self.get_performance_zones(performance)
            
            # Analyze seat packs
            self.analyze_seat_packs(performance, zones, detailed)
            
        except Exception as e:
            raise CommandError(f'Error during investigation: {str(e)}')

    def find_performance(self, performance_id):
        """Find performance by internal_performance_id"""
        try:
            # Try internal_performance_id
            performance = Performance.objects.filter(
                internal_performance_id=performance_id
            ).select_related('event_id', 'venue_id').first()
            
            if performance:
                self.stdout.write(f'📍 Found Performance:')
                self.stdout.write(f'   - Internal ID: {performance.internal_performance_id}')
                self.stdout.write(f'   - Event: {performance.event_id.name if performance.event_id else "N/A"}')
                self.stdout.write(f'   - Venue: {performance.venue_id.name if performance.venue_id else "N/A"}')
                self.stdout.write(f'   - DateTime: {performance.performance_datetime_utc}')
                
            return performance
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error finding performance: {str(e)}')
            )
            return None

    def get_performance_zones(self, performance):
        """Get all zones for this performance"""
        active_zones = Zone.objects.filter(
            performance_id=performance,
            is_active=True
        )
        
        all_zones = Zone.objects.filter(
            performance_id=performance
        )
        
        inactive_zones = all_zones.filter(is_active=False)
        
        self.stdout.write(f'\n📍 Zone Summary:')
        self.stdout.write(f'   - Active zones: {active_zones.count()}')
        self.stdout.write(f'   - Inactive zones: {inactive_zones.count()}')
        self.stdout.write(f'   - Total zones: {all_zones.count()}')
        
        # Return all zones for analysis (both active and inactive)
        return all_zones

    def analyze_seat_packs(self, performance, zones, detailed=False):
        """Analyze seat pack data for the performance"""
        
        # Get all seat packs for these zones
        all_packs = SeatPack.objects.filter(
            zone_id__in=zones
        ).select_related('zone_id', 'scrape_job_key')
        
        self.stdout.write(f'\n📊 SEAT PACK ANALYSIS')
        self.stdout.write('-' * 50)
        
        # Overall statistics
        total_packs = all_packs.count()
        active_packs = all_packs.filter(is_active=True).count()
        inactive_packs = all_packs.filter(is_active=False).count()
        
        self.stdout.write(f'Total Seat Packs: {total_packs}')
        self.stdout.write(f'  • Active: {active_packs}')
        self.stdout.write(f'  • Inactive: {inactive_packs}')
        
        # Pack size analysis
        self.stdout.write(f'\n📏 PACK SIZE DISTRIBUTION')
        self.stdout.write('-' * 30)
        
        pack_sizes = all_packs.values('pack_size', 'is_active').annotate(
            count=Count('internal_pack_id')
        ).order_by('pack_size', 'is_active')
        
        size_summary = {}
        for item in pack_sizes:
            size = item['pack_size']
            status = 'Active' if item['is_active'] else 'Inactive'
            count = item['count']
            
            if size not in size_summary:
                size_summary[size] = {'Active': 0, 'Inactive': 0}
            size_summary[size][status] = count
        
        for size in sorted(size_summary.keys()):
            active_count = size_summary[size]['Active']
            inactive_count = size_summary[size]['Inactive']
            total_count = active_count + inactive_count
            
            status_indicator = '⚠️' if size == 1 else '✅'
            self.stdout.write(
                f'{status_indicator} Pack Size {size}: {total_count} total '
                f'(Active: {active_count}, Inactive: {inactive_count})'
            )
        
        # Focus on pack_size=1 if they exist
        single_packs = all_packs.filter(pack_size=1)
        if single_packs.exists():
            self.stdout.write(f'\n⚠️  SINGLE SEAT PACKS (pack_size=1) - INVESTIGATION')
            self.stdout.write('-' * 55)
            self.analyze_single_seat_packs(single_packs)
        
        # Creation event analysis
        self.stdout.write(f'\n🔨 CREATION EVENT ANALYSIS')
        self.stdout.write('-' * 35)
        
        creation_events = all_packs.exclude(
            creation_event__isnull=True
        ).values('creation_event', 'is_active').annotate(
            count=Count('internal_pack_id')
        ).order_by('creation_event', 'is_active')
        
        event_summary = {}
        for item in creation_events:
            event = item['creation_event'] or 'None'
            status = 'Active' if item['is_active'] else 'Inactive'
            count = item['count']
            
            if event not in event_summary:
                event_summary[event] = {'Active': 0, 'Inactive': 0}
            event_summary[event][status] = count
        
        for event in sorted(event_summary.keys()):
            active_count = event_summary[event]['Active']
            inactive_count = event_summary[event]['Inactive']
            total_count = active_count + inactive_count
            
            self.stdout.write(
                f'📝 {event}: {total_count} total '
                f'(Active: {active_count}, Inactive: {inactive_count})'
            )
        
        # Delist reason analysis for inactive packs
        if inactive_packs > 0:
            self.stdout.write(f'\n❌ DELIST REASON ANALYSIS (Inactive Packs)')
            self.stdout.write('-' * 45)
            
            delist_reasons = all_packs.filter(
                is_active=False
            ).exclude(
                delist_reason__isnull=True
            ).values('delist_reason').annotate(
                count=Count('internal_pack_id')
            ).order_by('-count')
            
            for item in delist_reasons:
                reason = item['delist_reason'] or 'None'
                count = item['count']
                self.stdout.write(f'🗑️  {reason}: {count} packs')
        
        # Recent scrape job analysis
        self.stdout.write(f'\n🕐 RECENT SCRAPE JOBS')
        self.stdout.write('-' * 25)
        
        recent_scrapes = ScrapeJob.objects.filter(
            performance_id=performance,
            scrape_success=True
        ).order_by('-scraped_at_utc')[:5]
        
        for scrape in recent_scrapes:
            pack_count = all_packs.filter(scrape_job_key=scrape).count()
            active_count = all_packs.filter(scrape_job_key=scrape, is_active=True).count()
            
            self.stdout.write(
                f'📅 {scrape.scraped_at_utc.strftime("%Y-%m-%d %H:%M:%S UTC")}: '
                f'{pack_count} packs ({active_count} active)'
            )
        
        # Detailed breakdown if requested
        if detailed:
            self.detailed_analysis(all_packs)

    def analyze_single_seat_packs(self, single_packs):
        """Detailed analysis of single seat packs"""
        
        total_single = single_packs.count()
        active_single = single_packs.filter(is_active=True).count()
        inactive_single = single_packs.filter(is_active=False).count()
        
        self.stdout.write(f'Total Single Packs: {total_single}')
        self.stdout.write(f'  • Active: {active_single}')
        self.stdout.write(f'  • Inactive: {inactive_single}')
        
        # Creation event breakdown for single packs
        creation_events = single_packs.values('creation_event').annotate(
            count=Count('internal_pack_id')
        ).order_by('-count')
        
        self.stdout.write(f'\nCreation Events for Single Packs:')
        for item in creation_events:
            event = item['creation_event'] or 'None'
            count = item['count']
            self.stdout.write(f'  • {event}: {count}')
        
        # Show some examples
        self.stdout.write(f'\nExample Single Packs:')
        examples = single_packs.select_related('zone_id', 'scrape_job_key')[:5]
        for pack in examples:
            status = '✅ Active' if pack.is_active else '❌ Inactive'
            self.stdout.write(
                f'  • Pack {pack.internal_pack_id[:8]}... - '
                f'Zone: {pack.zone_id.name if pack.zone_id else "N/A"} - '
                f'Row: {pack.row_label} - Seat: {pack.start_seat_number} - '
                f'{status} - Created: {pack.creation_event or "N/A"}'
            )

    def detailed_analysis(self, all_packs):
        """Provide detailed breakdown by zone and other dimensions"""
        
        self.stdout.write(f'\n📋 DETAILED ANALYSIS')
        self.stdout.write('-' * 25)
        
        # By zone analysis
        zone_stats = all_packs.values(
            'zone_id__name', 'zone_id__internal_zone_id'
        ).annotate(
            total_packs=Count('internal_pack_id'),
            active_packs=Count('internal_pack_id', filter=Q(is_active=True)),
            min_size=Min('pack_size'),
            max_size=Max('pack_size'),
            avg_size=Avg('pack_size')
        ).order_by('-total_packs')
        
        self.stdout.write(f'\n🎯 BY ZONE:')
        for zone in zone_stats:
            zone_name = zone['zone_id__name'] or 'Unknown'
            total = zone['total_packs']
            active = zone['active_packs']
            min_size = zone['min_size']
            max_size = zone['max_size']
            avg_size = round(zone['avg_size'], 1) if zone['avg_size'] else 0
            
            self.stdout.write(
                f'  📍 {zone_name}: {total} packs ({active} active) - '
                f'Sizes: {min_size}-{max_size} (avg: {avg_size})'
            )
        
        self.stdout.write(f'\n✅ Investigation Complete!')
        self.stdout.write('=' * 80)