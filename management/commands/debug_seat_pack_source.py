#!/usr/bin/env python3
"""
Debug script to investigate the source of seat pack data that appears on frontend
but not found in Django database for a specific performance.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Q
from scrapers.models import SeatPack, Performance, ScrapeJob, Zone
import json


class Command(BaseCommand):
    help = 'Debug the source of seat pack data discrepancy'

    def add_arguments(self, parser):
        parser.add_argument(
            '--performance-id',
            type=str,
            required=True,
            help='Performance ID to debug'
        )

    def handle(self, *args, **options):
        performance_id = options['performance_id']
        
        self.stdout.write(
            self.style.SUCCESS(f'\n🔍 DEBUGGING SEAT PACK DATA SOURCE')
        )
        self.stdout.write('=' * 60)
        self.stdout.write(f'Performance ID: {performance_id}')
        
        # 1. Check if performance exists at all
        self.check_performance_existence(performance_id)
        
        # 2. Check for any seat packs that might be associated indirectly
        self.check_indirect_seat_packs(performance_id)
        
        # 3. Check scrape jobs for this performance
        self.check_scrape_jobs(performance_id)
        
        # 4. Check for potential Redis/cache data
        self.check_redis_possibility(performance_id)
        
        # 5. Check if data might be coming from a different performance ID
        self.check_similar_performances(performance_id)

    def check_performance_existence(self, performance_id):
        """Check if performance exists and get basic info"""
        self.stdout.write(f'\n1️⃣ PERFORMANCE EXISTENCE CHECK')
        self.stdout.write('-' * 35)
        
        try:
            performance = Performance.objects.filter(
                internal_performance_id=performance_id
            ).select_related('event_id', 'venue_id').first()
            
            if performance:
                self.stdout.write(f'✅ Performance found in Django DB')
                self.stdout.write(f'   - ID: {performance.internal_performance_id}')
                self.stdout.write(f'   - Event: {performance.event_id.name}')
                self.stdout.write(f'   - Venue: {performance.venue_id.name}')
                self.stdout.write(f'   - DateTime: {performance.performance_datetime_utc}')
                self.stdout.write(f'   - Active: {performance.is_active}')
                
                # Check zones for this performance
                zones = Zone.objects.filter(performance_id=performance)
                self.stdout.write(f'   - Total zones: {zones.count()}')
                
                if zones.exists():
                    active_zones = zones.filter(is_active=True).count()
                    inactive_zones = zones.filter(is_active=False).count()
                    self.stdout.write(f'     • Active: {active_zones}')
                    self.stdout.write(f'     • Inactive: {inactive_zones}')
                    
                    # Show zone details
                    for zone in zones[:5]:  # Show first 5 zones
                        self.stdout.write(f'     • Zone: {zone.name} ({zone.internal_zone_id}) - Active: {zone.is_active}')
                
                return performance
            else:
                self.stdout.write(f'❌ Performance NOT found in Django DB')
                return None
                
        except Exception as e:
            self.stdout.write(f'❌ Error checking performance: {str(e)}')
            return None

    def check_indirect_seat_packs(self, performance_id):
        """Check for seat packs that might be associated indirectly"""
        self.stdout.write(f'\n2️⃣ INDIRECT SEAT PACK CHECK')
        self.stdout.write('-' * 30)
        
        try:
            # Check if there are any seat packs with this performance_id in different ways
            
            # Method 1: Check seat packs by scrape job performance_id
            scrape_jobs = ScrapeJob.objects.filter(performance_id=performance_id)
            self.stdout.write(f'Scrape jobs for this performance: {scrape_jobs.count()}')
            
            total_packs_from_scrapes = 0
            for scrape in scrape_jobs[:10]:  # Check first 10 scrape jobs
                pack_count = SeatPack.objects.filter(scrape_job_key=scrape).count()
                total_packs_from_scrapes += pack_count
                if pack_count > 0:
                    self.stdout.write(f'  📦 Scrape {scrape.id} ({scrape.scraped_at_utc}): {pack_count} seat packs')
            
            self.stdout.write(f'Total seat packs from scrape jobs: {total_packs_from_scrapes}')
            
            # Method 2: Search for seat packs containing this performance_id in pack_id
            packs_with_perf_id = SeatPack.objects.filter(
                internal_pack_id__icontains=performance_id[:20]  # Check first 20 chars
            )
            self.stdout.write(f'Seat packs with performance ID in pack_id: {packs_with_perf_id.count()}')
            
            if packs_with_perf_id.exists():
                for pack in packs_with_perf_id[:5]:
                    self.stdout.write(f'  📦 Pack: {pack.internal_pack_id[:50]}... - Active: {pack.is_active}')
            
        except Exception as e:
            self.stdout.write(f'❌ Error checking indirect seat packs: {str(e)}')

    def check_scrape_jobs(self, performance_id):
        """Check scrape jobs for this performance"""
        self.stdout.write(f'\n3️⃣ SCRAPE JOBS ANALYSIS')
        self.stdout.write('-' * 25)
        
        try:
            # Find scrape jobs for this performance
            scrape_jobs = ScrapeJob.objects.filter(
                performance_id=performance_id
            ).order_by('-scraped_at_utc')
            
            self.stdout.write(f'Total scrape jobs: {scrape_jobs.count()}')
            
            if scrape_jobs.exists():
                self.stdout.write(f'\nRecent scrape jobs:')
                for scrape in scrape_jobs[:10]:
                    self.stdout.write(
                        f'  🕐 {scrape.scraped_at_utc} - '
                        f'Success: {scrape.scrape_success} - '
                        f'Status: {getattr(scrape, "status", "N/A")}'
                    )
                
                # Check the latest successful scrape for seat packs
                latest_success = scrape_jobs.filter(scrape_success=True).first()
                if latest_success:
                    seat_packs = SeatPack.objects.filter(scrape_job_key=latest_success)
                    self.stdout.write(f'\nLatest successful scrape seat packs: {seat_packs.count()}')
                    
                    if seat_packs.exists():
                        active_packs = seat_packs.filter(is_active=True).count()
                        self.stdout.write(f'  • Active: {active_packs}')
                        self.stdout.write(f'  • Inactive: {seat_packs.count() - active_packs}')
                        
                        # Show pack size distribution
                        sizes = seat_packs.values('pack_size').annotate(
                            count=Count('internal_pack_id')
                        ).order_by('pack_size')
                        
                        self.stdout.write(f'  Pack sizes:')
                        for size_info in sizes:
                            self.stdout.write(f'    • Size {size_info["pack_size"]}: {size_info["count"]} packs')
            
        except Exception as e:
            self.stdout.write(f'❌ Error checking scrape jobs: {str(e)}')

    def check_redis_possibility(self, performance_id):
        """Check if data might be coming from Redis/cache"""
        self.stdout.write(f'\n4️⃣ REDIS/CACHE POSSIBILITY')
        self.stdout.write('-' * 30)
        
        self.stdout.write(f'🔍 Frontend might be getting data from:')
        self.stdout.write(f'  • Redis cache (performance_data:{performance_id})')
        self.stdout.write(f'  • NestJS backend API that bypasses Django filters')
        self.stdout.write(f'  • WebSocket real-time data')
        self.stdout.write(f'  • Different performance ID mapping')
        
        self.stdout.write(f'\n💡 RECOMMENDATIONS:')
        self.stdout.write(f'  1. Check Redis for key: performance_data:{performance_id}')
        self.stdout.write(f'  2. Check NestJS backend logs for this performance')
        self.stdout.write(f'  3. Check if frontend is using a different performance ID')
        self.stdout.write(f'  4. Verify the RabbitMQ message being sent to Django')

    def check_similar_performances(self, performance_id):
        """Check for performances with similar IDs"""
        self.stdout.write(f'\n5️⃣ SIMILAR PERFORMANCE IDS')
        self.stdout.write('-' * 30)
        
        try:
            # Extract the base part of the performance ID
            base_parts = performance_id.split('_')
            if len(base_parts) >= 2:
                base_search = '_'.join(base_parts[:2])  # e.g., "bsf_perf"
                
                similar_perfs = Performance.objects.filter(
                    internal_performance_id__startswith=base_search
                ).select_related('event_id', 'venue_id')[:10]
                
                self.stdout.write(f'Performances starting with "{base_search}":')
                for perf in similar_perfs:
                    zones_count = Zone.objects.filter(performance_id=perf).count()
                    
                    # Check seat packs for this performance
                    scrapes = ScrapeJob.objects.filter(performance_id=perf.internal_performance_id)
                    total_packs = 0
                    for scrape in scrapes:
                        total_packs += SeatPack.objects.filter(scrape_job_key=scrape).count()
                    
                    self.stdout.write(
                        f'  🎭 {perf.internal_performance_id}'
                    )
                    self.stdout.write(
                        f'     Event: {perf.event_id.name} | '
                        f'Zones: {zones_count} | '
                        f'Seat Packs: {total_packs}'
                    )
                    
                    if total_packs > 0:
                        self.stdout.write(f'     ⭐ This performance has seat pack data!')
                        
        except Exception as e:
            self.stdout.write(f'❌ Error checking similar performances: {str(e)}')
        
        self.stdout.write(f'\n✅ Debug analysis complete!')
        self.stdout.write('=' * 60)