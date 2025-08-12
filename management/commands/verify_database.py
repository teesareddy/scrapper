from django.core.management.base import BaseCommand
from scrapers.models import *


class Command(BaseCommand):
    help = 'Verify database data'

    def handle(self, *args, **options):
        self.stdout.write('=== Database Verification ===')
        self.stdout.write(f'Venues: {Venue.objects.count()}')
        self.stdout.write(f'Events: {Event.objects.count()}')
        self.stdout.write(f'Performances: {Performance.objects.count()}')
        self.stdout.write(f'Zones: {Zone.objects.count()}')
        self.stdout.write(f'Seats: {Seat.objects.count()}')
        self.stdout.write(f'Scrape Jobs: {ScrapeJob.objects.count()}')

        # Check latest Washington Pavilion data
        wp_venues = Venue.objects.filter(source_website='washington_pavilion')
        if wp_venues.exists():
            venue = wp_venues.first()
            self.stdout.write(f'\nLatest WP Venue: {venue.name}')
            
            wp_performances = Performance.objects.filter(venue_key=venue).order_by('-perf_key')
            if wp_performances.exists():
                perf = wp_performances.first()
                self.stdout.write(f'Latest Performance: {perf.event_key.name} on {perf.performance_datetime_utc}')
                
                zones = Zone.objects.filter(perf_key=perf)
                seats = Seat.objects.filter(zone_key__in=zones)
                scrape_jobs = ScrapeJob.objects.filter(perf_key=perf).order_by('-scrape_job_key')
                
                self.stdout.write(f'Zones: {zones.count()}, Seats: {seats.count()}')
                if scrape_jobs.exists():
                    latest_job = scrape_jobs.first()
                    self.stdout.write(f'Latest Scrape: {latest_job.scraped_at_utc} - Success: {latest_job.scrape_success}')
                    
                # Show zone details
                self.stdout.write('\nZone Details:')
                for zone in zones[:3]:  # Show first 3 zones
                    self.stdout.write(f'  {zone.name}: ${zone.min_price}-${zone.max_price}')
        else:
            self.stdout.write('\nNo Washington Pavilion venues found in database')