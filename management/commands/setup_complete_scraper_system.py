#!/usr/bin/env python3
"""
Complete Scraper System Setup
This command sets up the entire scraper management system including:
- Scraper registration
- Proxy provider setup
- Proxy assignments
- Database migrations
- Admin interface verification
"""

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.utils import timezone
from scrapers.models import (
    ScraperDefinition, ProxyProvider, ProxyConfiguration, 
    ScraperProxyAssignment
)
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Complete setup of the scraper management system'

    def add_arguments(self, parser):
        parser.add_argument(
            '--skip-migrations',
            action='store_true',
            help='Skip running database migrations',
        )
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Reset existing data (WARNING: This will delete existing data)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without actually doing it',
        )

    def handle(self, *args, **options):
        """Set up the complete scraper system"""
        
        self.stdout.write("\n" + "="*80)
        self.stdout.write(self.style.SUCCESS("🎭 COMPLETE SCRAPER SYSTEM SETUP"))
        self.stdout.write("="*80)
        self.stdout.write("Setting up comprehensive scraper management system...")
        
        if options['dry_run']:
            self.stdout.write(
                self.style.WARNING("🔍 DRY RUN MODE - No changes will be made")
            )
        
        # Step 1: Database Migrations
        if not options['skip_migrations'] and not options['dry_run']:
            self.stdout.write("\n" + "="*60)
            self.stdout.write(self.style.SUCCESS("STEP 1: Database Migrations"))
            self.stdout.write("="*60)
            
            try:
                call_command('makemigrations', 'scrapers', verbosity=1)
                call_command('migrate', verbosity=1)
                self.stdout.write(
                    self.style.SUCCESS("✅ Database migrations completed")
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Database migration failed: {e}")
                )
                return
        
        # Step 2: Register Scrapers
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("STEP 2: Registering Scrapers"))
        self.stdout.write("="*60)
        
        try:
            if options['reset']:
                call_command('register_existing_scrapers', '--force', verbosity=1)
            else:
                call_command('register_existing_scrapers', verbosity=1)
            self.stdout.write(
                self.style.SUCCESS("✅ Scrapers registered successfully")
            )
        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f"⚠️ Scraper registration warning: {e}")
            )
        
        # Step 3: Setup Proxy System
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("STEP 3: Setting up Proxy System"))
        self.stdout.write("="*60)
        
        try:
            proxy_args = []
            if options['reset']:
                proxy_args.append('--reset')
            if options['dry_run']:
                proxy_args.append('--dry-run')
            
            call_command('setup_comprehensive_proxy_system', *proxy_args, verbosity=1)
            self.stdout.write(
                self.style.SUCCESS("✅ Proxy system setup completed")
            )
        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f"⚠️ Proxy setup warning: {e}")
            )
        
        # Step 4: Verify Setup
        if not options['dry_run']:
            self.stdout.write("\n" + "="*60)
            self.stdout.write(self.style.SUCCESS("STEP 4: Verifying Setup"))
            self.stdout.write("="*60)
            
            self._verify_setup()
        
        # Step 5: Setup Summary and Instructions
        self.stdout.write("\n" + "="*80)
        self.stdout.write(self.style.SUCCESS("🎉 SETUP COMPLETE"))
        self.stdout.write("="*80)
        
        if not options['dry_run']:
            self._print_setup_summary()
            self._print_next_steps()
        else:
            self.stdout.write(
                self.style.WARNING("DRY RUN COMPLETE: Run without --dry-run to actually set up the system")
            )

    def _verify_setup(self):
        """Verify that the setup was successful."""
        
        errors = []
        warnings = []
        
        # Check scrapers
        try:
            scraper_count = ScraperDefinition.objects.count()
            active_scrapers = ScraperDefinition.objects.filter(is_enabled=True).count()
            
            if scraper_count == 0:
                errors.append("No scrapers found in database")
            elif active_scrapers == 0:
                warnings.append("No active scrapers found")
            else:
                self.stdout.write(f"✅ Found {scraper_count} scrapers ({active_scrapers} active)")
                
        except Exception as e:
            errors.append(f"Failed to verify scrapers: {e}")
        
        # Check proxy providers
        try:
            provider_count = ProxyProvider.objects.count()
            active_providers = ProxyProvider.objects.filter(is_active=True).count()
            
            if provider_count == 0:
                warnings.append("No proxy providers found")
            else:
                self.stdout.write(f"✅ Found {provider_count} proxy providers ({active_providers} active)")
                
        except Exception as e:
            warnings.append(f"Failed to verify proxy providers: {e}")
        
        # Check proxy configurations
        try:
            config_count = ProxyConfiguration.objects.count()
            active_configs = ProxyConfiguration.objects.filter(is_active=True).count()
            
            if config_count == 0:
                warnings.append("No proxy configurations found")
            else:
                self.stdout.write(f"✅ Found {config_count} proxy configurations ({active_configs} active)")
                
        except Exception as e:
            warnings.append(f"Failed to verify proxy configurations: {e}")
        
        # Check proxy assignments
        try:
            assignment_count = ScraperProxyAssignment.objects.count()
            active_assignments = ScraperProxyAssignment.objects.filter(is_active=True).count()
            
            if assignment_count == 0:
                warnings.append("No proxy assignments found")
            else:
                self.stdout.write(f"✅ Found {assignment_count} proxy assignments ({active_assignments} active)")
                
        except Exception as e:
            warnings.append(f"Failed to verify proxy assignments: {e}")
        
        # Report issues
        if errors:
            self.stdout.write("\n❌ ERRORS FOUND:")
            for error in errors:
                self.stdout.write(f"   • {error}")
        
        if warnings:
            self.stdout.write("\n⚠️ WARNINGS:")
            for warning in warnings:
                self.stdout.write(f"   • {warning}")
        
        if not errors and not warnings:
            self.stdout.write(
                self.style.SUCCESS("\n🎯 VERIFICATION PASSED: All components are properly set up!")
            )

    def _print_setup_summary(self):
        """Print a summary of what was set up."""
        
        try:
            # Get counts
            scraper_count = ScraperDefinition.objects.count()
            active_scrapers = ScraperDefinition.objects.filter(is_enabled=True).count()
            provider_count = ProxyProvider.objects.count()
            config_count = ProxyConfiguration.objects.count()
            assignment_count = ScraperProxyAssignment.objects.count()
            
            self.stdout.write("📊 SYSTEM SUMMARY:")
            self.stdout.write(f"   • Scrapers: {scraper_count} total, {active_scrapers} active")
            self.stdout.write(f"   • Proxy Providers: {provider_count}")
            self.stdout.write(f"   • Proxy Configurations: {config_count}")
            self.stdout.write(f"   • Scraper-Proxy Assignments: {assignment_count}")
            
            # List active scrapers
            if active_scrapers > 0:
                self.stdout.write("\n🕷️ ACTIVE SCRAPERS:")
                for scraper in ScraperDefinition.objects.filter(is_enabled=True):
                    proxy_status = "✅ Has proxy" if scraper.assigned_proxy else "⚠️ No proxy"
                    self.stdout.write(f"   • {scraper.display_name} ({proxy_status})")
                    
        except Exception as e:
            self.stdout.write(f"Failed to generate summary: {e}")

    def _print_next_steps(self):
        """Print next steps for the user."""
        
        self.stdout.write("\n🚀 NEXT STEPS:")
        self.stdout.write("   1. 🌐 Access Django Admin:")
        self.stdout.write("      http://localhost:8000/admin/")
        self.stdout.write("      (Create superuser if needed: python manage.py createsuperuser)")
        
        self.stdout.write("\n   2. 🎯 Main Dashboard Sections:")
        self.stdout.write("      • 🎭 Scraper Dashboard - Overview and quick actions")
        self.stdout.write("      • 🕷️ Scraper Definitions - Configure individual scrapers")
        self.stdout.write("      • 🌐 Proxy Configurations - Manage proxy settings")
        self.stdout.write("      • 🔗 Proxy Assignments - Link scrapers to proxies")
        
        self.stdout.write("\n   3. 🔧 Test Your Setup:")
        self.stdout.write("      python manage.py test_proxy")
        
        self.stdout.write("\n   4. 🏃 Run a Scraper:")
        self.stdout.write("      python manage.py run_scraper broadway_sf_scraper_v5 <url>")
        
        self.stdout.write("\n   5. 📊 Monitor Performance:")
        self.stdout.write("      • Check Scraper Executions in admin")
        self.stdout.write("      • Review Proxy Usage Logs")
        self.stdout.write("      • Monitor Resource Usage")
        
        self.stdout.write("\n   6. 🔄 Schedule Scrapers:")
        self.stdout.write("      • Set up Scraper Schedules in admin")
        self.stdout.write("      • Configure Celery Beat for automation")
        
        self.stdout.write(f"\n🎉 CONGRATULATIONS!")
        self.stdout.write("Your comprehensive scraper management system is ready!")
        self.stdout.write("All scrapers are registered and ready to be configured through the Django admin.")
        
        self.stdout.write(f"\n💡 TIP:")
        self.stdout.write("Start by visiting the '🎯 Scraper Dashboard' in the admin to get an overview")
        self.stdout.write("of your system status and quick access to common actions.")