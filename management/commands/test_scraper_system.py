#!/usr/bin/env python3
"""
Test Scraper System
Quick command to test that the scraper management system is working correctly
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from scrapers.models import (
    ScraperDefinition, ProxyConfiguration, ScraperProxyAssignment
)
from scrapers.services.scraper_config_service import ScraperConfigurationService
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Test the scraper management system functionality'

    def add_arguments(self, parser):
        parser.add_argument(
            '--scraper',
            type=str,
            help='Specific scraper to test (e.g., broadway_sf_scraper_v5)',
        )
        parser.add_argument(
            '--proxy-test',
            action='store_true',
            help='Test proxy configurations',
        )

    def handle(self, *args, **options):
        """Test the scraper system"""
        
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("🧪 SCRAPER SYSTEM TEST"))
        self.stdout.write("="*60)
        
        all_tests_passed = True
        
        # Test 1: Database Connectivity
        self.stdout.write("\n1️⃣ Testing database connectivity...")
        try:
            scraper_count = ScraperDefinition.objects.count()
            self.stdout.write(
                self.style.SUCCESS(f"✅ Database connected - Found {scraper_count} scrapers")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Database connection failed: {e}")
            )
            all_tests_passed = False
            return
        
        # Test 2: Scraper Configuration Service
        self.stdout.write("\n2️⃣ Testing scraper configuration service...")
        try:
            active_scrapers = ScraperConfigurationService.get_active_scrapers()
            self.stdout.write(
                self.style.SUCCESS(f"✅ Configuration service working - {len(active_scrapers)} active scrapers")
            )
            
            for scraper_config in active_scrapers:
                self.stdout.write(f"   • {scraper_config['display_name']} ({scraper_config['name']})")
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Configuration service failed: {e}")
            )
            all_tests_passed = False
        
        # Test 3: Proxy System
        self.stdout.write("\n3️⃣ Testing proxy system...")
        try:
            proxy_count = ProxyConfiguration.objects.filter(is_active=True).count()
            assignment_count = ScraperProxyAssignment.objects.filter(is_active=True).count()
            
            self.stdout.write(
                self.style.SUCCESS(f"✅ Proxy system working - {proxy_count} active proxies, {assignment_count} assignments")
            )
            
            # Test proxy assignments for each scraper
            for scraper in ScraperDefinition.objects.filter(is_enabled=True):
                proxy_config = ScraperConfigurationService.get_assigned_proxy(scraper.name)
                if proxy_config:
                    self.stdout.write(f"   • {scraper.display_name}: {proxy_config['proxy_name']} ({proxy_config['provider_name']})")
                else:
                    self.stdout.write(f"   ⚠️ {scraper.display_name}: No proxy assigned")
                    
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Proxy system test failed: {e}")
            )
            all_tests_passed = False
        
        # Test 4: Specific Scraper Configuration
        if options['scraper']:
            self.stdout.write(f"\n4️⃣ Testing specific scraper: {options['scraper']}...")
            try:
                config = ScraperConfigurationService.get_scraper_config(options['scraper'])
                if config:
                    self.stdout.write(
                        self.style.SUCCESS(f"✅ Scraper configuration loaded successfully")
                    )
                    self.stdout.write(f"   • Display Name: {config['display_name']}")
                    self.stdout.write(f"   • Status: {config['status']}")
                    self.stdout.write(f"   • Enabled: {config['is_enabled']}")
                    self.stdout.write(f"   • Use Proxy: {config['use_proxy']}")
                    self.stdout.write(f"   • Optimization: {config['optimization_enabled']} ({config['optimization_level']})")
                    
                    if config['proxy_config']:
                        proxy = config['proxy_config']
                        self.stdout.write(f"   • Proxy: {proxy['proxy_name']} ({proxy['provider_name']})")
                    else:
                        self.stdout.write("   • Proxy: None assigned")
                        
                else:
                    self.stdout.write(
                        self.style.ERROR(f"❌ Scraper configuration not found for: {options['scraper']}")
                    )
                    all_tests_passed = False
                    
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Scraper configuration test failed: {e}")
                )
                all_tests_passed = False
        
        # Test 5: Proxy Testing (if requested)
        if options['proxy_test']:
            self.stdout.write("\n5️⃣ Testing proxy connections...")
            try:
                active_proxies = ProxyConfiguration.objects.filter(is_active=True)
                tested_count = 0
                working_count = 0
                
                for proxy in active_proxies:
                    self.stdout.write(f"   Testing {proxy.name}...")
                    
                    # Simple proxy test (you can expand this)
                    try:
                        import requests
                        proxy_url = proxy.proxy_url
                        proxies = {
                            'http': proxy_url,
                            'https': proxy_url
                        }
                        
                        response = requests.get(
                            'http://httpbin.org/ip', 
                            proxies=proxies, 
                            timeout=10
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            self.stdout.write(f"     ✅ Working - IP: {data.get('origin', 'Unknown')}")
                            working_count += 1
                        else:
                            self.stdout.write(f"     ❌ Failed - HTTP {response.status_code}")
                            
                        tested_count += 1
                        
                    except Exception as proxy_error:
                        self.stdout.write(f"     ❌ Failed - {str(proxy_error)}")
                        tested_count += 1
                
                self.stdout.write(
                    self.style.SUCCESS(f"✅ Proxy test completed - {working_count}/{tested_count} working")
                )
                
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Proxy testing failed: {e}")
                )
                all_tests_passed = False
        
        # Test Results Summary
        self.stdout.write("\n" + "="*60)
        if all_tests_passed:
            self.stdout.write(
                self.style.SUCCESS("🎉 ALL TESTS PASSED")
            )
            self.stdout.write("Your scraper management system is working correctly!")
        else:
            self.stdout.write(
                self.style.ERROR("❌ SOME TESTS FAILED")
            )
            self.stdout.write("Please check the errors above and fix any issues.")
        
        self.stdout.write("="*60)
        
        # Additional Testing Suggestions
        self.stdout.write("\n💡 ADDITIONAL TESTING:")
        self.stdout.write("   • Test with a specific scraper:")
        self.stdout.write("     python manage.py test_scraper_system --scraper broadway_sf_scraper_v5")
        self.stdout.write("   • Test proxy connections:")
        self.stdout.write("     python manage.py test_scraper_system --proxy-test")
        self.stdout.write("   • Access Django admin:")
        self.stdout.write("     http://localhost:8000/admin/")
        self.stdout.write("   • Run a full scraper test:")
        self.stdout.write("     python manage.py run_scraper <scraper_name> <url>")
        
        return all_tests_passed