#!/usr/bin/env python3
"""
Check Admin Setup
Quick command to verify that all admin models are properly registered
"""

from django.core.management.base import BaseCommand
from django.contrib import admin
from django.apps import apps


class Command(BaseCommand):
    help = 'Check that all scraper models are properly registered in Django admin'

    def handle(self, *args, **options):
        """Check admin registration"""
        
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("🔍 ADMIN REGISTRATION CHECK"))
        self.stdout.write("="*60)
        
        # Get all models from scrapers app
        scrapers_app = apps.get_app_config('scrapers')
        scrapers_models = scrapers_app.get_models()
        
        self.stdout.write(f"\n📋 Found {len(scrapers_models)} models in scrapers app:")
        
        registered_count = 0
        unregistered_models = []
        
        for model in scrapers_models:
            model_name = f"{model._meta.app_label}.{model._meta.model_name}"
            verbose_name = model._meta.verbose_name
            
            if model in admin.site._registry:
                admin_class = admin.site._registry[model]
                admin_class_name = admin_class.__class__.__name__
                self.stdout.write(
                    self.style.SUCCESS(f"✅ {verbose_name} ({model_name}) - {admin_class_name}")
                )
                registered_count += 1
            else:
                self.stdout.write(
                    self.style.WARNING(f"⚠️ {verbose_name} ({model_name}) - NOT REGISTERED")
                )
                unregistered_models.append((model, model_name, verbose_name))
        
        # Summary
        self.stdout.write("\n" + "="*60)
        self.stdout.write(f"📊 SUMMARY:")
        self.stdout.write(f"   • Total models: {len(scrapers_models)}")
        self.stdout.write(f"   • Registered: {registered_count}")
        self.stdout.write(f"   • Unregistered: {len(unregistered_models)}")
        
        if unregistered_models:
            self.stdout.write(f"\n⚠️ UNREGISTERED MODELS:")
            for model, model_name, verbose_name in unregistered_models:
                self.stdout.write(f"   • {verbose_name} ({model_name})")
        
        # Check for key scraper models
        key_models = [
            'ScraperDefinition',
            'ProxyProvider', 
            'ProxyConfiguration',
            'ScraperProxyAssignment'
        ]
        
        self.stdout.write(f"\n🎯 KEY SCRAPER MODELS CHECK:")
        all_key_registered = True
        
        for model_name in key_models:
            try:
                model = apps.get_model('scrapers', model_name)
                if model in admin.site._registry:
                    self.stdout.write(
                        self.style.SUCCESS(f"✅ {model_name} - Registered")
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR(f"❌ {model_name} - NOT REGISTERED")
                    )
                    all_key_registered = False
            except LookupError:
                self.stdout.write(
                    self.style.ERROR(f"❌ {model_name} - MODEL NOT FOUND")
                )
                all_key_registered = False
        
        # Final status
        self.stdout.write("\n" + "="*60)
        if all_key_registered:
            self.stdout.write(
                self.style.SUCCESS("🎉 ALL KEY MODELS ARE REGISTERED!")
            )
            self.stdout.write("You should be able to see scraper management in Django admin.")
            self.stdout.write("\n💡 ACCESS PATHS:")
            self.stdout.write("   • Main admin: http://localhost:8000/admin/")
            self.stdout.write("   • Scrapers: http://localhost:8000/admin/scrapers/")
            self.stdout.write("   • Scraper Definitions: http://localhost:8000/admin/scrapers/scraperdefinition/")
            self.stdout.write("   • Proxy Configurations: http://localhost:8000/admin/scrapers/proxyconfiguration/")
        else:
            self.stdout.write(
                self.style.ERROR("❌ SOME KEY MODELS ARE NOT REGISTERED")
            )
            self.stdout.write("There may be import or configuration issues.")
        
        self.stdout.write("="*60)
        
        # Additional info
        self.stdout.write(f"\n🔧 TROUBLESHOOTING:")
        self.stdout.write("   1. Check if scrapers app is in INSTALLED_APPS")
        self.stdout.write("   2. Run: python manage.py migrate")
        self.stdout.write("   3. Check for admin.py import errors")
        self.stdout.write("   4. Restart Django server")
        self.stdout.write("   5. Create superuser: python manage.py createsuperuser")
        
        return all_key_registered