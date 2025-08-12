from django.apps import AppConfig


class ScrapersConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'scrapers'
    verbose_name = '🕷️ Scrapers & Proxies'
    
    def ready(self):
        """Called when the app is ready. Auto-register scrapers in DB if missing."""
        # Import admin configurations to ensure they're registered
        try:
            from . import admin
            from . import admin_unfold
        except ImportError:
            pass

        # Avoid database access during app initialization
        # The auto-registration will be handled by a management command or signal handler
        # instead of doing it directly here
