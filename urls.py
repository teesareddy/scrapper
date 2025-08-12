from django.urls import path
from . import views

app_name = 'scrapers'

urlpatterns = [
    # Scraper onboarding page
    path('onboard/', views.scraper_onboarding, name='onboarding'),

    
    # AJAX endpoints
    path('api/proxy-configurations/', views.get_proxy_configurations, name='get_proxy_configurations'),
    path('api/test-proxy/', views.test_proxy_connection, name='test_proxy_connection'),
] 