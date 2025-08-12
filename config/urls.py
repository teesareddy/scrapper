"""
URL configuration for config project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import views as auth_views
from django.http import HttpResponse

def root_redirect(request, path=None):
    """Redirect root URL to admin login or dashboard based on authentication"""
    if request.user.is_authenticated:
        return redirect('/admin/')
    else:
        return redirect('/admin/login/')

def health_check(request):
    """Simple health check endpoint"""
    return HttpResponse('OK', content_type='text/plain')

urlpatterns = [
    # Admin site (main interface)
    path('admin/', admin.site.urls),
    
    # Scraper management URLs
    path('scrapers/', include('scrapers.urls')),

    # Root redirect to admin
    path('', root_redirect, name='root'),
    
    # Health check for monitoring
    path('health/', health_check, name='health_check'),
    
    # All other URLs redirect to admin
    path('<path:path>', root_redirect),
]