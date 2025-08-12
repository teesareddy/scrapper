from django.contrib import admin
from .models import PerformanceScrapeData


@admin.register(PerformanceScrapeData)
class PerformanceScrapeDataAdmin(admin.ModelAdmin):
    list_display = ('pattern', 'scrape_job_id', 'created_at', 'data_preview')
    search_fields = ('pattern', 'data', 'scrape_job_id')
    list_filter = ('pattern', 'created_at')
    readonly_fields = ('created_at',)

    def data_preview(self, obj):
        return obj.data[:100] + "..." if len(obj.data) > 100 else obj.data

    data_preview.short_description = "Data Preview"