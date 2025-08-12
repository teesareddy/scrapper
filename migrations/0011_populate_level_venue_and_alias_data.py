# Data migration to populate venue_id and alias fields

from django.db import migrations


def populate_level_venue_and_alias(apps, schema_editor):
    """Populate venue_id and alias fields for existing levels"""
    Level = apps.get_model('scrapers', 'Level') 
    PerformanceLevel = apps.get_model('scrapers', 'PerformanceLevel')
    Performance = apps.get_model('scrapers', 'Performance')
    
    # Populate alias field with name value
    for level in Level.objects.filter(alias=''):
        level.alias = level.name
        level.save(update_fields=['alias'])
    
    # Populate venue_id based on performance relationships
    for level in Level.objects.filter(venue_id__isnull=True):
        # Find a performance that uses this level
        perf_level = PerformanceLevel.objects.filter(level=level).first()
        if perf_level:
            level.venue_id = perf_level.performance.venue_id
            level.save(update_fields=['venue_id'])


def reverse_populate_level_venue_and_alias(apps, schema_editor):
    """Reverse migration - clear venue_id and alias"""
    Level = apps.get_model('scrapers', 'Level')
    Level.objects.update(venue_id=None, alias='')


class Migration(migrations.Migration):

    dependencies = [
        ('scrapers', '0010_add_venue_to_level_and_section_to_seatpack'),
    ]

    operations = [
        migrations.RunPython(
            populate_level_venue_and_alias,
            reverse_populate_level_venue_and_alias,
        ),
    ]