# Generated manually for Level venue relationship and SeatPack section field

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scrapers', '0009_remove_seatpack_seat_pack_is_acti_sync_idx_and_more'),
    ]

    operations = [
        # Step 1: Add alias field to Level with default value
        migrations.AddField(
            model_name='level',
            name='alias',
            field=models.CharField(blank=True, default='', help_text='User-friendly alias for the level name', max_length=255),
        ),
        
        # Step 2: Add venue_id field to Level (nullable first)
        migrations.AddField(
            model_name='level',
            name='venue_id',
            field=models.ForeignKey(
                blank=True,
                null=True,
                db_column='internal_venue_id',
                help_text='Venue this level belongs to',
                on_delete=django.db.models.deletion.CASCADE,
                related_name='levels',
                to='scrapers.venue'
            ),
        ),
        
        # Step 3: Add section field to SeatPack
        migrations.AddField(
            model_name='seatpack',
            name='section',
            field=models.ForeignKey(
                blank=True,
                null=True,
                db_column='internal_section_id',
                on_delete=django.db.models.deletion.CASCADE,
                related_name='seat_packs',
                to='scrapers.section'
            ),
        ),
        
        # Step 4: Add indexes
        migrations.AddIndex(
            model_name='level',
            index=models.Index(fields=['venue_id'], name='level_venue_id_idx'),
        ),
        migrations.AddIndex(
            model_name='level',
            index=models.Index(fields=['alias'], name='level_alias_idx'),
        ),
    ]