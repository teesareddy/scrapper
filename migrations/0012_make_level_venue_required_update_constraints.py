# Make venue_id required and update constraints

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scrapers', '0011_populate_level_venue_and_alias_data'),
    ]

    operations = [
        # Make venue_id non-nullable
        migrations.AlterField(
            model_name='level',
            name='venue_id',
            field=models.ForeignKey(
                db_column='internal_venue_id',
                help_text='Venue this level belongs to',
                on_delete=django.db.models.deletion.CASCADE,
                related_name='levels',
                to='scrapers.venue'
            ),
        ),
        
        # Update Level unique_together constraint
        migrations.AlterUniqueTogether(
            name='level',
            unique_together={('venue_id', 'source_level_id', 'source_website', 'name')},
        ),
    ]