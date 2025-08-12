# scrapers/utils/json_serializer.py
import json
import decimal
import datetime
import uuid
from django.db import models
from django.core.serializers.json import DjangoJSONEncoder


class DjangoModelJSONEncoder(DjangoJSONEncoder):
    """
    Custom JSON encoder that handles Django models and other common Python objects.
    """

    def default(self, obj):
        # Handle Django model instances
        if isinstance(obj, models.Model):
            return self.serialize_model(obj)

        # Handle QuerySets
        elif hasattr(obj, '__iter__') and hasattr(obj, 'model'):
            return [self.serialize_model(item) for item in obj]

        # Handle Decimal
        elif isinstance(obj, decimal.Decimal):
            return float(obj)

        # Handle UUID
        elif isinstance(obj, uuid.UUID):
            return str(obj)

        # Handle datetime objects
        elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()

        # Use Django's default encoder for other types
        return super().default(obj)

    def serialize_model(self, obj):
        """Serialize a Django model instance to a dictionary."""
        if not isinstance(obj, models.Model):
            return obj

        data = {}

        # Get all fields for the model
        for field in obj._meta.fields:
            field_name = field.name
            field_value = getattr(obj, field_name, None)

            # Handle different field types
            if field_value is None:
                data[field_name] = None
            elif isinstance(field, models.ForeignKey):
                # For foreign keys, include the primary key and a string representation
                data[field_name] = {
                    'pk': field_value.pk,
                    'str': str(field_value)
                }
            elif isinstance(field, models.DateTimeField):
                data[field_name] = field_value.isoformat() if field_value else None
            elif isinstance(field, models.DecimalField):
                data[field_name] = float(field_value) if field_value else None
            elif isinstance(field, models.UUIDField):
                data[field_name] = str(field_value) if field_value else None
            elif isinstance(field, models.JSONField):
                data[field_name] = field_value  # Already JSON serializable
            else:
                data[field_name] = field_value

        # Add model metadata
        data['__model__'] = {
            'app_label': obj._meta.app_label,
            'model_name': obj._meta.model_name,
            'pk': obj.pk
        }

        return data


class SafeJSONSerializer:
    """Safe JSON serializer with custom encoder and error handling."""

    @staticmethod
    def dumps(obj, **kwargs):
        """Safely serialize object to JSON string."""
        try:
            return json.dumps(obj, cls=DjangoModelJSONEncoder, **kwargs)
        except Exception as e:
            return json.dumps({
                'error': f'JSON serialization failed: {str(e)}',
                'object_type': str(type(obj))
            })

    @staticmethod
    def loads(json_str):
        """Safely deserialize JSON string."""
        try:
            return json.loads(json_str)
        except Exception as e:
            return {
                'error': f'JSON deserialization failed: {str(e)}',
                'json_str': json_str[:100] + '...' if len(json_str) > 100 else json_str
            }


def make_json_serializable(obj):
    """Quick fix to make any object JSON serializable."""
    if hasattr(obj, '__dict__') and hasattr(obj, '_meta'):  # Django model
        model_dict = {}
        for field in obj._meta.fields:
            field_name = field.name
            field_value = getattr(obj, field_name, None)

            if field_value is None:
                model_dict[field_name] = None
            elif hasattr(field_value, '__dict__') and hasattr(field_value, '_meta'):
                # Foreign key - just store the primary key
                model_dict[field_name] = field_value.pk
            else:
                model_dict[field_name] = field_value

        return model_dict

    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]

    elif isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}

    else:
        return obj


def safe_json_dumps(obj, **kwargs):
    """Safe version of json.dumps that handles Django models."""
    try:
        return json.dumps(obj, cls=DjangoModelJSONEncoder, **kwargs)
    except TypeError:
        # If DjangoJSONEncoder fails, use our custom serializer
        safe_obj = make_json_serializable(obj)
        return json.dumps(safe_obj, **kwargs)