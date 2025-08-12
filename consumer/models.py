from django.db import models
import json

class PerformanceScrapeData(models.Model):
    pattern = models.CharField(max_length=255)
    data = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    scrape_job_id = models.CharField(max_length=255, null=True, blank=True)  # Adding the id field

    def __str__(self):
        return f"{self.pattern} - {self.created_at}"

    def get_data_json(self):
        try:
            return json.loads(self.data)
        except:
            return {}