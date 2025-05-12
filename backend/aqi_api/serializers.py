from rest_framework import serializers
from data_pipeline.models import AQIRecord

class AQIRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = AQIRecord
        fields = [
            'id',
            'station_id',
            'timestamp',
            'aqi',
            'quality',
            'description',
            'measure',
            'timestr',
            'co',
            'no2',
            'o3',
            'pm10',
            'pm25',
            'so2',
            'raw_data',
            'created_at'
        ]
