from rest_framework.views import APIView
from rest_framework.response import Response
from data_pipeline.models import AQIStation, AQIRecord
from django.db.models import Subquery, OuterRef
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import AllowAny

class GetStationListView(APIView):
    """
    获取所有监测站最新空气质量数据的API
    返回数据包含:
    - 监测站名称
    - 位置坐标
    - 最新AQI数据
    """
    # authentication_classes = [TokenAuthentication]
    permission_classes = [AllowAny]
    def get(self, request):
        # 获取每个监测站最新的记录ID
        latest_records = AQIRecord.objects.filter(
            station=OuterRef('pk')
        ).order_by('-timestamp').values('id')[:1]

        # 获取所有监测站及其最新记录
        stations = AQIStation.objects.annotate(
            latest_record_id=Subquery(latest_records)
        ).prefetch_related('records')

        response_data = []

        for station in stations:
            # 获取该站点的最新记录
            latest_record = station.records.filter(id=station.latest_record_id).first()

            if not latest_record:
                continue

            # 构建响应数据
            station_data = {
                'station_name': station.name,
                'location': {
                    'latitude': station.location.y,
                    'longitude': station.location.x
                },
                'aqi_data': {
                    'aqi': latest_record.aqi,
                    'quality': latest_record.quality,
                    'description': latest_record.description,
                    'measure': latest_record.measure,
                    'timestr': latest_record.timestr,
                    'co': latest_record.co,
                    'no2': latest_record.no2,
                    'o3': latest_record.o3,
                    'pm10': latest_record.pm10,
                    'pm25': latest_record.pm25,
                    'so2': latest_record.so2,
                }
            }
            response_data.append(station_data)

        return Response({"code": 201, "data": response_data})
