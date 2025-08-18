# aqi_api/views.py
from datetime import timedelta
from rest_framework import status, generics
from rest_framework.views import APIView
from rest_framework.response import Response
from aqi_api.serializers import AQIRecordSerializer
from data_pipeline.models import AQIStation, AQIRecord
from django.db.models import Subquery, OuterRef
from rest_framework.authentication import TokenAuthentication
from datetime import datetime
from rest_framework.permissions import AllowAny


class GetStationListView(APIView):
    """
    获取所有监测站最新空气质量数据的API
    """
    # authentication_classes = [TokenAuthentication]
    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
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
                'station_id': station.id,
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
        response_data.sort(key=lambda item: item['aqi_data']['aqi'], reverse=False)
        return Response({"code": 201, "data": response_data})


class StationHourlyDataAPIView(APIView):
    """
    获取指定监测站最新数据点往前24小时内的数据 (DRF版本)
    """
    permission_classes = [AllowAny]

    def get(self, request, station_id, format=None):
        try:
            station = AQIStation.objects.get(pk=station_id)
        except AQIStation.DoesNotExist:
            return Response(
                {'code': 404, 'error': '监测站未找到'},
                status=status.HTTP_404_NOT_FOUND
            )

        # 找到该站点的最新一条记录
        latest_record = AQIRecord.objects.filter(station=station).order_by('-timestamp').first()

        if not latest_record:
            # 如果该站点没有任何记录
            return Response(
                {'code': 201, 'data': []},
                status=status.HTTP_200_OK
            )

        # 以最新记录的时间为基准，计算24小时前的时间点
        latest_timestamp = latest_record.timestamp
        start_time = latest_timestamp - timedelta(hours=24)

        #查询这个时间窗口内的数据
        records_queryset = AQIRecord.objects.filter(
            station=station,
            timestamp__gte=start_time,
            timestamp__lte=latest_timestamp
        ).order_by('-timestamp')

        # 使用序列化器处理数据
        serializer = AQIRecordSerializer(records_queryset, many=True)

        return Response(
            {'code': 201, 'data': serializer.data},
            status=status.HTTP_200_OK
        )


class AQIRecordByTimeView(generics.ListAPIView):

    serializer_class = AQIRecordSerializer
    permission_classes = [AllowAny]

    def get_queryset(self):
        queryset = AQIRecord.objects.all()

        time_str = self.request.query_params.get('date', None)

        if not time_str:
            return AQIRecord.objects.none()

        try:
            target_datetime = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return AQIRecord.objects.none()
        # 去重的逻辑
        queryset = queryset.filter(timestamp__lte=target_datetime)
        queryset = queryset.order_by('station_id', '-timestamp')
        queryset = queryset.distinct('station_id')

        return queryset
