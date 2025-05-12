from django.urls import path, include
from aqi_api.views import GetStationListView,StationHourlyDataAPIView
urlpatterns = [
    path('station_lastest_list/',GetStationListView.as_view(),name='station_lastest_list'),
    path('station/<int:station_id>/hourly-records/',StationHourlyDataAPIView.as_view(),name='station_hourly_data'),
]