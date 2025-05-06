from django.urls import path, include
from aqi_api.views import GetStationListView
urlpatterns = [
    path('station_lastest_list',GetStationListView.as_view(),name='station_lastest_list'),
]