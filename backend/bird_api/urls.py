from django.urls import path, include
from bird_api.views import DistrictSpeciesSummaryView
urlpatterns = [
    path('summary/district-species/',DistrictSpeciesSummaryView.as_view(),name='district_species_summary'),
]