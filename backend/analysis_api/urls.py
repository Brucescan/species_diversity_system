# analysis_api/urls.py

from django.urls import path
from .views import SpearmanAnalysisView

urlpatterns = [
    path('spearman/', SpearmanAnalysisView.as_view(), name='spearman-analysis'),
]