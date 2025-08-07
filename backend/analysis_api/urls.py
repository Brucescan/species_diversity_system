# analysis_api/urls.py

from django.urls import path
from .views import SpearmanAnalysisView, PredictFutureBaselineView, GridGeometriesView

urlpatterns = [
    path('spearman/', SpearmanAnalysisView.as_view(), name='spearman-analysis'),
    path('predict_future_baseline/',PredictFutureBaselineView.as_view(), name='predict_future_baseline'),
    path('grid_geometries/', GridGeometriesView.as_view(), name='grid-geometries'),
]