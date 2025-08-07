from django.urls import path, include
from bird_api.views import DistrictSpeciesSummaryView,RecordCreateAPIView,RecordListBasicAPIView,RecordDetailFullAPIView,CommentCreateAPIView,CurrentUserRecordListView,RecordRetrieveDestroyAPIView

urlpatterns = [
    path('summary/district-species/',DistrictSpeciesSummaryView.as_view(),name='district_species_summary'),
    # 记录相关接口
    path('bird_records/', RecordCreateAPIView.as_view(), name='bird-record-create'),  # POST: 创建记录 (接收完整JSON)
    path('bird_records/basic/', RecordListBasicAPIView.as_view(), name='bird-record-list-basic'),  # GET: 获取所有记录的基本信息
    path('bird_records/<int:pk>/full/', RecordDetailFullAPIView.as_view(), name='bird-record-detail-full'),
    # GET: 获取单个记录的详细信息
    path('bird_records/my-records/', CurrentUserRecordListView.as_view(), name='bird-record-list-my-records'),
    path('bird_records/<int:pk>/', RecordRetrieveDestroyAPIView.as_view(), name='record-retrieve-destroy'),
    path('bird_records/<int:pk>/comments/', CommentCreateAPIView.as_view(), name='comment-create'),  # POST: 为指定记录添加评论
]


