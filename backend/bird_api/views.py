import re
from collections import defaultdict
from rest_framework.permissions import AllowAny
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics, permissions, status
from data_pipeline.models import BirdSpeciesRecord
from .models import Record, Comment, RecordDetail, SpeciesCount
from .serializers import (
    RecordCreateSerializer,
    RecordBasicSerializer,
    RecordFullSerializer,
    CommentSerializer,
    UserSerializer
)
from .permissions import IsOwnerOrReadOnly
from .filters import RecordFilter

class DistrictSpeciesSummaryView(APIView):
    """
    返回每个处理后地区的唯一物种总数。
    """

    def _extract_district(self, full_address_str):
        if not isinstance(full_address_str, str):
            return "未知地区"

        match = re.search(r'([^市]+市)([^区]+区)', full_address_str)
        if match:
            return match.group(2).strip()

        match = re.search(r'([^区]+区)', full_address_str)
        if match:
            return match.group(1).strip()

        if full_address_str.endswith(('区', '县', '市')):
            return full_address_str.strip()

        return full_address_str.strip() if full_address_str.strip() else "未知地区"

    permission_classes = [AllowAny]
    def get(self, request, *args, **kwargs):
        species_records = BirdSpeciesRecord.objects.select_related('observation').only(
            'observation__address', 'taxon_id'
        ).all()

        #构建一个字典来存储每个处理后地区的唯一物种ID集合
        district_taxon_ids = defaultdict(set)

        for record in species_records:
            original_address = record.observation.address
            taxon_id = record.taxon_id

            processed_district_name = self._extract_district(original_address)
            processed_district_name = processed_district_name[3:]
            district_taxon_ids[processed_district_name].add(taxon_id)

        # 转换数据为最终的输出格式
        result_data = []
        for district_name, taxon_set in district_taxon_ids.items():
            result_data.append({
                "地区": district_name,
                "物种总数": len(taxon_set)
            })

        # 按物种总数排序或按地区名称排序
        result_data.sort(key=lambda x: x["物种总数"], reverse=True)

        return Response({"code":201,"data":result_data})

# 创建记录信息的接口
class RecordCreateAPIView(generics.CreateAPIView):
    queryset = Record.objects.all()
    serializer_class = RecordCreateSerializer
    permission_classes = [permissions.IsAuthenticated]


# 提供基本信息接口
class RecordListBasicAPIView(generics.ListAPIView):
    queryset = Record.objects.select_related('user').all()
    serializer_class = RecordBasicSerializer
    permission_classes = [permissions.IsAuthenticated]

    filter_backends = [DjangoFilterBackend]
    filterset_class = RecordFilter

# 提供详细信息接口
class RecordDetailFullAPIView(generics.RetrieveAPIView):
    queryset = Record.objects.select_related('user', 'details') \
        .prefetch_related('comments__user', 'details__species_counts').all()
    serializer_class = RecordFullSerializer
    permission_classes = [permissions.IsAuthenticated]

# 评论接口
class CommentCreateAPIView(generics.CreateAPIView):
    queryset = Comment.objects.all()
    serializer_class = CommentSerializer
    permission_classes = [permissions.IsAuthenticated]

    def perform_create(self, serializer):
        # 从 URL 参数中获取 record_pk
        record_pk = self.kwargs.get('pk')
        try:
            record = Record.objects.get(pk=record_pk)
        except Record.DoesNotExist:
            return Response(
                {"detail": "Record not found."},
                status=status.HTTP_404_NOT_FOUND
            )
        # 自动设置评论所属的记录和评论用户
        serializer.save(record=record, user=self.request.user)

# 获取当前登录用户提交的所有记录的基本信息
class CurrentUserRecordListView(generics.ListAPIView):
    serializer_class = RecordBasicSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return Record.objects.select_related('user').filter(user=self.request.user)


# 删除记录
class RecordRetrieveDestroyAPIView(generics.RetrieveDestroyAPIView):
    queryset = Record.objects.all()
    # 对于获取详情，我们返回完整信息
    serializer_class = RecordFullSerializer
    # 使用我们创建的自定义权限
    permission_classes = [permissions.IsAuthenticated, IsOwnerOrReadOnly]