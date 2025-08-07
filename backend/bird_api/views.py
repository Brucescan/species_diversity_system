import re
from collections import defaultdict

from rest_framework.permissions import AllowAny
from django_filters.rest_framework import DjangoFilterBackend # 导入 DjangoFilterBackend
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
    API 视图，返回每个处理后地区的唯一物种总数。
    """

    def _extract_district(self, full_address_str):
        """
        从完整地址中提取地区名称。
        例如："北京市北京市西城区北京师范大学附属实验中学" -> "西城区"
        """
        if not isinstance(full_address_str, str):
            return "未知地区"

        # 匹配模式：前一个"市"之后到第一个"区"之间的内容
        match = re.search(r'([^市]+市)([^区]+区)', full_address_str)
        if match:
            return match.group(2).strip()

        # 如果没有"市"前缀，但包含"区"，则尝试直接提取第一个"区"
        match = re.search(r'([^区]+区)', full_address_str)
        if match:
            return match.group(1).strip()

        # 如果上面没有匹配到，但地址以 "区"、"县" 或 "市" 结尾，直接返回
        if full_address_str.endswith(('区', '县', '市')):
            return full_address_str.strip()

        # 如果都无法提取，返回原始地址（去除首尾空格）或一个默认值
        return full_address_str.strip() if full_address_str.strip() else "未知地区"

    permission_classes = [AllowAny]
    def get(self, request, *args, **kwargs):
        # 1. 获取所有相关的 BirdSpeciesRecord，包含其观测地点的地址和物种ID
        #    select_related('observation') 可以优化查询，避免N+1问题
        species_records = BirdSpeciesRecord.objects.select_related('observation').only(
            'observation__address', 'taxon_id'
        ).all()

        # 2. 构建一个字典来存储每个处理后地区的唯一物种ID集合
        #    键: 处理后的地区名 (e.g., "海淀区")
        #    值: set of taxon_id (e.g., {101, 102, 205})
        district_taxon_ids = defaultdict(set)

        for record in species_records:
            original_address = record.observation.address
            taxon_id = record.taxon_id

            processed_district_name = self._extract_district(original_address)
            processed_district_name = processed_district_name[3:]
            district_taxon_ids[processed_district_name].add(taxon_id)

        # 3. 转换数据为最终的输出格式
        result_data = []
        for district_name, taxon_set in district_taxon_ids.items():
            result_data.append({
                "地区": district_name,
                "物种总数": len(taxon_set)
            })

        # 按物种总数排序或按地区名称排序
        result_data.sort(key=lambda x: x["物种总数"], reverse=True) # 按物种总数降序

        return Response({"code":201,"data":result_data})

# 1. 创建记录信息的接口 (POST /api/bird_records/)
class RecordCreateAPIView(generics.CreateAPIView):
    queryset = Record.objects.all()
    serializer_class = RecordCreateSerializer
    permission_classes = [permissions.IsAuthenticated]

    # perform_create 不再需要手动设置 user，因为 create 序列化器中已经处理

# 2. 提供基本信息接口 (GET /api/bird_records/basic/)
# 搜索指定条件的报告 (修改现有的 RecordListBasicAPIView)
class RecordListBasicAPIView(generics.ListAPIView):
    queryset = Record.objects.select_related('user').all()
    serializer_class = RecordBasicSerializer
    permission_classes = [permissions.IsAuthenticated]

    # --- 新增过滤配置 ---
    filter_backends = [DjangoFilterBackend]  # 指定使用 DjangoFilterBackend
    filterset_class = RecordFilter  # 指定我们创建的过滤器类

# 3. 提供详细信息接口 (GET /api/bird_records/<pk>/full/)
# 获取指定记录的详细信息，包含 RecordDetail 和 SpeciesCount，以及其他用户的评论
class RecordDetailFullAPIView(generics.RetrieveAPIView):
    # 使用 select_related 获取 Record 的 user 和 details
    # 使用 prefetch_related 获取 comments (以及 comments 的 user) 和 details 的 species_counts
    queryset = Record.objects.select_related('user', 'details') \
        .prefetch_related('comments__user', 'details__species_counts').all()
    serializer_class = RecordFullSerializer
    permission_classes = [permissions.IsAuthenticated]

# 4. 评论接口 (POST /api/bird_records/<pk>/comments/)
# 为某个记录添加评论
class CommentCreateAPIView(generics.CreateAPIView):
    queryset = Comment.objects.all()
    serializer_class = CommentSerializer
    permission_classes = [permissions.IsAuthenticated]

    def perform_create(self, serializer):
        # 从 URL 参数中获取 record_pk (这里是 <int:pk>)
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

#新增：获取当前登录用户提交的所有记录的基本信息
class CurrentUserRecordListView(generics.ListAPIView):
    serializer_class = RecordBasicSerializer # 返回基本信息，与 RecordListBasicAPIView 保持一致
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return Record.objects.select_related('user').filter(user=self.request.user)


# 删除记录
class RecordRetrieveDestroyAPIView(generics.RetrieveDestroyAPIView):
    """
    - GET: 获取单个记录的详细信息 (使用 RecordFullSerializer)。
    - DELETE: 删除单个记录 (只有记录所有者可以删除)。
    """
    queryset = Record.objects.all() # queryset 保持不变
    # 对于获取详情，我们返回完整信息
    serializer_class = RecordFullSerializer
    # 使用我们创建的自定义权限
    permission_classes = [permissions.IsAuthenticated, IsOwnerOrReadOnly]