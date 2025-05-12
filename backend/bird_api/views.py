import re
from collections import defaultdict

from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from data_pipeline.models import BirdSpeciesRecord

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