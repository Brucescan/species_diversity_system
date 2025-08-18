# analysis_api/serializers.py
import pandas as pd
from rest_framework import serializers


class SpearmanAnalysisSerializer(serializers.Serializer):
    """
    用于验证Spearman相关性分析请求参数的序列化器。
    """
    gdb_path = serializers.CharField(
        required=True,
        allow_blank=False,
        help_text="文件地理数据库(.gdb)的完整文件夹路径 (例如: \\\\server\\share\\data.gdb)。"
    )

    layer_name = serializers.CharField(
        required=True,
        allow_blank=False,
        help_text="在 .gdb 中的要素类（图层）名称。"
    )

    fields = serializers.CharField(
        required=True,
        allow_blank=False,
        help_text="需要分析的字段，用分号(;)分隔。"
    )

    def validate_fields(self, value):
        field_list = value.split(';')
        if len(field_list) < 2:
            raise serializers.ValidationError("分析字段至少需要两个。")
        return value  # 验证通过后必须返回原始值

class PredictionInputSerializer(serializers.Serializer):
    start_month_str = serializers.CharField(max_length=7, help_text="预测开始月份，格式 YYYY-MM")
    num_months = serializers.ChoiceField(choices=[1, 3, 6], help_text="预测月数，可选 1, 3, 6")

    def validate_start_month_str(self, value):
        try:
            # 尝试解析，确保格式正确
            pd.to_datetime(value, format='%Y-%m')
        except ValueError:
            raise serializers.ValidationError("start_month_str 格式不正确，应为 YYYY-MM。")
        return value