# analysis_api/serializers.py

from rest_framework import serializers


class SpearmanAnalysisSerializer(serializers.Serializer):
    """
    用于验证Spearman相关性分析请求参数的序列化器。
    该版本明确区分了数据源路径和图层名称。
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
        """
        自定义验证，确保字段至少有两个，因为相关性分析至少需要两个变量。
        """
        field_list = value.split(';')
        if len(field_list) < 2:
            raise serializers.ValidationError("分析字段至少需要两个。")
        return value  # 验证通过后必须返回原始值