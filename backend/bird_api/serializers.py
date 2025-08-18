from rest_framework import serializers
from .models import Record, RecordDetail, SpeciesCount, Comment
from django.contrib.auth.models import User
from datetime import datetime

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username']
        ref_name = 'RecordUser'

class SpeciesCountSerializer(serializers.ModelSerializer):
    order = serializers.CharField(source='order_name')
    family = serializers.CharField(source='family_name')

    class Meta:
        model = SpeciesCount
        fields = ['count_id', 'china_name', 'order', 'family', 'count']
        read_only_fields = ['record_detail']

class RecordDetailReadSerializer(serializers.ModelSerializer):
    species_count = SpeciesCountSerializer(source='species_counts', many=True, read_only=True)

    class Meta:
        model = RecordDetail
        fields = ['basic_counts', 'longitude', 'latitude', 'species_count']

# 用于评论
class CommentSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True)

    class Meta:
        model = Comment
        fields = ['id', 'record', 'user', 'text', 'created_at']
        read_only_fields = ['record', 'user']

class RecordBasicSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True)
    observation_time = serializers.SerializerMethodField()

    class Meta:
        model = Record
        fields = [
            'id', 'record_identifier', 'user', 'observation_time',
            'observation_address', 'bird_count', 'created_at'
        ]
        read_only_fields = ['id', 'user', 'created_at']

    def get_observation_time(self, obj):
        # 将 observation_start_time 和 observation_end_time 格式化为 "YYYY-MM-DD HH:MM 至 YYYY-MM-DD HH:MM"
        return f"{obj.observation_start_time.strftime('%Y-%m-%d %H:%M')} 至 {obj.observation_end_time.strftime('%Y-%m-%d %H:%M')}"

# 提供详细信息
class RecordFullSerializer(RecordBasicSerializer):
    details = RecordDetailReadSerializer(read_only=True)
    comments = CommentSerializer(many=True, read_only=True)

    class Meta(RecordBasicSerializer.Meta):
        fields = RecordBasicSerializer.Meta.fields + ['details', 'comments', 'updated_at']

# 用于前端请求创建记录的序列化器
class RecordCreateSerializer(serializers.ModelSerializer):
    record_user = serializers.CharField(write_only=True, required=False)
    observation_time = serializers.CharField(write_only=True)
    # details 是一个 JSON 对象，使用 JSONField 接收，然后在 create 方法中手动处理
    details = serializers.JSONField(write_only=True)

    class Meta:
        model = Record
        fields = [
            'record_identifier', 'observation_time', 'observation_address',
            'record_user', 'bird_count', 'details'
        ]

    def create(self, validated_data):
        # 提取并移除嵌套数据
        observation_time_str = validated_data.pop('observation_time')
        record_user_from_frontend = validated_data.pop('record_user', None)
        details_data = validated_data.pop('details')
        species_counts_data = details_data.pop('species_count', []) # 确保即使没有 species_count 也不会报错

        try:
            start_time_str, end_time_str = observation_time_str.split(' 至 ')
            validated_data['observation_start_time'] = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M')
            validated_data['observation_end_time'] = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M')
        except ValueError:
            raise serializers.ValidationError(
                {"observation_time": "Invalid time format. Expected 'YYYY-MM-DD HH:MM 至 YYYY-MM-DD HH:MM'"}
            )

        # 确保 user 字段被设置为当前认证用户
        request_user = self.context['request'].user
        if not request_user.is_authenticated:
            raise serializers.ValidationError({"detail": "Authentication required to create a record."})
        validated_data['user'] = request_user # 覆盖或设置user字段

        # 创建 Record 实例
        record = Record.objects.create(**validated_data)

        # 确保 details_data 只包含 RecordDetail 模型的字段
        record_detail = RecordDetail.objects.create(record=record, **details_data)

        # 创建 SpeciesCount 实例列表
        for species_data in species_counts_data:
            # 映射 JSON 字段名到模型字段名
            SpeciesCount.objects.create(
                record_detail=record_detail,
                count_id=species_data.get('count_id'),
                china_name=species_data.get('china_name'),
                order_name=species_data.get('目'),
                family_name=species_data.get('科'),
                count=species_data.get('count')
            )

        return record

    def to_representation(self, instance):
        return RecordFullSerializer(instance, context=self.context).data