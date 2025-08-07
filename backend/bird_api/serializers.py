from rest_framework import serializers
from .models import Record, RecordDetail, SpeciesCount, Comment
from django.contrib.auth.models import User
from datetime import datetime

# 辅助序列化器，用于显示用户的部分信息
class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username']
        ref_name = 'RecordUser'

# 用于处理 RecordDetail 内部的 species_count 列表
class SpeciesCountSerializer(serializers.ModelSerializer):
    # 注意：JSON 中的 "目" 和 "科" 需要映射到模型中的字段
    order = serializers.CharField(source='order_name') # 将 'order' 字段映射到模型 'order_name'
    family = serializers.CharField(source='family_name') # 将 'family' 字段映射到模型 'family_name'

    class Meta:
        model = SpeciesCount
        fields = ['count_id', 'china_name', 'order', 'family', 'count']
        read_only_fields = ['record_detail'] # 此字段在创建时由上层序列化器设置

# 用于读取 RecordDetail 及其嵌套的物种统计
class RecordDetailReadSerializer(serializers.ModelSerializer):
    # 使用 source='species_counts' 指向 RecordDetail 模型中定义的反向关系名
    species_count = SpeciesCountSerializer(source='species_counts', many=True, read_only=True)

    class Meta:
        model = RecordDetail
        fields = ['basic_counts', 'longitude', 'latitude', 'species_count']

# 用于评论
class CommentSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True) # 评论的用户只读

    class Meta:
        model = Comment
        fields = ['id', 'record', 'user', 'text', 'created_at']
        read_only_fields = ['record', 'user'] # record 和 user 在视图中设置

# 接口一：提供基本信息 (除 details 以外)
class RecordBasicSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True) # 显示记录用户
    # observation_time 在模型中是两个字段，这里通过方法序列化为前端期望的格式
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

# 接口二：提供详细信息 (所有信息以及评论)
class RecordFullSerializer(RecordBasicSerializer): # 继承基本信息序列化器
    details = RecordDetailReadSerializer(read_only=True) # 嵌套详细信息
    comments = CommentSerializer(many=True, read_only=True) # 嵌套评论列表

    class Meta(RecordBasicSerializer.Meta): # 继承基本信息的 Meta
        # 在基本信息字段的基础上，添加 details, comments, updated_at
        fields = RecordBasicSerializer.Meta.fields + ['details', 'comments', 'updated_at']

# 用于前端 POST 请求创建记录的序列化器
class RecordCreateSerializer(serializers.ModelSerializer):
    # 前端发送的 record_user 字段（字符串），仅用于写入，实际用户从request中获取
    record_user = serializers.CharField(write_only=True, required=False) # 尽管前端提供，但我们用request.user
    # observation_time 是一个字符串，需要解析
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
        # 提取并移除嵌套数据，以便主 Record 模型可以创建
        observation_time_str = validated_data.pop('observation_time')
        record_user_from_frontend = validated_data.pop('record_user', None) # 存储前端传来的用户名，但实际使用request.user
        details_data = validated_data.pop('details')
        species_counts_data = details_data.pop('species_count', []) # 确保即使没有 species_count 也不会报错

        # 解析 observation_time 字符串为两个 DateTimeField
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

        # 创建 RecordDetail 实例
        # 确保 details_data 只包含 RecordDetail 模型的字段
        record_detail = RecordDetail.objects.create(record=record, **details_data)

        # 创建 SpeciesCount 实例列表
        for species_data in species_counts_data:
            # 映射 JSON 字段名到模型字段名
            SpeciesCount.objects.create(
                record_detail=record_detail,
                count_id=species_data.get('count_id'),
                china_name=species_data.get('china_name'),
                order_name=species_data.get('目'), # 注意：JSON键是"目"
                family_name=species_data.get('科'), # 注意：JSON键是"科"
                count=species_data.get('count')
            )

        return record

    def to_representation(self, instance):
        # 创建成功后，返回完整记录的表示形式，以便前端获取新创建的记录的完整信息
        return RecordFullSerializer(instance, context=self.context).data