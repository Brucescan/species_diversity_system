from rest_framework import serializers
from django.contrib.auth import get_user_model

from user_api.models import UserProfile

User = get_user_model()

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('username', 'password', 'email')  # 或其他字段
        extra_kwargs = {'password': {'write_only': True}}
        ref_name = 'UserRegister'

    def create(self, validated_data):
        user = User.objects.create_user(
            username=validated_data['username'],
            password=validated_data['password'],
            email=validated_data.get('email', '')
        )
        return user

class UserProfileSerializer(serializers.ModelSerializer):
    username = serializers.CharField(source='user.username', read_only=True)
    email = serializers.EmailField(source='user.email', read_only=True)

    class Meta:
        model = UserProfile
        fields = (
            'id', 'username', 'email', 'openid', 'unionid',
            'nickname', 'avatar_url', 'gender'
        )
        read_only_fields = ('id', 'openid', 'unionid', 'session_key')
        ref_name = 'UserProfile' # 防止和UserSerializer的ref_name冲突