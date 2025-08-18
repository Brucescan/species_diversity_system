import requests
from drf_yasg.utils import swagger_auto_schema
from django.conf import settings
from drf_yasg import openapi
from django.contrib.auth import get_user_model
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authentication import TokenAuthentication
from rest_framework.views import APIView
from django.contrib.auth import authenticate
from .serializers import UserSerializer
from .models import UserProfile

import logging

User = get_user_model()
logger = logging.getLogger(__name__)


@swagger_auto_schema(
    method='post',
    request_body=UserSerializer,
    responses={201: openapi.Response('Registration successful', examples={'application/json': {'code': 201, 'token': 'string'}})},
)
@api_view(['POST'])
@permission_classes([AllowAny])
def register(request):
    serializer = UserSerializer(data=request.data)
    if serializer.is_valid():
        user = serializer.save()
        token, created = Token.objects.get_or_create(user=user)
        return Response({'code':201,'token': token.key}, status=status.HTTP_201_CREATED)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class LoginView(APIView):
    authentication_classes = [TokenAuthentication]
    permission_classes = [AllowAny]

    @swagger_auto_schema(
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'username': openapi.Schema(type=openapi.TYPE_STRING),
                'password': openapi.Schema(type=openapi.TYPE_STRING),
            },
            required=['username', 'password'],
        ),
        responses={
            201: openapi.Response('Login successful', examples={'application/json': {'code': 201, 'token': 'string', 'user_id': 'integer', 'email': 'string'}}),
            400: openapi.Response('Invalid credentials'),
        },
    )
    def post(self, request, *args, **kwargs):
        username = request.data.get('username')
        password = request.data.get('password')
        print(f"Attempting to authenticate user: {username}")

        user = authenticate(username=username, password=password)
        print(f"User found: {user}")  # 调试
        print(f"Is active: {user.is_active if user else 'No user'}")

        if user is not None:
            token, created = Token.objects.get_or_create(user=user)
            return Response({
                'code': 201,
                'token': token.key,
                'user_id': user.pk,
                'email': user.email,
                'username':user.username,
            })
        return Response(
            {'error': 'Invalid Credentials'},
            status=status.HTTP_400_BAD_REQUEST
        )


@swagger_auto_schema(
    method='post',
    responses={200: openapi.Response('Logout successful', examples={'application/json': {'code': 201, 'message': 'success'}})},
)
@api_view(['POST'])
@permission_classes([IsAuthenticated])
def logout(request):
    request.user.auth_token.delete()
    return Response({'code':201,'message':'sucess'},status=status.HTTP_200_OK)

class GetCurrentUserView(APIView):
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        responses={200: openapi.Response('Current user data', examples={'application/json': {'code': 200, 'data': {'id': 'integer', 'username': 'string', 'email': 'string'}}})},
    )
    def get(self, request, *args, **kwargs):
        try:
            serializer = UserSerializer(request.user)
            return Response({'code':200,'data':serializer.data},status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@swagger_auto_schema(
    method='get',
    manual_parameters=[
        openapi.Parameter('q', openapi.IN_QUERY, description="Search query", type=openapi.TYPE_STRING),
    ],
    responses={200: openapi.Response('Search results', examples={'application/json': {'code': 201, 'data': [{'id': 'integer', 'username': 'string', 'email': 'string'}]}})},
)
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def search_users(request):
    query = request.query_params.get('q', '')
    users = User.objects.filter(username__icontains=query)
    serializer = UserSerializer(users, many=True)
    return Response({"code":201,'data':serializer.data},status=status.HTTP_200_OK)

@swagger_auto_schema(
    method='delete',
    responses={204: openapi.Response('User deleted', examples={'application/json': {'code': 201, 'message': 'User deleted'}})},
)
@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def delete_user(request):
    user = request.user
    user.delete()
    return Response({'code':201,'message':'User deleted'},status=status.HTTP_204_NO_CONTENT)


@swagger_auto_schema(
    method='post',
    request_body=openapi.Schema(
        type=openapi.TYPE_OBJECT,
        properties={'code': openapi.Schema(type=openapi.TYPE_STRING, description="微信登录凭证code")},
        required=['code'],
    ),
    responses={
        200: openapi.Response(
            'WeChat Login/Register success',
            examples={
                'application/json': {
                    'code': 200,
                    'msg': '登录成功',
                    'data': {
                        'token': 'your_auth_token',
                        'user_id': 1,
                        'username': 'wechat_user_123',
                        'nickname': '微信用户',
                        'avatarUrl': 'http://example.com/avatar.jpg'
                    }
                }
            }
        ),
        400: openapi.Response('Invalid code or WeChat API error'),
    },
)
@api_view(['POST'])
@permission_classes([AllowAny])
def wechat_login(request):
    code = request.data.get('code')
    if not code:
        return Response({"code": 400, "msg": "缺少code参数"}, status=status.HTTP_400_BAD_REQUEST)

    APPID = settings.WECHAT_APPID
    APPSECRET = settings.WECHAT_APPSECRET
    wechat_api_url = f"https://api.weixin.qq.com/sns/jscode2session?appid={APPID}&secret={APPSECRET}&js_code={code}&grant_type=authorization_code"

    try:
        response = requests.get(wechat_api_url)
        response.raise_for_status()
        wechat_data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"微信API请求失败: {e}")
        return Response({"code": 500, "msg": f"微信服务异常: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    if 'errcode' in wechat_data:
        logger.error(f"微信API返回错误: {wechat_data.get('errmsg', '未知错误')}, code: {wechat_data.get('errcode')}")
        return Response({"code": 400, "msg": f"微信授权失败: {wechat_data.get('errmsg', '未知错误')}"},
                        status=status.HTTP_400_BAD_REQUEST)

    openid = wechat_data.get('openid')
    session_key = wechat_data.get('session_key')
    unionid = wechat_data.get('unionid')

    if not openid:
        return Response({"code": 400, "msg": "未能从微信获取openid"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        user_profile = UserProfile.objects.get(openid=openid)
        user = user_profile.user

        user_profile.session_key = session_key
        user_profile.save()

        logger.info(f"Existing WeChat user logged in: {user.username}")

    except UserProfile.DoesNotExist:
        logger.info(f"New WeChat user detected with openid: {openid}, creating new account.")

        username = f"wechat_{openid[:20]}"

        i = 1
        original_username = username
        while User.objects.filter(username=username).exists():
            username = f"{original_username}_{i}"
            i += 1

        try:
            user = User.objects.create_user(
                username=username,
                password='!',
                email=''
            )
            user_profile = UserProfile.objects.create(
                user=user,
                openid=openid,
                unionid=unionid,
                session_key=session_key
            )
            logger.info(f"New User and UserProfile created for openid: {openid}")
        except Exception as e:
            logger.error(f"创建微信用户失败: {e}")
            return Response({"code": 500, "msg": f"用户注册失败: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    token, created = Token.objects.get_or_create(user=user)

    user_info_data = {
        "user_id": user.pk,
        "username": user.username,
        "email": user.email,
        "nickname": user_profile.nickname,
        "avatarUrl": user_profile.avatar_url,
        "gender": user_profile.gender,
        # ... 任何其他你希望返回的用户信息
    }

    return Response({
        "code": 200,
        "msg": "登录成功",
        "data": {
            "token": token.key,
            "userInfo": user_info_data
        }
    }, status=status.HTTP_200_OK)


@swagger_auto_schema(
    method='post',
    request_body=openapi.Schema(
        type=openapi.TYPE_OBJECT,
        properties={
            'nickname': openapi.Schema(type=openapi.TYPE_STRING, description="用户昵称"),
            'avatarUrl': openapi.Schema(type=openapi.TYPE_STRING, description="用户头像URL"),
            'gender': openapi.Schema(type=openapi.TYPE_INTEGER, description="性别 (0未知, 1男, 2女)"),
        },
        required=['nickname', 'avatarUrl'],
    ),
    responses={
        200: openapi.Response(
            'Profile update successful',
            examples={
                'application/json': {
                    'code': 200,
                    'msg': '资料更新成功',
                    'data': {
                        'nickname': '新昵称',
                        'avatarUrl': 'http://new_avatar.com',
                        'gender': 1
                    }
                }
            }
        ),
        400: 'Bad request',
    },
)
@api_view(['POST'])
@permission_classes([IsAuthenticated])
def update_wechat_profile(request):
    user = request.user
    nickname = request.data.get('nickname')
    avatar_url = request.data.get('avatarUrl')
    gender = request.data.get('gender')

    try:
        user_profile = user.profile
    except UserProfile.DoesNotExist:
        return Response({"code": 500, "msg": "用户资料模型异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    if nickname:
        user_profile.nickname = nickname
    if avatar_url:
        user_profile.avatar_url = avatar_url
    if gender is not None:
        user_profile.gender = gender

    user_profile.save()
    logger.info(f"User {user.username} (openid: {user_profile.openid}) updated profile.")

    return Response({
        "code": 200,
        "msg": "资料更新成功",
        "data": {
            "nickname": user_profile.nickname,
            "avatarUrl": user_profile.avatar_url,
            "gender": user_profile.gender
        }
    }, status=status.HTTP_200_OK)