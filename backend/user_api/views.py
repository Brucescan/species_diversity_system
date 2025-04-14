from drf_yasg.utils import swagger_auto_schema
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
import logging

User = get_user_model()


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
        token, created = Token.objects.get_or_create(user=user)  # 确保 Token 表已正确迁移
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
        user = authenticate(username=username, password=password)
        if user is not None:
            token, created = Token.objects.get_or_create(user=user)
            return Response({'code':201,'token': token.key, 'user_id': user.pk, 'email': user.email})
        return Response({'error': 'Invalid Credentials'}, status=status.HTTP_400_BAD_REQUEST)

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
        logger = logging.getLogger(__name__)  # 初始化日志记录器
        try:
            serializer = UserSerializer(request.user)
            # logger.debug(f"Current user: {request.user}")
            # logger.debug(f"Serialized data: {serializer.data}")
            return Response({'code':200,'data':serializer.data},status=status.HTTP_200_OK)
        except Exception as e:
            # logger.error(f"Exception in GetCurrentUserView: {str(e)}")
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