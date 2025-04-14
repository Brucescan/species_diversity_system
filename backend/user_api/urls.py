from django.conf.urls.static import static
from django.urls import path, re_path
from backend import settings
from .views import register, LoginView, logout, GetCurrentUserView, search_users, delete_user
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi

# 创建 Schema View 配置
schema_view = get_schema_view(
    openapi.Info(
        title="用户文档",
        default_version='v1',
        description="用户接口文档",
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),  # 权限设置
)
urlpatterns = [
    path('register/', register, name='register'),
    path('login/', LoginView.as_view(), name='login'),  # 修改为使用 LoginView 类
    path('logout/', logout, name='logout'),
    path('current/', GetCurrentUserView.as_view(), name='get_current_user'),  # 修改为使用 GetCurrentUserView 类
    path('search/', search_users, name='search_users'),
    path('delete/', delete_user, name='delete_user'),

    # Swagger 文档
    re_path(r'^swagger(?P<format>\.json|\.yaml)$', schema_view.without_ui(cache_timeout=0), name='schema-json'),
    path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
]

