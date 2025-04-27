import os  # 添加此行以导入 os 模块


SECRET_KEY = 'l1(5s5y%@(zi!vrhf3!sd)h8cyb%=5bbzq^l4bcto1h*vwo+ab'  # 添加此行以设置 SECRET_KEY
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEBUG = True
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    'user_api',
    'rest_framework.authtoken',
    'corsheaders',  # 添加此行以启用 CORS 中间件
    'rest_framework',
    'drf_yasg',
    'data_pipeline',
    'django.contrib.gis',
    'aqi_api',
    'bird_api',
]

ALLOWED_HOSTS = ['localhost', '127.0.0.1', 'your_domain.com']  # 根据实际情况添加域名或IP地址

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'corsheaders.middleware.CorsMiddleware',  # 添加此行以启用 CORS 中间件
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'backend.urls'  # 添加此行以指定根URL配置模块

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.TokenAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
}

# LOGGING = {
#     'version': 1,
#     'disable_existing_loggers': False,
#     'handlers': {
#         'console': {
#             'class': 'logging.StreamHandler',
#         },
#     },
#     'loggers': {
#         'django': {
#             'handlers': ['console'],
#             'level': 'DEBUG',
#         },
#         'user_api': {
#             'handlers': ['console'],
#             'level': 'DEBUG',
#             'propagate': False,
#         },
#     },
# }
# docker数据库配置
DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': os.getenv('POSTGRES_DB', 'mydb'),
        'USER': os.getenv('POSTGRES_USER', 'postgres'),
        'PASSWORD': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'HOST': os.getenv('POSTGRES_HOST', 'db'),  # 修改为 Docker Compose 中的服务名
        'PORT': os.getenv('POSTGRES_PORT', '5432'),
    }
}
STATIC_URL = '/static/'
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, "static"),
]
STATIC_ROOT = os.path.join(BASE_DIR, 'static_collected')
# STATICFILES_STORAGE = 'django.contrib.staticfiles.storage.StaticFilesStorage'
# 本地开发数据配置
# DATABASES = {
#     'default': {
#         'ENGINE': 'django.contrib.gis.db.backends.postgis',
#         'NAME': 'mydb',     # 数据库名称
#         'USER': 'postgres',      # 数据库用户名（默认可能是 postgres）
#         'PASSWORD': '123456', # 用户密码
#         'HOST': 'localhost',         # 数据库地址（本地为 localhost 或 127.0.0.1）
#         'PORT': '5432',              # 默认端口 5432
#     }
# }

# GDAL_LIBRARY_PATH = r'E:\anaconda3\envs\backend\Library\bin\gdal.dll'  # 替换为实际路径
# GEOS_LIBRARY_PATH = r'E:\anaconda3\envs\backend\Library\bin\geos_c.dll'

# 添加 CORS 配置
CORS_ORIGIN_ALLOW_ALL = True
CORS_ALLOW_CREDENTIALS = True

