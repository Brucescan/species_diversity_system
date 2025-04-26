from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # path("admin/", admin.site.urls),
    path("api/users/", include("user_api.urls")),
    path("api/birds/",include("bird_api.urls")),
    path("api/aqi/",include("aqi_api.urls"))
]