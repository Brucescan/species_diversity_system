from django.db import models
from django.contrib.auth.models import User

class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile') #

    # 微信相关的字段
    openid = models.CharField(max_length=50, unique=True, null=True, blank=True, verbose_name="微信openid")
    unionid = models.CharField(max_length=50, unique=True, null=True, blank=True, verbose_name="微信unionid")
    nickname = models.CharField(max_length=100, null=True, blank=True, verbose_name="昵称")
    avatar_url = models.URLField(max_length=255, null=True, blank=True, verbose_name="头像")
    gender = models.SmallIntegerField(null=True, blank=True, choices=((0, '未知'), (1, '男'), (2, '女')), verbose_name="性别")
    session_key = models.CharField(max_length=100, null=True, blank=True, verbose_name="微信session_key")

    class Meta:
        verbose_name = '用户扩展信息'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.user.username}'s Profile"