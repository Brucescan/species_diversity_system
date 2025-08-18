from django.db import models
from django.contrib.auth.models import User

class Record(models.Model):
    """
    鸟类观察记录的主表，包含基本信息
    """
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='bird_records',
        verbose_name="记录用户"
    )
    record_identifier = models.CharField(max_length=50, unique=True, verbose_name="记录唯一标识")
    observation_start_time = models.DateTimeField(verbose_name="观察开始时间")
    observation_end_time = models.DateTimeField(verbose_name="观察结束时间")
    observation_address = models.CharField(max_length=255, verbose_name="观察地址")
    bird_count = models.IntegerField(verbose_name="鸟类总数") # 对应 JSON 中的 bird_count
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    class Meta:
        verbose_name = "鸟类观察记录"
        verbose_name_plural = "鸟类观察记录"
        ordering = ['-created_at'] # 默认按创建时间倒序排列

    def __str__(self):
        return f"Record {self.record_identifier} by {self.user.username}"

class RecordDetail(models.Model):
    """
    记录的详细信息，与Record模型一对一关联
    """
    record = models.OneToOneField(
        Record,
        on_delete=models.CASCADE,
        related_name='details',
        verbose_name="所属记录"
    )
    basic_counts = models.CharField(max_length=255, verbose_name="基本统计")
    longitude = models.FloatField(verbose_name="经度")
    latitude = models.FloatField(verbose_name="纬度")

    class Meta:
        verbose_name = "记录详情"
        verbose_name_plural = "记录详情"

    def __str__(self):
        return f"Detail for Record {self.record.record_identifier}"

class SpeciesCount(models.Model):
    """
    记录详情中的物种统计列表
    """
    record_detail = models.ForeignKey(
        RecordDetail,
        on_delete=models.CASCADE,
        related_name='species_counts',
        verbose_name="所属记录详情"
    )
    count_id = models.IntegerField(verbose_name="统计ID")
    china_name = models.CharField(max_length=100, verbose_name="中文名")
    order_name = models.CharField(max_length=100, verbose_name="目")
    family_name = models.CharField(max_length=100, verbose_name="科")
    count = models.IntegerField(verbose_name="数量")

    class Meta:
        verbose_name = "物种统计"
        verbose_name_plural = "物种统计"
        unique_together = ('record_detail', 'count_id')
        ordering = ['count_id']

    def __str__(self):
        return f"{self.china_name} ({self.count}) on {self.record_detail.record.record_identifier}"

class Comment(models.Model):
    """
    用户对记录的评论
    """
    record = models.ForeignKey(
        Record,
        on_delete=models.CASCADE,
        related_name='comments',
        verbose_name="所属记录"
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='comments_made',
        verbose_name="评论用户"
    )
    text = models.TextField(verbose_name="评论内容")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="评论时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    class Meta:
        verbose_name = "评论"
        verbose_name_plural = "评论"
        ordering = ['created_at']

    def __str__(self):
        return f"Comment by {self.user.username} on Record {self.record.record_identifier}"
