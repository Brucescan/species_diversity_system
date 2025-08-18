from django.db import models
from django.contrib.gis.db import models as gis_models
from django.db.models import JSONField
class BirdObservation(models.Model):
    """
    鸟类观测记录主表
    """
    address = models.CharField(max_length=255, verbose_name="观测地点")
    start_time = models.DateTimeField(verbose_name="开始时间")
    end_time = models.DateTimeField(verbose_name="结束时间")
    taxon_count = models.IntegerField(verbose_name="物种总数")
    serial_id = models.CharField(max_length=50, unique=False, verbose_name="记录编号")

    location = gis_models.PointField(verbose_name="坐标点")

    raw_data = JSONField(null=True, blank=True, verbose_name="原始数据")

    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    class Meta:
        verbose_name = "鸟类观测记录"
        verbose_name_plural = verbose_name
        indexes = [
            models.Index(fields=['serial_id']),
            models.Index(fields=['start_time']),
        ]

    def __str__(self):
        return f"{self.address} ({self.start_time.date()})"


class BirdSpeciesRecord(models.Model):
    observation = models.ForeignKey(
        BirdObservation,
        on_delete=models.CASCADE,
        related_name='species_records',
        verbose_name="观测记录"
    )

    # 分类信息
    taxon_id = models.IntegerField(verbose_name="物种ID")
    taxon_name = models.CharField(max_length=100, verbose_name="中文名")
    latin_name = models.CharField(max_length=100, verbose_name="拉丁名")
    taxon_order = models.CharField(max_length=50, verbose_name="目")
    taxon_family = models.CharField(max_length=50, verbose_name="科")

    # 观测详情
    count = models.IntegerField(default=1, verbose_name="数量")
    has_images = models.BooleanField(default=False, verbose_name="是否有照片")
    outside_type = models.IntegerField(default=0, verbose_name="外来类型")

    # 活动信息
    activity_id = models.IntegerField(null=True, blank=True, verbose_name="活动ID")

    class Meta:
        verbose_name = "鸟类物种记录"
        verbose_name_plural = verbose_name
        unique_together = [['observation', 'taxon_id']]
        app_label = 'data_pipeline'

    def __str__(self):
        return f"{self.taxon_name} ({self.latin_name})"


class AQIStation(models.Model):
    """空气质量监测站基础信息"""
    name = models.CharField(max_length=100, verbose_name="监测站名称")
    location = gis_models.PointField(verbose_name="坐标位置")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    class Meta:
        verbose_name = "空气质量监测站"
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.name


class AQIRecord(models.Model):
    """空气质量实时记录"""
    station = models.ForeignKey(
        AQIStation,
        on_delete=models.CASCADE,
        related_name='records',
        verbose_name="监测站"
    )
    timestamp = models.DateTimeField(verbose_name="记录时间")
    aqi = models.FloatField(null=True, blank=True, verbose_name="AQI指数")
    quality = models.CharField(max_length=50, null=True, blank=True, verbose_name="空气质量等级")
    description = models.CharField(max_length=50,null=True,verbose_name="健康指引")
    measure = models.CharField(max_length=50,null=True,verbose_name="描述")
    timestr = models.CharField(max_length=20,null=True,verbose_name="时间描述")

    # 污染物浓度
    co = models.CharField(null=True, blank=True, verbose_name="CO(mg/m³)")
    no2 = models.CharField(null=True, blank=True, verbose_name="NO2(μg/m³)")
    o3 = models.CharField(null=True, blank=True, verbose_name="O3(μg/m³)")
    pm10 = models.CharField(null=True, blank=True, verbose_name="PM10(μg/m³)")
    pm25 = models.CharField(null=True, blank=True, verbose_name="PM2.5(μg/m³)")
    so2 = models.CharField(null=True, blank=True, verbose_name="SO2(μg/m³)")

    # 原始数据备份
    raw_data = models.JSONField(default=dict, verbose_name="原始数据")

    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")

    class Meta:
        verbose_name = "空气质量记录"
        verbose_name_plural = verbose_name
        indexes = [
            models.Index(fields=['station', 'timestamp']),
            models.Index(fields=['timestamp']),
        ]
        ordering = ['-timestamp']
        app_label = 'data_pipeline'

    def __str__(self):
        return f"{self.station.name} - {self.timestamp}"
