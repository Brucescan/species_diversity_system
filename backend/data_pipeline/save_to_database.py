from django.db import transaction
import django
import os
from collections import defaultdict
from django.contrib.gis.geos import Point
from datetime import datetime

# 设置 Django 环境变量
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()
from data_pipeline.models import BirdObservation, BirdSpeciesRecord, AQIStation, AQIRecord


def consumer(queue):
    # 批量插入缓冲区
    buffers = {
        "bird_observations": [],  # 存储鸟类观测主记录
        "bird_species": [],  # 存储鸟类物种记录
        "aqi": []  # 存储AQI数据
    }
    BATCH_SIZE = 100
    completed_producers = set()
    station_cache = {}  # 缓存监测站信息

    while True:
        resp = queue.get()
        # print(f"收到数据: {resp}")

        # 检查结束信号
        if resp in ["空气质量数据抓取完毕", "鸟类数据抓取完毕"]:
            completed_producers.add(resp)
            print(f"收到完成信号: {resp}")

            # 检查是否所有生产者都完成
            if len(completed_producers) >= 2:
                print("所有生产者已完成，处理剩余数据...")
                process_remaining_data(buffers, station_cache)
                break
            continue

        # 数据处理逻辑
        data_type = resp["type"]
        data = resp["data"]

        try:
            if data_type == "bird":
                process_bird_data(data, buffers)
            elif data_type == "AQI":
                process_aqi_data(data, buffers, station_cache)

            # 检查批量插入
            check_batch_insert(buffers, BATCH_SIZE, station_cache)

        except Exception as e:
            print(f"处理{data_type}数据时出错: {e}")
            continue

    print("消费者进程正常结束")


def process_remaining_data(buffers, station_cache):
    """处理缓冲区剩余数据"""
    with transaction.atomic():
        # 处理剩余的鸟类观测数据
        if buffers["bird_observations"]:
            BirdObservation.objects.bulk_create(buffers["bird_observations"])
            print(f"插入最后一批鸟类观测数据，共{len(buffers['bird_observations'])}条")

        # 处理剩余的鸟类物种数据
        if buffers["bird_species"]:
            BirdSpeciesRecord.objects.bulk_create(buffers["bird_species"])
            print(f"插入最后一批鸟类物种数据，共{len(buffers['bird_species'])}条")

        # 处理剩余的AQI数据
        if buffers["aqi"]:
            AQIRecord.objects.bulk_create(buffers["aqi"])
            print(f"插入最后一批AQI数据，共{len(buffers['aqi'])}条")


def process_bird_data(raw_data, buffers):
    """处理鸟类观测数据"""
    # 准备观测记录数据
    observation = BirdObservation(
        address=raw_data['address'],
        start_time=datetime.strptime(raw_data['startTime'], '%Y-%m-%d %H:%M'),
        end_time=datetime.strptime(raw_data['endTime'], '%Y-%m-%d %H:%M'),
        taxon_count=raw_data['taxonCount'],
        serial_id=raw_data['serialId'],
        location=Point(float(raw_data['longitude']), float(raw_data['latitude'])),
        raw_data=raw_data
    )
    buffers["bird_observations"].append(observation)

    # 准备物种记录数据
    for species in raw_data['species']:
        species_record = BirdSpeciesRecord(
            observation=observation,  # 注意：这里需要先保存observation才能使用
            taxon_id=species['taxon_id'],
            taxon_name=species['taxon_name'],
            latin_name=species['latinname'],
            taxon_order=species['taxonordername'],
            taxon_family=species['taxonfamilyname'],
            count=species['taxon_count'],
            has_images=species['record_image_num'] > 0,
            outside_type=species['outside_type'],
            activity_id=species['activity_id']
        )
        buffers["bird_species"].append(species_record)


def process_aqi_data(raw_data, buffers, station_cache):
    """处理AQI数据"""
    station_name = raw_data['stationName']

    # 从缓存获取或创建监测站
    if station_name not in station_cache:
        station, _ = AQIStation.objects.get_or_create(
            name=station_name,
            defaults={
                'location': Point(float(raw_data['longitude']), float(raw_data['latitude']))
            }
        )
        station_cache[station_name] = station
    else:
        station = station_cache[station_name]

    # 转换时间戳
    timestamp = datetime.fromtimestamp(int(raw_data['timeStamp']) / 1000)

    # 创建AQI记录
    aqi_record = AQIRecord(
        station=station,
        timestamp=timestamp,
        aqi=0 if raw_data['AQI']=="-" else float(raw_data['AQI']),
        timestr=raw_data["timePointStr"],
        description=raw_data["description"],
        measure=raw_data["measure"],
        quality=raw_data['quality'] if raw_data['quality'] != '—' else None,
        co=raw_data['CO'],
        no2=raw_data['NO2'],
        o3=raw_data['O3'],
        pm10=raw_data['PM10'],
        pm25=raw_data['PM2.5'],
        so2=raw_data['SO2'],
        raw_data=raw_data
    )
    buffers["aqi"].append(aqi_record)


def check_batch_insert(buffers, batch_size, station_cache):
    """检查并执行批量插入"""
    with transaction.atomic():
        # 处理鸟类观测数据
        if len(buffers["bird_observations"]) >= batch_size:
            # 先保存观测记录
            BirdObservation.objects.bulk_create(buffers["bird_observations"])
            print(f"批量插入鸟类观测数据，共{len(buffers['bird_observations'])}条")

            # 然后保存物种记录（需要先有observation的id）
            for species in buffers["bird_species"]:
                species.observation_id = species.observation.id
            BirdSpeciesRecord.objects.bulk_create(buffers["bird_species"])
            print(f"批量插入鸟类物种数据，共{len(buffers['bird_species'])}条")

            buffers["bird_observations"] = []
            buffers["bird_species"] = []

        # 处理AQI数据
        if len(buffers["aqi"]) >= batch_size:
            AQIRecord.objects.bulk_create(buffers["aqi"])
            print(f"批量插入AQI数据，共{len(buffers['aqi'])}条")
            buffers["aqi"] = []
