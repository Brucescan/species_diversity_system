"""
用来更新数据库中的数据
"""
import django
import os
from django.utils import timezone
from datetime import timedelta
from datetime import datetime
from django.db import transaction
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()
from data_pipeline.models import AQIStation, AQIRecord

class UpDateAQI:
    def __init__(self):
        self.url = "https://air.cnemc.cn:18007/CityData/GetAQIDataPublishLive"
        self.headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en,zh-CN;q=0.9,zh;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "connection": "keep-alive",
            "host": "air.cnemc.cn:18007",
            "referer": "https://air.cnemc.cn:18007/",
            "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }
        self.batch_size = 100  # 每批处理100条数据
        self.batch_buffer = []  # 批量插入缓冲区

    def get_data(self):
        params = {
            "cityName": "北京市",
        }
        resp = requests.get(self.url, headers=self.headers, params=params)
        stations_list = resp.json()
        history_station_data = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(self.get_station_history,station["StationCode"]) for station in stations_list]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    history_station_data.append(result)
                except Exception as e:
                    print(f"An error occurred: {e}")
        return self.process_data(history_station_data)

    def get_station_history(self, station_code):
        history_params = {
            "stationCode": station_code,
        }
        # print(history_params)
        history_resp = requests.post("https://air.cnemc.cn:18007/HourChangesPublish/GetAqiHistoryByCondition",
                                     headers=self.headers, params=history_params)
        return history_resp.text

    def process_data(self, all_data):
        processed_data = []
        for station in all_data:
            for one_time in eval(station):
                one_data = {}
                one_data["stationName"] = one_time["PositionName"]
                one_data["timePointStr"] = one_time["TimePointStr"]
                one_data["timeStamp"] = one_time["TimePoint"].replace("\\", "").replace("/", "").replace("Date(","").replace(")","")
                one_data["longitude"] = one_time["Longitude"]
                one_data["latitude"] = one_time["Latitude"]
                one_data["measure"] = one_time["Measure"]
                one_data["quality"] = one_time["Quality"]
                one_data["description"] = one_time["Unheathful"]
                one_data["AQI"] = one_time["AQI"]
                one_data["CO"] = str(one_time["COLevel"]) + "," + str(one_time["CO"]) + "," + str(one_time["CO_24h"])
                one_data["NO2"] = str(one_time["NO2Level"]) + "," + str(one_time["NO2"]) + "," + str(one_time["NO2_24h"])
                one_data["O3"] = str(one_time["O3Level"]) + "," + str(one_time["O3_8hLevel"]) + "," + str(one_time["O3"]) + "," + str(one_time["O3_8h"]) + "," + str(one_time["O3_24h"])
                one_data["PM10"] = str(one_time["PM10Level"]) + "," + str(one_time["PM10"]) + "," + str(one_time["PM10_24h"])
                one_data["PM2.5"] = str(one_time["PM2_5Level"]) + "," + str(one_time["PM2_5"]) + "," + str(one_time["PM2_5_24h"])
                one_data["SO2"] = str(one_time["SO2Level"]) + "," + str(one_time["SO2"]) + "," + str(one_time["SO2_24h"])
                print(f"{one_data['timePointStr']}抓取完毕")
                processed_data.append(one_data)
        return processed_data

    @transaction.atomic
    def update_database(self):
        """更新数据库，处理批量插入和过期数据清理"""
        new_data = self.get_data()
        print(f"获取到{len(new_data)}条新数据")

        # 处理每条数据
        for station_data in new_data:
            self._process_station_data(station_data)

            # 检查是否达到批量插入条件
            if len(self.batch_buffer) >= self.batch_size:
                self._batch_insert()

        # 处理剩余数据
        if self.batch_buffer:
            self._batch_insert()

        # 清理过期数据
        self._clean_old_data()

    def _process_station_data(self, station_data):
        """处理单个站点数据"""
        try:
            # 解析数据
            station_name = station_data['stationName']
            # 1. 从原始 timestamp (毫秒) 计算秒数
            unix_timestamp_sec = int(station_data['timeStamp']) / 1000

            # 2. 使用 utcfromtimestamp 创建一个代表 UTC 时间的 naive datetime 对象
            naive_utc_dt = datetime.utcfromtimestamp(unix_timestamp_sec)

            # 3. 使用 timezone.make_aware 将其转换为带 UTC 时区的 aware datetime 对象
            aware_utc_dt = timezone.make_aware(naive_utc_dt, timezone.utc)

            # 将转换后的 aware 对象赋值给 timestamp 变量
            timestamp = aware_utc_dt
            # 检查是否已存在
            if not AQIRecord.objects.filter(
                    station__name=station_name,
                    timestamp=timestamp
            ).exists():
                # 准备数据
                record_data = {
                    'station_name': station_name,
                    'timestamp': timestamp,
                    'aqi': float(station_data['AQI']) if station_data['AQI'] != "—" else 0,
                    'quality': station_data['quality'],
                    'description': station_data['description'],
                    'measure': station_data['measure'],
                    'timestr': station_data['timePointStr'],
                    'co': station_data['CO'],
                    'no2': station_data['NO2'],
                    'o3': station_data['O3'],
                    'pm10': station_data['PM10'],
                    'pm25': station_data['PM2.5'],
                    'so2': station_data['SO2'],
                    'raw_data': station_data
                }
                self.batch_buffer.append(record_data)

        except Exception as e:
            print(f"处理数据时出错: {e}, 数据: {station_data}")

    def _batch_insert(self):
        """执行批量插入"""
        if not self.batch_buffer:
            return

        try:
            # 获取或创建站点
            station_names = {d['station_name'] for d in self.batch_buffer}
            stations = {
                s.name: s for s in
                AQIStation.objects.filter(name__in=station_names)
            }

            # 准备批量创建记录
            records_to_create = []
            for data in self.batch_buffer:
                station = stations.get(data['station_name'])
                if not station:
                    station = AQIStation.objects.create(
                        name=data['station_name'],
                        location=f"POINT({data['longitude']} {data['latitude']})"
                    )
                    stations[data['station_name']] = station

                records_to_create.append(AQIRecord(
                    station=station,
                    timestamp=data['timestamp'],
                    aqi=data['aqi'],
                    quality=data['quality'],
                    description=data['description'],
                    measure=data['measure'],
                    timestr=data['timestr'],
                    co=data['co'],
                    no2=data['no2'],
                    o3=data['o3'],
                    pm10=data['pm10'],
                    pm25=data['pm25'],
                    so2=data['so2'],
                    raw_data=data['raw_data']
                ))

            # 批量插入
            AQIRecord.objects.bulk_create(records_to_create)
            print(f"批量插入{len(records_to_create)}条记录")

            # 清空缓冲区
            self.batch_buffer = []

        except Exception as e:
            print(f"批量插入失败: {e}")
            self.batch_buffer = []  # 清空缓冲区，避免重复插入

    def _clean_old_data(self):
        """清理超过30天的旧数据"""
        try:
            thirty_days_ago = timezone.now() - timedelta(days=365)
            deleted_count, _ = AQIRecord.objects.filter(
                timestamp__lt=thirty_days_ago
            ).delete()
            print(f"已删除{deleted_count}条超过一年的旧数据")
        except Exception as e:
            print(f"清理旧数据失败: {e}")



if __name__ == '__main__':
    fetch = UpDateAQI()
    fetch.update_database()



