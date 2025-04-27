"""
用来更新数据库中的数据
"""
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class FetchAQI:
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

    def get_data(self,queue):
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
        return self.process_data(history_station_data,queue)

    def get_station_history(self, station_code):
        history_params = {
            "stationCode": station_code,
        }
        # print(history_params)
        history_resp = requests.post("https://air.cnemc.cn:18007/HourChangesPublish/GetAqiHistoryByCondition",
                                     headers=self.headers, params=history_params)
        return history_resp.text

    def process_data(self, all_data,queue):
        # processed_data = []
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
                queue.put({
                    "type":"AQI",
                    "data":one_data
                })
        queue.put("空气质量数据抓取完毕")