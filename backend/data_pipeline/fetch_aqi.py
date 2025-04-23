"""
@description 获取实时空气质量
@auther brucescan
"""
import requests


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

    def get_data(self):
        params = {
            "cityName": "北京市",
        }
        resp = requests.get(self.url, headers=self.headers, params=params)
        stations_list = resp.json()
        history_station_data = []
        for station in stations_list:
            history_station_data.append(self.get_station_history(station["StationCode"]))
        return history_station_data

    def get_station_history(self, station_code):
        history_params = {
            "stationCode": station_code,
        }
        # print(history_params)
        history_resp = requests.post("https://air.cnemc.cn:18007/HourChangesPublish/GetAqiHistoryByCondition",
                                     headers=self.headers, params=history_params)
        return history_resp.text


if __name__ == '__main__':
    fetch = FetchAQI()
    data = fetch.get_data()
    print(data)
