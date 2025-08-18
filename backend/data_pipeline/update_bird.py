import json
import os
import random
import django
import requests
import execjs
import time
import ast
from ddddocr import DdddOcr
from datetime import datetime, date
from django.db import transaction
from django.contrib.gis.geos import Point


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()
from data_pipeline.models import BirdObservation, BirdSpeciesRecord

class UpDateBird:
    def __init__(self):
        self.headers = None
        self.session = requests.session()
        self.url = "https://api.birdreport.cn/front/activity/search"
        with open("guanniao.js", 'r', encoding='utf-8') as f:
            js_code = f.read()
        self.js = execjs.compile(js_code)

    def get_all_data(self):
        page_count = 1
        bird_data = []
        for i in range(page_count):
            jiami_data = {
                "data": f"page={i + 1}&limit=50&sortBy=startTime&orderBy=desc",
            }
            resp = self.js.call("encryptHeaders", jiami_data)
            self.headers = {
                "accept": "application/json, text/javascript, */*; q=0.01",
                # "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "en-US,en;q=0.9",
                "connection": "keep-alive",
                "content-length": "172",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "host": "api.birdreport.cn",
                "origin": "https://www.birdreport.cn",
                "referer": "https://www.birdreport.cn/",
                "requestid": resp["requestId"],
                "sec-ch-ua": "Chromium;v=129, Not=A?Brand;v=8",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "Windows",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "sign": resp["sign"],
                "timestamp": str(resp["timestamp"]),
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
            }
            data_resp = self.session.post(self.url, headers=self.headers, data=resp["urlParam"])
            if data_resp.json()["code"]==505:
                print("开始验证")
                while True:
                    if self.process_verify():
                        print("验证成功")
                        break
                    print("继续验证")
                data_resp = self.session.post(self.url, headers=self.headers, data=resp["urlParam"])
                print("收到响应",resp)
            data_res = self.js.call("decryptFn", data_resp.json()['data'])

            one_page_list = ast.literal_eval(data_res)
            for one_report in one_page_list:
                is_beijing, is_today = self.is_beijing_today_data(one_report)
                if is_beijing and is_today:
                    # 判断是否是北京市
                    get_details = self.get_get_details("reportId=" + one_report["reportId"])
                    one_report["get_details"] = get_details
                    species_details = self.get_species_details(f"page=1&limit=1500&reportId={one_report['reportId']}")
                    one_report["species_details"] = species_details
                    bird_data.append(one_report)
            print(f"page{i+1}抓取完毕")
        return self.process_bird_data(bird_data)

    def get_get_details(self, reportId_data):
        details = {}
        params_resp = self.js.call("encryptHeaders", {"data": reportId_data})
        url = "https://api.birdreport.cn/front/activity/get"
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            # "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "connection": "keep-alive",
            "content-length": "172",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "host": "api.birdreport.cn",
            "origin": "https://www.birdreport.cn",
            "referer": "https://www.birdreport.cn/",
            "requestid": params_resp["requestId"],
            "sec-ch-ua": "Chromium;v=129, Not=A?Brand;v=8",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "sign": params_resp["sign"],
            "timestamp": str(params_resp["timestamp"]),
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        data_get_resp = self.session.post(url, headers=headers, data=params_resp["urlParam"])
        if data_get_resp.json()['code']==505:
            print("开始验证")
            while True:
                if self.process_verify():
                    print("成功验证")
                    break
                print("继续验证")
            data_get_resp = self.session.post(url, headers=headers, data=params_resp["urlParam"])
            print("收到响应",data_get_resp.json())
        data_get = self.js.call("decryptFn", data_get_resp.json()['data'])
        # print(data_get)
        details["details"] = data_get
        return details

    def get_species_details(self, params):
        details = {}
        params_resp = self.js.call("encryptHeaders", {"data": params})
        url = "https://api.birdreport.cn/front/activity/taxon"
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            # "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "connection": "keep-alive",
            "content-length": "172",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "host": "api.birdreport.cn",
            "origin": "https://www.birdreport.cn",
            "referer": "https://www.birdreport.cn/",
            "requestid": params_resp["requestId"],
            "sec-ch-ua": "Chromium;v=129, Not=A?Brand;v=8",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "sign": params_resp["sign"],
            "timestamp": str(params_resp["timestamp"]),
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        data_get_resp = self.session.post(url, headers=headers, data=params_resp["urlParam"])
        if data_get_resp.json()['code']==505:
            print("开始验证")
            while True:
                if self.process_verify():
                    print("验证成功")
                    break
                print("继续验证")
            data_get_resp = self.session.post(url, headers=headers, data=params_resp["urlParam"])
            print("收到响应",data_get_resp.json())
        data_get = self.js.call("decryptFn", data_get_resp.json()['data'])
        # print(data_get)
        details["details"] = data_get
        return details

    def process_verify(self):
        time.sleep(random.randint(1, 3))
        url = "https://api.birdreport.cn/front/code/visited/generate"
        headers = {
            "accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "connection": "keep-alive",
            "cookie": "Hm_lvt_1546b4feab0a3de87d0ccdcc5900128e=1744721366,1744804395,1744976023,1745141046; HMACCOUNT=DEF8FA2CCE9EB727; Hm_lpvt_1546b4feab0a3de87d0ccdcc5900128e=1745141170; JSESSIONID=8C4E1901C5D542F4961AA53E2E227834",
            "host": "api.birdreport.cn",
            "pragma": "no-cache",
            "referer": "https://www.birdreport.cn/",
            "sec-ch-ua": "\"Chromium\";v=\"129\", \"Not=A?Brand\";v=\"8\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "image",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        session = requests.Session()
        resp = session.get(url, headers=headers, params={
            "timestamp": str(time.time_ns()),
        })
        docr = DdddOcr(show_ad=False)
        try:
            verify_code = docr.classification(resp.content)
        except:
            return False

        verify_url = "https://api.birdreport.cn/front/code/visited/verify"
        verify_headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            # "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "connection": "keep-alive",
            "content-length": "15",
            "content-type": "application/json",
            "host": "api.birdreport.cn",
            "origin": "https://www.birdreport.cn",
            "pragma": "no-cache",
            "referer": "https://www.birdreport.cn/",
            "sec-ch-ua": "\"Chromium\";v=\"129\", \"Not=A?Brand\";v=\"8\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        res = session.post(verify_url, headers=verify_headers, json={
            "code": str(verify_code),
        })
        print("返回的sucess==========",res.json()['success'])
        if res.json()["success"]:
            return True
        return False

    def process_bird_data(self,bird_data):
        processed_data = []
        for bird in bird_data:
            one_report = {}
            print(bird["get_details"]["details"])
            one_report["address"] = bird["address"]
            one_report["startTime"] = bird["startTime"]
            one_report["endTime"] = bird["endTime"]
            one_report["taxonCount"] = bird["taxonCount"]
            one_report["serialId"] = bird["serialId"]
            one_report["longitude"] = eval(bird["get_details"]["details"])["location"].split(",")[0]
            one_report["latitude"] = eval(bird["get_details"]["details"])["location"].split(",")[1]
            one_report["species"] = eval(bird["species_details"]["details"])
            processed_data.append(one_report)
        return processed_data

    def update_database(self):
        data_list:list = self.get_all_data()
        try:
            for data in data_list:
                with transaction.atomic():
                    # 解析时间
                    start_time = datetime.strptime(data['startTime'], "%Y-%m-%d %H:%M")
                    end_time = datetime.strptime(data['endTime'], "%Y-%m-%d %H:%M")

                    # 创建观测记录
                    observation = BirdObservation.objects.create(
                        address=data['address'],
                        start_time=start_time,
                        end_time=end_time,
                        taxon_count=data.get('taxonCount', 0),
                        serial_id=data.get('serialId', ''),
                        location=Point(float(data['longitude']), float(data['latitude'])),
                        raw_data=json.dumps(data)
                    )

                    # 批量创建物种记录
                    species_to_create = []
                    for species_data in data.get('species', []):
                        species_to_create.append(BirdSpeciesRecord(
                            observation=observation,
                            taxon_id=species_data.get('taxon_id', 0),
                            taxon_name=species_data.get('taxon_name', ''),
                            latin_name=species_data.get('latinname', ''),
                            taxon_order=species_data.get('taxonordername', ''),
                            taxon_family=species_data.get('taxonfamilyname', ''),
                            count=species_data.get('taxon_count', 1),
                            has_images=bool(species_data.get('record_image_num', 0)),
                            outside_type=species_data.get('outside_type', 0),
                            activity_id=species_data.get('activity_id', None)
                        ))

                    BirdSpeciesRecord.objects.bulk_create(species_to_create)

                print(f"成功保存观测记录: {observation.id}")
                return None
            return None

        except Exception as e:
            print(f"保存数据失败: {e}")
            return None

    def is_beijing_today_data(self,report:dict):
        is_beijing = report.get("address", "").startswith("北京市")

        # 检查今天数据
        is_today = False
        try:
            start_time_str = report.get("startTime", "")
            if start_time_str:
                start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
                is_today = start_time.date() == date.today()
        except ValueError as e:
            print(f"时间解析错误: {e}, 数据: {report}")

        return is_beijing, is_today

if __name__ == '__main__':
    fetch = UpDateBird()
    fetch.update_database()