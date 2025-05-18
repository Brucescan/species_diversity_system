"""
@description 获取鸟类数据
@auther brucescan
"""
import random
import requests
import execjs
import time
import ast
from ddddocr import DdddOcr
# TODO 暂时不知道什么原因导致一些数据无法获取，等待修复

class FetchBird:
    def __init__(self):
        self.headers = None
        self.session = requests.session()
        self.url = "https://api.birdreport.cn/front/activity/search"
        with open("guanniao.js", 'r', encoding='utf-8') as f:
            js_code = f.read()
        self.js = execjs.compile(js_code)

    def get_page_count(self):
        """
        得到总页数，用来循环获取所有数据
        :return: 总页数
        """
        jiami_data = {
            "data": "page=1&limit=50",
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
        if data_resp.json()['code']==505:
            print("开始验证")
            while True:
                if self.process_verify():
                    print("成功验证")
                    break
                print("继续验证")
            data_resp = self.session.post(self.url, headers=self.headers, data=resp["urlParam"])
            print("收到响应",data_resp.json())
        return int(int(data_resp.json()["count"])/50)+1

    def get_all_data(self,queue):
        """
        获取数据的主函数
        :return: 总数据
        """
        # page_count = self.get_page_count()
        # 因为数据量大，所以先写死
        page_count = 3
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
            print(data_resp.text,"这是第100行的响应")
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
            one_page_data = []
            one_page_list = ast.literal_eval(data_res)
            for one_report in one_page_list:
                if one_report["address"][:3] == "北京市":
                    # 判断是否是北京市
                    get_details = self.get_get_details("reportId=" + one_report["reportId"])
                    one_report["get_details"] = get_details
                    species_details = self.get_species_details(f"page=1&limit=1500&reportId={one_report['reportId']}")
                    one_report["species_details"] = species_details
                    one_page_data.append(one_report)
            print(f"page{i+1}抓取完毕")
            self.process_bird_data(one_page_data, queue)
        queue.put("鸟类数据抓取完毕")

    def get_get_details(self, reportId_data):
        """
        获取一个观测的详细信息
        :param reportId_data:
        :return:一个报告中详细的信息，包括地点等
        """
        details = {}
        params_resp = self.js.call("encryptHeaders", {"data": reportId_data})
        url = "https://api.birdreport.cn/front/activity/get"
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
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
        print(data_get_resp.text,"这是第153行的响应")
        try:
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
        except:
            return details
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
        print(data_get_resp.text,"我是第194行的错误")
        try:
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
        except:
            return details
        # print(data_get)
        details["details"] = data_get
        return details

    def process_verify(self):
        time.sleep(random.randint(1, 2))
        url = "https://api.birdreport.cn/front/code/visited/generate"
        headers = {
            "accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "connection": "keep-alive",
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
        resp = self.session.get(url, headers=headers, params={
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
        res = self.session.post(verify_url, headers=verify_headers, json={
            "code": str(verify_code),
        })
        print("返回的sucess==========",res.json()['success'])
        if res.json()["success"]:
            return True
        return False

    def process_bird_data(self,bird_data,queue):
        print("开始处理数据==============================")
        for bird in bird_data:
            one_report = {}
            one_report["address"] = bird["address"]
            one_report["startTime"] = bird["startTime"]
            one_report["endTime"] = bird["endTime"]
            one_report["taxonCount"] = bird["taxonCount"]
            one_report["serialId"] = bird["serialId"]
            print(bird["get_details"],type(bird["get_details"]))
            if bird["get_details"] =={}:
                continue
            one_report["longitude"] = eval(bird["get_details"]["details"])["location"].split(",")[0]
            if bird["get_details"] =={}:
                continue
            one_report["latitude"] = eval(bird["get_details"]["details"])["location"].split(",")[1]
            print(bird["species_details"],type(bird["species_details"]))
            if bird["species_details"] =={}:
                continue
            one_report["species"] = eval(bird["species_details"]["details"])
            # processed_data.append(one_report)
            queue.put({"type":"bird","data":one_report})
        return None
