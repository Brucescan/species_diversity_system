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


    def get_all_data(self):
        """
        获取数据的主函数
        :return: 总数据
        """
        page_count = self.get_page_count()
        print(page_count)
        # 因为数据量大，所以先写死
        # page_count = 10
        bird_data = []
        for i in range(page_count):
            print(i)
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
            print("运行到这里了")
            data_resp = self.session.post(self.url, headers=self.headers, data=resp["urlParam"])
            # print(data_resp.json())
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
            # print(data_res)

            one_page_list = ast.literal_eval(data_res)
            # print(one_page_list)
            for one_report in one_page_list:
                if one_report["address"][:3] == "北京市":
                    time.sleep(random.randint(1,3))
                    # 判断是否是北京市
                    print(one_report["address"])
                    get_details = self.get_get_details("reportId=" + one_report["reportId"])
                    one_report["get_details"] = get_details
                    species_details = self.get_species_details(f"page=1&limit=1500&reportId={one_report['reportId']}")
                    one_report["species_details"] = species_details
                    bird_data.append(one_report)
                    # print(one_report)
            # bird_data.append(data_res)
            time.sleep(random.randint(1, 5))
            # break
        return bird_data

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
        # print(resp.content)
        docr = DdddOcr(show_ad=False)
        try:
            verify_code = docr.classification(resp.content)
        except:
            return False
        print(verify_code)

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
        print(type(res.json()["success"]))
        print(res.json()['success']==True)
        if res.json()["success"]:
            return True
        return False

        # print(res.text)


if __name__ == '__main__':
    fetch = FetchBird()
    all_data = {
        "type": "bird",
        "data": fetch.get_all_data()
    }
    print(all_data)
    # print(all_data)
    # fetch.process_verify()
