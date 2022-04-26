import csv
import json
import pathlib
import pytz
import requests
import os
import base64
import hashlib
import hmac

from datetime import date, datetime, timedelta
import urllib3
import uuid
import json 
import pandas as pd 
from pandas.io.json import json_normalize 
  


def sign_string( to_sign):
    key_b64 = "vO+LEGMQhBJt4vF/3cII6MqhWi/JLI03vKwp/Tu7FOdcHUgs3nF8O/ItxVrBPknk9wgbG7R1AP0HELi70K1eTg=="
    key = base64.b64decode(key_b64)
    print(key)
    signed_hmac_sha256 = hmac.HMAC(key, to_sign.encode(), hashlib.sha256)
    digest = signed_hmac_sha256.digest()
    return base64.b64encode(digest).decode()


def DownloadRunnerData(teamID ):
    requestId=uuid.uuid4().hex
    db = [] 
    headers = {
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
            'Content-Type': 'application/json',
            'Origin': 'https://run.bidv.com.vn',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': f'https://run.bidv.com.vn/home/clubs/201/{teamID}',
            'Accept-Language': 'vi,en;q=0.9,en-US;q=0.8',
        }

    body='{"content":{"bidvrunBody":{},"bidvrunHeaders":{"Content-Type":"application/json","Authorization":""},"method":"GET","uri":"/api/run/run_vandongvien_giaichay_view?orderBy=xepHangDoi&runGiaichayId=201&runDoiId='+str(teamID)+'&queryOffset=1&queryLimit=50"}}'
    checksum = sign_string(body)
    header='{"appToken":"eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJCSURWQVBJIiwiYXBwaWQiOjQzNSwiY2xpZW50aWQiOjIsInBsYW5pZCI6NDM0fQ.1JW3nQq6c3osVIxIY3vMVJcPTiw2JW7pvxV6ITI2wkHL0iaZ0vJFH7llRJThzwMeOSoCx7_K9KJjD0iytyUHLg","custToken":"","checksum":"'+checksum+'","requestID":"'+requestId+'","aurthUrl":"11"}'
    data = '{"body":'+body+',"header":'+header+'}'
       # print(data)
    response = requests.post('https://run.bidv.com.vn/bidvrunapiapp/global/vn/bidvrun/forward_unauth/v1', headers=headers,
                                 data=data, verify=False)
    #print(response.content)
    data = json.loads(response.content)['body']['results']        
    df =  pd.json_normalize(data)
    db.append(df)    
    rowcount = json.loads(response.content)['body']['rowCount']    
    items_per_page = 50   
    number_of_pages = (int(rowcount) + items_per_page - 1) // items_per_page
    print(number_of_pages)
    for i in range(2,number_of_pages+1):
        offcess = ((i-1) * items_per_page +1)
        print("-----------------------------------------------------------------")  
        headers = {
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
            'Content-Type': 'application/json',
            'Origin': 'https://run.bidv.com.vn',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': f'https://run.bidv.com.vn/home/clubs/201/{teamID}',
            'Accept-Language': 'vi,en;q=0.9,en-US;q=0.8',
        }
        requestId=uuid.uuid4().hex
        body='{"content":{"bidvrunBody":{},"bidvrunHeaders":{"Content-Type":"application/json","Authorization":""},"method":"GET","uri":"/api/run/run_vandongvien_giaichay_view?orderBy=xepHangDoi&runGiaichayId=201&runDoiId='+str(teamID)+'&queryOffset='+str(offcess)+'&queryLimit=50"}}'
        checksum = sign_string(body)
        header='{"appToken":"eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJCSURWQVBJIiwiYXBwaWQiOjQzNSwiY2xpZW50aWQiOjIsInBsYW5pZCI6NDM0fQ.1JW3nQq6c3osVIxIY3vMVJcPTiw2JW7pvxV6ITI2wkHL0iaZ0vJFH7llRJThzwMeOSoCx7_K9KJjD0iytyUHLg","custToken":"","checksum":"'+checksum+'","requestID":"'+requestId+'","aurthUrl":"11"}'

        data = '{"body":'+body+',"header":'+header+'}'
        #print(data)
        response = requests.post('https://run.bidv.com.vn/bidvrunapiapp/global/vn/bidvrun/forward_unauth/v1', headers=headers,
                                    data=data, verify=False)      
        #print(response.content)
        data = json.loads(response.content)['body']['results'] 
         
        db.append(pd.json_normalize(data))
    return pd.concat(db)

dp = DownloadRunnerData(175)
dp.reset_index()
cbphong = pd.read_excel('cb_phongban.xlsx')
dp ['phong'] = dp ['runVandongvienId'].map(cbphong.set_index('runVandongvienId')['phong'])
dp ['hoten'] = dp ['runVandongvienId'].map(cbphong.set_index('runVandongvienId')['runVdvHoten'])
dp ['ngaydl'] = date.today()
#output[['soHoatDong','thoiGian','quangDuong','mucDongGop','dongGop','dongGopQuangDuong','dongGopSuKien','dongGopKhuyenMai']] = output[['soHoatDong','thoiGian','quangDuong','mucDongGop','dongGop','dongGopQuangDuong','dongGopSuKien','dongGopKhuyenMai']].astype(int)


headers=['ngaydl','hoten','phong','gioiTinh','ebib','soHoatDong','thoiGian','quangDuong','mucDongGop','dongGop','dongGopQuangDuong','dongGopSuKien','xepHangDoi','xepHangDoiGioiTinh','dongGopKhuyenMai','runVandongvienId','runVdvHoten','runVdvNickname','nickname','runVdvGioitinh','runVdvAnhTen','runDoiTen']

checl = dp[headers] 
ngayhientai = date.today().strftime("%d-%m-%Y")

checl.to_excel(f"data\cb_{ngayhientai}.xlsx",index=False)