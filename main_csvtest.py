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

def getLocalTime():
    currentTime = datetime.now(pytz.utc)+timedelta(hours=7)
    return currentTime

def createTeamCSVFile(jsonContent, updateDate):
    updateDate=updateDate+timedelta(hours=-1)
    fileName = "Team_"+updateDate.strftime("%Y%m%d_%H")
    updateDateStr=updateDate.strftime("%Y-%m-%d %H:00")
    data = json.loads(jsonContent)['body']['results']

    count = 0;
    data_file = open('data/teams/'+fileName + '.csv', 'w', encoding="utf-8-sig",newline='')
    total_file = open('data/teams/ummary.csv', 'a', encoding="utf-8-sig",newline='')
    csv_writer = csv.writer(data_file)
    total_writer = csv.writer(total_file)

    for team in data:
        team['updatedate'] = updateDateStr
        if count == 0:
            header = "id, ten, soVanDongVien, quangDuong, thoiGian, updatedate"

            csv_writer.writerow(header)

        csv_writer.writerow([team['runDoiId'] ,team['runDoi']['ten'], team['soVanDongVien'], team['quangDuong'], team['thoiGian'], updateDateStr])
        total_writer.writerow([team['runDoiId'] ,team['runDoi']['ten'], team['soVanDongVien'], team['quangDuong'], team['thoiGian'], updateDateStr])
        count += 1
        if count > 19:
            break;

def createRunnerCSVFile(jsonContent, updateDate,teamID, page):


    fileName = "Runner_"+teamID+"_"+updateDate.strftime("%Y%m%d_%H")
    updateDateStr=updateDate.strftime("%Y-%m-%d %H:00")
    data = json.loads(jsonContent)['body']['results']

    count = 0;

    if (page==1):
        data_file = open('data/runners/'+fileName + '.csv', 'w', encoding="utf-8-sig",newline='')
    else:
        data_file = open('data/runners/' + fileName + '.csv', 'a', encoding="utf-8-sig", newline='')

    total_file = open('data/RunnerSummary.csv', 'a', encoding="utf-8-sig",newline='')
    csv_writer = csv.writer(data_file)
    total_writer = csv.writer(total_file)

    #print(data)

    for team in data:
        team['updatedate'] = updateDateStr
        if count == 0 and page==1:
            #header = "id, gioiTinh,ebib, tinhThanh, xepHangChung, xepHangGioiTinh, xepHangDoi, soHoatDong, thoiGian, quangDuong, mucDongGop, dongGop, dongGopQuangDuong, dongGopSuKien, dongGopKhuyenMai, runVandongvienId, runDoiID, runVdvNickname,runDoiTen, updatedate"
            #csv_writer.writerow([header])
            csv_writer.writerow(team.keys())
        #csv_writer.writerow([team['id'] ,team['gioiTinh'],team['ebib'], team['tinhThanh'], team['xepHangChung'], team['xepHangGioiTinh'], team['xepHangDoi'], team['soHoatDong'], team['thoiGian'], team['quangDuong'], team['mucDongGop'], team['dongGop'], team['dongGopQuangDuong'], team['dongGopSuKien'], team['dongGopKhuyenMai'], team['runVandongvienId'], team['runDoiId'], team['runVdvNickname'],team['runDoiTen'], updateDateStr])
        csv_writer.writerow(team.values())
        #    total_writer.writerow([team['id'] ,team['gioiTinh'],team['ebib'], team['tinhThanh'], team['xepHangChung'], team['xepHangGioiTinh'], team['xepHangDoi'], team['soHoatDong'], team['thoiGian'], team['quangDuong'], team['mucDongGop'], team['dongGop'], team['dongGopQuangDuong'], team['dongGopSuKien'], team['dongGopKhuyenMai'], team['runVandongvienId'], team['runDoiId'], team['runVdvNickname'],team['runDoiTen'], updateDateStr])
        count+=1
    return count

def IsInterestedHour(currenttTime):
    if (currenttTime.hour in (1,9,17)):
        return True
    else:
        return False

def DownloadRunnerData(teamID ):
    #787=3T
    updateDate = getLocalTime() + timedelta(hours=-1)
    updateDateStr = updateDate.strftime("%Y-%m-%d %H:00")
    page=1
    
    
    requestId=uuid.uuid4().hex

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

    body='{"content":{"bidvrunBody":{},"bidvrunHeaders":{"Content-Type":"application/json","Authorization":""},"method":"GET","uri":"/api/run/run_vandongvien_giaichay_view?orderBy=xepHangDoi&runGiaichayId=201&runDoiId='+teamID+'&queryOffset='+str(page)+'&queryLimit=50"}}'
    checksum = sign_string(body)
    header='{"appToken":"eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJCSURWQVBJIiwiYXBwaWQiOjQzNSwiY2xpZW50aWQiOjIsInBsYW5pZCI6NDM0fQ.1JW3nQq6c3osVIxIY3vMVJcPTiw2JW7pvxV6ITI2wkHL0iaZ0vJFH7llRJThzwMeOSoCx7_K9KJjD0iytyUHLg","custToken":"","checksum":"'+checksum+'","requestID":"'+requestId+'","aurthUrl":"11"}'

    data = '{"body":'+body+',"header":'+header+'}'
       # print(data)
    response = requests.post('https://run.bidv.com.vn/bidvrunapiapp/global/vn/bidvrun/forward_unauth/v1', headers=headers,
                                 data=data, verify=False)
    print(response.content)
    createRunnerCSVFile(response.content, updateDate,teamID,1)
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
        body='{"content":{"bidvrunBody":{},"bidvrunHeaders":{"Content-Type":"application/json","Authorization":""},"method":"GET","uri":"/api/run/run_vandongvien_giaichay_view?orderBy=xepHangDoi&runGiaichayId=201&runDoiId='+teamID+'&queryOffset='+str(offcess)+'&queryLimit=50"}}'
        checksum = sign_string(body)
        header='{"appToken":"eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJCSURWQVBJIiwiYXBwaWQiOjQzNSwiY2xpZW50aWQiOjIsInBsYW5pZCI6NDM0fQ.1JW3nQq6c3osVIxIY3vMVJcPTiw2JW7pvxV6ITI2wkHL0iaZ0vJFH7llRJThzwMeOSoCx7_K9KJjD0iytyUHLg","custToken":"","checksum":"'+checksum+'","requestID":"'+requestId+'","aurthUrl":"11"}'

        data = '{"body":'+body+',"header":'+header+'}'
        #print(data)
        response = requests.post('https://run.bidv.com.vn/bidvrunapiapp/global/vn/bidvrun/forward_unauth/v1', headers=headers,
                                    data=data, verify=False)      
        print(response.content)
        createRunnerCSVFile(response.content, updateDate,teamID,i)
    




def sign_string( to_sign):
    key_b64 = "vO+LEGMQhBJt4vF/3cII6MqhWi/JLI03vKwp/Tu7FOdcHUgs3nF8O/ItxVrBPknk9wgbG7R1AP0HELi70K1eTg=="
    
    key = base64.b64decode(key_b64)
    print(key)
    signed_hmac_sha256 = hmac.HMAC(key, to_sign.encode(), hashlib.sha256)
    digest = signed_hmac_sha256.digest()
    return base64.b64encode(digest).decode()


# def DownloadTeamData():
#     requestId=uuid.uuid4().hex
#     currentTime = getLocalTime()
#     urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#     headers = {
#         'Connection': 'keep-alive',
#         'Accept': 'application/json, text/plain, */*',
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
#         'Content-Type': 'application/json',
#         'Origin': 'https://run.bidv.com.vn',
#         'Sec-Fetch-Site': 'same-origin',
#         'Sec-Fetch-Mode': 'cors',
#         'Sec-Fetch-Dest': 'empty',
#         'Referer': 'https://run.bidv.com.vn/home/clubs/201/175',
#         'Accept-Language': 'vi,en;q=0.9,en-US;q=0.8',
#     }
#     body='{"content":{"bidvrunBody":{},"bidvrunHeaders":{"Content-Type":"application/json","Authorization":""},"method":"GET","uri":"/api/run/run_vandongvien_giaichay_view?orderBy=xepHangDoi&runGiaichayId=201&runDoiId=175&clientType=1&queryOffset=1&queryLimit=200"}}'
#     checksum=sign_string(body)
#     header='{"appToken":"eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJCSURWQVBJIiwiYXBwaWQiOjQzNSwiY2xpZW50aWQiOjIsInBsYW5pZCI6NDM0fQ.1JW3nQq6c3osVIxIY3vMVJcPTiw2JW7pvxV6ITI2wkHL0iaZ0vJFH7llRJThzwMeOSoCx7_K9KJjD0iytyUHLg","custToken":"","checksum":"'+checksum+'","requestID":"'+requestId+'","aurthUrl":"11"}'
#     data = '{"body":'+body+', "header":'+header+'}'
#     print(data)
#     response = requests.post('https://run.bidv.com.vn/bidvrunapiapp/global/vn/bidvrun/forward_unauth/v1', headers=headers, data=data, verify=False)
#     print (response.content)
#     #createTeamCSVFile(response.content, currentTime)

# DownloadTeamData()

# FocusTeams=[175]
# for i in FocusTeams:
#    DownloadRunnerData(str(i))
DownloadRunnerData('13')