
from datetime import date, datetime, timedelta

import json 
import pandas as pd 
from pandas.io.json import json_normalize 
import bidvrunlib as lib

machinhanh = 175

dp = lib.DownloadRunnerData(machinhanh)
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