import pandas as pd 
from datetime import datetime, timedelta, date

output = pd.read_pickle("data.pkl")

cbphong = pd.read_excel('cb_phongban.xlsx')
output['phong'] = output['runVandongvienId'].map(cbphong.set_index('runVandongvienId')['phong'])
output['hoten'] = output['runVandongvienId'].map(cbphong.set_index('runVandongvienId')['runVdvHoten'])
output['ngaydl'] = date.today()
#output[['soHoatDong','thoiGian','quangDuong','mucDongGop','dongGop','dongGopQuangDuong','dongGopSuKien','dongGopKhuyenMai']] = output[['soHoatDong','thoiGian','quangDuong','mucDongGop','dongGop','dongGopQuangDuong','dongGopSuKien','dongGopKhuyenMai']].astype(int)


headers=['ngaydl','hoten','phong','gioiTinh','ebib','xepHangDoi','xepHangDoiGioiTinh','soHoatDong','thoiGian','quangDuong','mucDongGop','dongGop','dongGopQuangDuong','dongGopSuKien','dongGopKhuyenMai','runVandongvienId','runVdvHoten','runVdvNickname','nickname','runVdvGioitinh','runVdvAnhTen','runDoiTen']

checl = output[headers]
 
checl.to_excel("asdfasdf.xlsx")

