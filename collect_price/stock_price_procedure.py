import yaml
import os
import requests
import json
import asyncio
import platform
import csv
import tarfile

import time
import datetime
from openpyxl import load_workbook

today = datetime.datetime.today().strftime('%Y_%m_%d')

def _read_yaml(base_dir, file_name):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir) as f:
		ret = yaml.load(f, Loader=yaml.FullLoader)

	return ret

def _read_xlxs(base_dir, file_name):
	file_dir = base_dir + _dir_seperator_check() + file_name
	load_wb = load_workbook(file_dir, read_only=True)
	ret = load_wb['Sheet1']

	return ret

def _write_yaml(base_dir, file_name, data):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir, 'w') as f:
		yaml.dump(data=data, stream=f)

def _writerow_csv(base_dir, file_name, data):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir + '.csv', 'a') as f:
		wr = csv.writer(f)
		wr.writerow(data)

def _writerows_csv(base_dir, file_name, data):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir + '.csv', 'a') as f:
		wr = csv.writer(f)
		wr.writerows(data)

def _new_app_token(key, url):
	app_key_url = url['real_app_domain'] + url['new_app_token']
	# 원래는 지금 현재 발급되어 있는 앱 토큰이 작동이 되는 지 확인 후 작동되는 거지만,
	# 이럴 경우 예전 토큰을 사용하고 명령이 실행되는 도중 만료되면 에러가 발생하므로 해제시켜놓음.
	# if key['apptoken']:
	# 	task = asyncio.run(domestic_stock_price(key, url, "000660"))
	# 	if task.status_code == 200:
	# 		return key['apptoken']

	data = {
		"grant_type": key['grant_type'],
		"appkey": key['appkey'],
		"appsecret": key['appsecret']
	}
	ret = requests.post(url=app_key_url, data=json.dumps(data)).json()

	return ret['access_token']


def _dir_seperator_check():
	'''함수의 구분자를 os마다 판단해서 리턴하는 값'''
	if platform.system() == 'Windows':
		return '\\'
	else:
		return '/'

def _create_folder(base_dir, folder_name):
	folder = base_dir + _dir_seperator_check() + folder_name
	try:
		if not os.path.exists(folder):
			os.makedirs(folder)
	except:
		print("Error:create folder : " + folder)
	return folder

def _make_tar(folder_dir, tar_name):
	os.chdir(os.path.dirname(__file__))
	if os.path.exists(folder_dir):
		with tarfile.open(tar_name, 'w:gz') as tar:
			tar.add(folder_dir)


async def _domestic_stock_price(key, url, stock_num):
	header = {
		'authorization' : "Bearer " + key['apptoken'],
		'appkey' : key['appkey'],
		'appsecret' : key['appsecret'],
		'tr_id' : 'FHKST01010100'
	}

	params = {
		'FID_COND_MRKT_DIV_CODE' : 'J',
		'FID_INPUT_ISCD' : stock_num
	}

	url = url['real_app_domain'] + '/uapi/domestic-stock/v1/quotations/inquire-price'
	ret = requests.get(url=url, headers=header, params=params)
	return ret

async def _foreign_stock_price(key, url, excd, stock_num):
	header = {
		'authorization' : "Bearer " + key['apptoken'],
		'appkey' : key['appkey'],
		'appsecret' : key['appsecret'],
		'tr_id' : 'HHDFS00000300'
	}

	params = {
		'AUTH' : "",
		'EXCD' : excd,
		'SYMB' : stock_num
	}

	url = url['real_app_domain'] + '/uapi/overseas-price/v1/quotations/price'
	ret = requests.get(url=url, headers=header, params=params)
	return ret

async def _domestic_stock_min_price(key, url, stock_num, time):
	header = {
		'content-type' : 'application/json; charset=utf-8',
		'authorization' : "Bearer " + key['apptoken'],
		'appkey' : key['appkey'],
		'appsecret' : key['appsecret'],
		'tr_id' : 'FHKST03010200',
		'custtype' : 'P',
	}
	params = {
		'FID_ETC_CLS_CODE' : "",
		'FID_COND_MRKT_DIV_CODE' : 'J',
		'FID_INPUT_ISCD' : stock_num,
		'FID_INPUT_HOUR_1' : time,
		'FID_PW_DATA_INCU_YN' : 'Y' #과거 데이터 포함 여부(기본은 Y, 이럴 경우 time 시간 기준 과거 시간으로 30분까지 포함하여 리턴됨.)
	}

	url = url['real_app_domain'] + '/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice'
	# await asyncio.sleep(0.01)
	ret = requests.get(url=url, headers=header, params=params)
	return ret

def _init(base_dir):
	conf_dir = 'conf'
	dir_seperator = _dir_seperator_check()
	key_file = 'key.yaml'
	url_file = 'url.yaml'

	key = _read_yaml(base_dir + dir_seperator + conf_dir, key_file)
	url = _read_yaml(base_dir + dir_seperator + conf_dir, url_file)

	key['apptoken'] = _new_app_token(key, url)

	_write_yaml(base_dir + dir_seperator + conf_dir, key_file, key)

	return (key, url)

async def korea_min_stock_price(key, url, stock_num, csv_dir):
	list_stock_price = list()
	inquire_time = datetime.datetime(year=2022, month=12, day=12, hour=9, minute=29, second=0)
	# 수많은 뻘짓의 향연...
	# loop = asyncio.get_event_loop()
	# task = asyncio.create_task(_domestic_stock_min_price(key, url, stock_num, inquire_time.strftime("%H%M%S")))
	# loop.run_until_complete(task)
	# print(task.result())
	# stock_price = asyncio.run(_domestic_stock_min_price(key, url, stock_num, inquire_time.strftime("%H%M%S"))).json()
	stock_price = (await _domestic_stock_min_price(key, url, stock_num, inquire_time.strftime("%H%M%S"))).json()
	if stock_price['rt_cd'] != '0':
		print("정상응답이 아닙니다. 종료합니다.")
		return
	_writerow_csv(csv_dir, stock_num, list(stock_price['output1'].keys()) + list((stock_price['output2'])[0].keys()))
	for i in range(0, 19):
		stock_price = (await _domestic_stock_min_price(key, url, stock_num, inquire_time.strftime("%H%M%S"))).json()
		if stock_price['rt_cd'] != '0':
			print("정상응답이 아닙니다. 종료합니다.")
			return
		for output1_val in stock_price['output2']:
			list_stock_price.append(list(list(stock_price['output1'].values()) + list(output1_val.values())))
		inquire_time = inquire_time + datetime.timedelta(minutes=30)
	list_stock_price.sort(key=lambda x:x[9])
	_writerows_csv(csv_dir, stock_num, list_stock_price)

def kospi_stock_price_csv(base_dir, key, url, ws):
	kospi_price = "kospi"
	global today

	start = time.time()
	tmp = _create_folder(base_dir, today)
	kospi_dir = _create_folder(tmp, kospi_price)

	for j in range(2, ws.max_row + 1):
		cell_num = "A" + str(j)
		cell_val = ws[cell_num].value
		asyncio.run(korea_min_stock_price(key, url, cell_val, kospi_dir))
		print("kospi 분봉 수집 퍼센트 : " + str(j / ws.max_row * 100))
		end = time.time()
		print("kospi 분봉 수집시간 : " + str(end - start))

def kosdaq_stock_price_csv(base_dir, key, url, ws):
	kosdaq_price = "kosdaq"
	global today

	start = time.time()
	tmp = _create_folder(base_dir, today)
	kosdaq_dir = _create_folder(tmp, kosdaq_price)

	for j in range(2, ws.max_row + 1):
		cell_num = "A" + str(j)
		cell_val = ws[cell_num].value
		asyncio.run(korea_min_stock_price(key, url, cell_val, kosdaq_dir))
		print("kosdaq 분봉 수집 퍼센트 : " + str(j / ws.max_row * 100))
		end = time.time()
		print("kosdaq 분봉 수집시간 : " + str(end - start))

def nasdaq_stock_price(base_dir, key, url, dir_seperator):
	ws1 = _read_xlxs(base_dir + "/xlsx_file", "nas_code.xlsx")
	# ws1 = _read_xlxs(base_dir + "/xlsx_file", "nas_code.xlsx")

	# i = 1
	# start = time.time()
	# while i < 5000:
	# 	i = i + 1
	# 	cell_num = "E" + str(i)
	# 	print(cell_num)
	# 	cell_val = ws1[cell_num].value
	# 	stock_price = foreign_stock_price(key, url, "NAS", cell_val)

	# 	print((stock_price.json()["output"])["last"])
	# 	if i % 100 == 0:
	# 		end = time.time()
	# 		print(end - start)

def main():
	base_dir = os.path.dirname(__file__)
	dir_seperator = _dir_seperator_check()
	(key, url) = _init(base_dir)
	target_dir = base_dir + dir_seperator + today

	kospi_ws = _read_xlxs(target_dir + dir_seperator + 'kospi', 'kospi_code.xlsx')
	kospi_stock_price_csv(base_dir, key, url, kospi_ws)
	kosdaq_ws = _read_xlxs(target_dir + dir_seperator + 'kosdaq', 'kosdaq_code.xlsx')
	kosdaq_stock_price_csv(base_dir, key, url, kosdaq_ws)
	_make_tar(today, today + '.tar.gz')

main()
