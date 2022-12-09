import yaml
import os
import requests
import json
import time
import asyncio
import csv

from openpyxl import load_workbook

def _read_yaml(base_dir, file_name):
	file_dir = base_dir + '/' + file_name
	with open(file_dir) as f:
		ret = yaml.load(f, Loader=yaml.FullLoader)
		f.close()

	return ret

def _read_xlxs(base_dir, file_name):
	file_dir = base_dir + '/' + file_name
	load_wb = load_workbook(file_dir, read_only=True)
	ret = load_wb['Sheet1']

	return ret

def _write_yaml(base_dir, file_name, data):
	file_dir = base_dir + '/' + file_name
	with open(file_dir, 'w') as f:
		yaml.dump(data=data, stream=f)
		f.close()

def _write_csv(base_dir, file_name, data):
	file_dir = base_dir + '/' + file_name
	with open(file_dir, 'a') as f:
		wr = csv.writer(f)
		wr.writerow(data)
		f.close()

def _new_app_token(key, url):
	app_key_url = url['real_app_domain'] + url['new_app_token']
	if key['apptoken']:
		task = asyncio.run(domestic_stock_price(key, url, "000660"))
		if task.status_code == 200:
			return key['apptoken']

	data = {
		"grant_type": key['grant_type'],
		"appkey": key['appkey'],
		"appsecret": key['appsecret']
	}
	ret = requests.post(url=app_key_url, data=json.dumps(data)).json()

	return ret['access_token']

async def domestic_stock_price(key, url, stock_num):
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

async def foreign_stock_price(key, url, excd, stock_num):
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

def _init(base_dir):
	conf_dir = 'conf'
	mac_slice = '/'
	key_file = 'key.yaml'
	url_file = 'url.yaml'

	key = _read_yaml(base_dir + mac_slice + conf_dir, key_file)
	url = _read_yaml(base_dir + mac_slice + conf_dir, url_file)

	key['apptoken'] = _new_app_token(key, url)

	_write_yaml(base_dir + mac_slice + conf_dir, key_file, key)

	return (key, url)

def main():
	base_dir = os.path.dirname(__file__)
	(key, url) = _init(base_dir)

	# 코스피 기준
	ws1 = _read_xlxs(base_dir + "/xlsx_file", "kospi_code.xlsx")

	i = 1

	start = time.time()
	while i < 2000:
		i = i + 1
		cell_num = "A" + str(i)
		cell_val = ws1[cell_num].value
		stock_price = asyncio.run(domestic_stock_price(key, url, cell_val)).json()
		if i % 100 == 0:
			end = time.time()
			print(end - start)

	# 나스닥 기준
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
main()
