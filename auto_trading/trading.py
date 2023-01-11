import multiprocessing as mp
import os

import func as fun
import kis_api as kis
import requests
import time
import datetime
import asyncio

from sqlalchemy import (
	create_engine,

)
from sqlalchemy.orm import (
	Session
)

engine = create_engine('sqlite://')

class CollectPrice:
	'''
	분봉을 수집해 데이터베이스에 넣는 자식 프로세스 클래스
	'''
	def __init__(self, key, secret, token, url):
		self.key = key
		self.secret = secret
		self.token = token
		self.url = url

	async def collect_stock(self):
		ret = []
		start_time = time.time()
		while True:
			ret.append((await kis._domestic_stock_min_price((self.key, self.secret, self.token), self.url, self.slack_url, '000060', '000900', False)).json())
			if ret[-1]['rt_cd'] != '0':
				await fun._writerow_csv(os.path.dirname(__file__), self.file_name, ('Not Correct!',))
			if len(ret) % 20 == 0:
				await asyncio.sleep(0.15)
			if len(ret) % 100 == 0:
				end_time = time.time()
				total = end_time - start_time
				string = (str(total), datetime.datetime.today().strftime('%Y%m%d_%h%M%S'))
				await fun._writerow_csv(os.path.dirname(__file__), self.file_name, string)
				start_time = end_time


	def run(self, slack_url, file_name):
		#test
		self.slack_url = slack_url
		self.file_name = file_name #test용 파일

		fun._send_slack(self.slack_url, 'Start SubProcess')
		asyncio.run(self.collect_stock())


class RealTimeStockPrice:
	'''
	웹소켓을 이용해 실시간 정보를 받는 프로세스.
	'''
	def __init__(self, key, secret, url):
		self.key = key
		self.secret = secret
		self.url = url

	def run(self):
		pass

class Trading:
	'''
	정보를 이용해 실제로 트레이딩을 하는 클래스.
	'''
	def __init__(self):
		pass

def _init():
	dir = os.path.dirname(__file__)
	conf_dir = dir + fun._dir_seperator_check() + 'conf'
	global engine

	first = '7390'
	second = '6363'
	mock = '5007'

	key = fun._read_yaml(conf_dir, 'key.yaml')
	url = fun._read_yaml(conf_dir, 'url.yaml')

	real_url = url['real_api_domain']
	mock_url = url['fake_api_domain']

	#appkey 발급 후 yaml 작성
	app_token = (
		kis._new_app_token(real_url, key[f'{first}_key'], key[f'{first}_secret']),
		kis._new_app_token(real_url, key[f'{second}_key'], key[f'{second}_secret']),
		kis._new_app_token(mock_url, key[f'{mock}_key'], key[f'{mock}_secret'])
	)

	key[f'{first}_token'] = app_token[0]
	key[f'{second}_token'] = app_token[1]
	key[f'{mock}_token'] = app_token[2]

	fun._write_yaml(conf_dir, 'key.yaml', key)

	first_real_w = CollectPrice(key[f'{first}_key'], key[f'{first}_secret'], app_token[0], real_url)

	second_real_w = CollectPrice(key[f'{second}_key'], key[f'{second}_secret'],app_token[1], real_url)

	mock_w = CollectPrice(key[f'{mock}_key'], key[f'{mock}_secret'], app_token[2], mock_url)

	return engine, key, url, first_real_w, second_real_w, mock_w

if __name__=='__main__':
	(engine, key, url, first_w, second_w, mock_w)= _init()

	first_p = mp.Process(name='First', target=first_w.run, args=(url['slack_webhook_url'], 'first'))
	first_p.start()

	second_p = mp.Process(name='Second', target=second_w.run, args=(url['slack_webhook_url'], 'second'))
	second_p.start()

	mock_p = mp.Process(name='Mock', target=mock_w.run, args=(url['slack_webhook_url'], 'mock'))
	mock_p.start()

