import requests
import json
import func as fun
import time

real_api_domain = "https://openapi.koreainvestment.com:9443"
fake_api_domain = "https://openapivts.koreainvestment.com:29443"

def _new_app_token(account_info: dict) -> dict:
	global real_api_domain
	global fake_api_domain
	api_url = real_api_domain if account_info['check_real'] else fake_api_domain

	data = {
		"grant_type": 'client_credentials',
		"appkey": account_info['key'],
		"appsecret": account_info['secret']
	}
	url = api_url + '/oauth2/tokenP'

	ret = requests.post(url=url, data=json.dumps(data)).json()
	account_info['token'] = ret['access_token']
	return account_info

def _check_korea_holiday(account_info: dict, slack_url, check_day : str) -> requests.Response:
	'''
	account_info : dict / key, secret, token, check_real(boolean)
	api_url : real or fake
	slack_url : webhook url
	check_day : 'YYMMDD'
	'''
	global real_api_domain
	global fake_api_domain
	api_url = real_api_domain if account_info['check_real'] else fake_api_domain
	header = {
		'content-type' : 'application/json; charset=utf-8',
		'authorization' : 'Bearer ' + account_info['token'],
		'appkey' : account_info['key'],
		'appsecret': account_info['secret'],
		'tr_id' : 'CTCA0903R'
	}
	params = {
		'BASS_DT' : check_day,
		'CTX_AREA_NK' : '',
		'CTX_AREA_FK' : ''
	}
	url = api_url + '/uapi/domestic-stock/v1/quotations/chk-holiday'
	try:
		ret = requests.get(url=url, headers=header, params=params)
	except:
		fun._send_slack(slack_url, 'Error in check holiday!')
	return ret


async def _domestic_stock_today_price(account_info : list, slack_url : str, stock_num : str) -> requests.Response:
	'''
	account_info : key, secret, token, real or mock(boolean)
	api_url : real or fake
	slack_url : webhook url
	stock_num : korea stock num
	'''
	global real_api_domain
	global fake_api_domain
	api_url = real_api_domain if account_info['check_real'] else fake_api_domain
	header = {
		'authorization' : "Bearer " + account_info['token'],
		'appkey' : account_info['key'],
		'appsecret' : account_info['secret'],
		'tr_id' : 'FHKST01010100'
	}

	params = {
		'FID_COND_MRKT_DIV_CODE' : 'J',
		'FID_INPUT_ISCD' : stock_num
	}

	url = api_url + '/uapi/domestic-stock/v1/quotations/inquire-price'
	try:
		ret = requests.get(url=url, headers=header, params=params)
	except:
		fun._send_slack(slack_url, f'requests Error!\n Stock_num: {stock_num}')
		pass
	return ret

async def _domestic_stock_min_price(account_info : list, api_url : str, slack_url : str, stock_num : str, time : str, past : bool) -> requests.Response:
	'''
	account_info : key, secret, token, real or mock(boolean)
	api_url : real or fake
	slack_url : webhook url
	stock_num : korea stock num
	time : 'YYMMDD'
	past : If false, just stock price in time. Else, include last 30 min stock price
	'''
	global real_api_domain
	global fake_api_domain
	api_url = real_api_domain if account_info['check_real'] else fake_api_domain
	past = 'Y' if past else 'N'
	header = {
		'content-type' : 'application/json; charset=utf-8',
		'authorization' : "Bearer " + account_info['token'],
		'appkey' : account_info['key'],
		'appsecret' : account_info['secret'],
		'tr_id' : 'FHKST03010200',
		'custtype' : 'P',
	}
	params = {
		'FID_ETC_CLS_CODE' : "",
		'FID_COND_MRKT_DIV_CODE' : 'J',
		'FID_INPUT_ISCD' : stock_num,
		'FID_INPUT_HOUR_1' : time,
		'FID_PW_DATA_INCU_YN' : past
	}

	url = api_url + '/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice'
	try:
		ret = requests.get(url=url, headers=header, params=params)
	except:
		fun._send_slack(slack_url, f'requests Error!\n Stock_num: {stock_num}, Time: {time}')
		pass
	return ret
