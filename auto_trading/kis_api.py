import requests
import json
import func as fun
import time

def _new_app_token(api_url : str, key :str, secret : str):
	data = {
		"grant_type": 'client_credentials',
		"appkey": key,
		"appsecret": secret
	}
	url = api_url + '/oauth2/tokenP'

	ret = requests.post(url=url, data=json.dumps(data)).json()
	print(ret)
	return ret['access_token']

def _check_korea_holiday(account_info, api_url, slack_url, check_day : str) -> requests.Response:
	'''
	account_info : key, secret, token
	api_url : real or fake
	slack_url : webhook url
	check_day : 'YYMMDD'
	'''
	header = {
		'content-type' : 'application/json; charset=utf-8',
		'authorization' : 'Bearer ' + account_info[2],
		'appkey' : account_info[0],
		'appsecret': account_info[1],
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


async def _domestic_stock_today_price(account_info : list, api_url : str, slack_url : str, stock_num : str) -> requests.Response:
	'''
	account_info : key, secret, token
	api_url : real or fake
	slack_url : webhook url
	stock_num : korea stock num
	'''
	header = {
		'authorization' : "Bearer " + account_info[2],
		'appkey' : account_info[0],
		'appsecret' : account_info[1],
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
	account_info : key, secret, token
	api_url : real or fake
	slack_url : webhook url
	stock_num : korea stock num
	time : 'YYMMDD'
	past : If false, just stock price in time. Else, include last 30 min stock price
	'''
	past = 'Y' if past else 'N'
	header = {
		'content-type' : 'application/json; charset=utf-8',
		'authorization' : "Bearer " + account_info[2],
		'appkey' : account_info[0],
		'appsecret' : account_info[1],
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
