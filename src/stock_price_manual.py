import func as fun
import kis_api as kis
import kis_develop as kis_dev
import stock_price_multiprocess as stock

import os
import asyncio
import sys

if __name__=='__main__':
	base_dir = os.path.dirname(__file__)
	conf_dir = base_dir + '/conf'
	key_file = fun._read_yaml(conf_dir, 'key.yaml')
	url_file = fun._read_yaml(conf_dir, 'url.yaml')
	slack_url = url_file['slack_webhook_url']
	stock_num = str(sys.argv[1])

	account_info = stock._account_info('5007', key_file)

	try:
		asyncio.run(stock.korea_30min_stock_price(account_info, stock_num, base_dir, slack_url))
	except:
		print(f"Error: PLZ check your argument \n{stock_num}")

