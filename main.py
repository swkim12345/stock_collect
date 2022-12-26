import os
# import git
import datetime
import shutil
import requests
import os

from collect_price import stock_info as info
from collect_price import stock_price_procedure as price

if __name__=='__main__':
	today_date = datetime.datetime.today().strftime('%Y_%m_%d')
	dir_seperator = info._dir_seperator_check()
	src = os.path.dirname(__file__) + dir_seperator + 'collect_price'
	tar_dir = 'korea_min_stock_price'
	tar_file = src + dir_seperator + today_date + '.tar.gz'

	info.main()
	price.main()

	try:
		if os.path.isfile(tar_file):
			shutil.move(tar_file, tar_dir)
		else:
			print('not exists')
	except:
		print('not exists')

