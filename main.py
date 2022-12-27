import os
import git
import datetime
import shutil
import os

from collect_price import stock_info as info
from collect_price import stock_price_procedure as price

if __name__=='__main__':
	today_date = datetime.datetime.today().strftime('%Y_%m_%d')
	dir_seperator = info._dir_seperator_check()
	src = os.path.dirname(__file__) + dir_seperator + 'collect_price'
	tar_dir = os.path.dirname(__file__) + dir_seperator + 'korea_min_stock_price'
	tar_file = today_date + '.tar.gz'
	url_file = 'url.yaml'

	info.main()
	price.main()

	url = price._read_yaml(src + dir_seperator + 'conf', url_file)

	try:
		shutil.move(src + dir_seperator + tar_file, tar_dir)
	except FileNotFoundError as e:
		price._send_slack(url['slack_webhook_url'], f'File Not Found Error! \n Detail \n {e}')
	except:
		price._send_slack(url['slack_webhook_url'], 'Main Error! PLZ check log file.')

	try:
		repo = git.Repo(tar_dir)
		index = repo.index
		remote = repo.remote()
	except:
		print("error in init")
	try:
		index.add(tar_dir + dir_seperator + tar_file)

	except:
		print('Error in git error in add')
	try:
		index.commit(f'add {today_date}.tar.gz file')
	except:
		print("error in commit")
	try:
		remote.push()
	except:
		print("error in push")
	price._send_slack(url['slack_webhook_url'], 'End collect price')
