import func as fun
import kis_api as kis
import kis_develop as kis_dev
import multiprocessing as mp
import os
import asyncio
import time
import datetime
import shutil
import git

csv_column = 'prdy_vrss','prdy_vrss_sign','prdy_ctrt','stck_prdy_clpr','acml_vol','acml_tr_pbmn','hts_kor_isnm','stck_prpr','stck_bsop_date','stck_cntg_hour','stck_prpr','stck_oprc','stck_hgpr','stck_lwpr','cntg_vol','acml_tr_pbmn'

async def korea_30min_stock_price(account_info, stock_num, target_dir, slack_url):
	global csv_column
	stock_price = list()
	time = datetime.datetime(year=2022, month=12, day=12, hour=9, minute=29, second=0)

	for i in range(0, 19):
		tmp = (await kis._domestic_stock_min_price(account_info, stock_num, slack_url, stock_num, time.strftime('%H%M%S'), True)).json()
		if tmp['rt_cd'] != '0':
			fun._send_slack(slack_url, 'Error : Not Invalid Return - ' + stock_num)
		for output in tmp['output2']:
			stock_price.append(list(list(tmp['output1'].values()) + list(output.values())))
		time += datetime.timedelta(minutes=30)

	stock_price.sort(key=lambda x:x[9])
	await fun._writerow_csv(target_dir, stock_num, csv_column)
	await fun._writerows_csv(target_dir, stock_num, stock_price)

def korea_price_subprocess(account_info: dict, stock_list: tuple, target_dir: str, slack_url: str) -> None:
	error_stock = []

	fun._send_slack(slack_url, f'Start Collect Subprocess - {os.getpid()}')

	for stock in stock_list:
		try:
			asyncio.run(korea_30min_stock_price(account_info, stock, target_dir, slack_url))
		except Exception as e:
			error_stock.append(stock)
			fun._send_slack(slack_url, 'Min Price Subprocess Error! \nStock - ' + stock)
			print(e)
			time.sleep(1)
			continue
	for stock in error_stock:
		try:
			asyncio.run(korea_30min_stock_price(account_info, stock, target_dir, slack_url))
		except:
			fun._send_slack(slack_url, 'Retry Error! \nStock - ' + stock)
			continue

	fun._send_slack(slack_url, f'End Collect Subprocess - {os.getpid()}')

def _account_info(string, key_file) -> dict:
	string = str(string)

	account_info = {
		'key' : key_file[f'{string}_key'],
		'secret' : key_file[f'{string}_secret'],
		'check_real' : key_file[f'{string}_check']
	}
	account_info = kis._new_app_token(account_info)
	return account_info

if __name__=='__main__':
	base_dir = os.path.dirname(__file__)
	conf_dir = base_dir + '/conf'
	key_file = fun._read_yaml(conf_dir, 'key.yaml')
	url_file = fun._read_yaml(conf_dir, 'url.yaml')
	slack_url = url_file['slack_webhook_url']

	account_list = (_account_info('5007', key_file), _account_info('6363', key_file), _account_info('4660', key_file), _account_info('7390', key_file))

	dir_sep = fun._dir_seperator_check()
	today = datetime.datetime.today()
	target = base_dir + dir_sep + today.strftime('%Y_%m_%d')
	kospi_dir = target + dir_sep + 'kospi'
	kosdaq_dir = target + dir_sep + 'kosdaq'

	fun._create_folder(target, 'kospi')
	fun._create_folder(target, 'kosdaq')

	kis_dev.kosdaq_master_download(base_dir)
	df = kis_dev.get_kosdaq_master_dataframe(base_dir)
	kosdaq_list = tuple(df['단축코드'])

	df.to_excel(kosdaq_dir + dir_sep + 'kosdaq_code.xlsx', index=False)
	os.remove('kosdaq_code.mst')

	kis_dev.kospi_master_download(base_dir)
	df = kis_dev.get_kospi_master_dataframe(base_dir)
	kospi_list = tuple(df['단축코드'])

	df.to_excel(kospi_dir + dir_sep + 'kosdpi_code.xlsx', index=False)
	os.remove('kospi_code.mst')

	start_time = datetime.datetime.today()
	fun._send_slack(slack_url, f'Start Collect kospi min price \n Start in {start_time.strftime("%Y년 %m월 %d일 %H시 %M분 %S초")}')

	kospi_sep = int(len(kospi_list) / len(account_list))
	process = []
	for i in range(len(account_list)):
		process.append(mp.Process(target=korea_price_subprocess, args=(account_list[i], kospi_list[kospi_sep * i : kospi_sep * (i + 1)], kospi_dir, slack_url)))

	for p in process:
		p.start()

	for p in process:
		p.join()

	end_time = datetime.datetime.today()
	fun._send_slack(slack_url, f'End Collect kospi min price \n End in {end_time.strftime("%Y년 %m월 %d일 %H시 %M분 %S초")}\n\n\n')

	start_time = datetime.datetime.today()
	fun._send_slack(slack_url, f'Start Collect kosdaq min price \n Start in {start_time.strftime("%Y년 %m월 %d일 %H시 %M분 %S초")}')

	kosdaq_sep = int(len(kosdaq_list) / len(account_list))

	process = []
	for i in range(len(account_list)):
		process.append(mp.Process(target=korea_price_subprocess, args=(account_list[i], kosdaq_list[kosdaq_sep * i : kosdaq_sep * (i + 1)], kosdaq_dir, slack_url)))

	for p in process:
		p.start()

	for p in process:
		p.join()

	end_time = datetime.datetime.today()
	fun._send_slack(slack_url, f'End Collect kosdaq min price \n End in {end_time.strftime("%Y년 %m월 %d일 %H시 %M분 %S초")}\n\n\n')

	tarfile_name = today.strftime('%Y_%m_%d') + '.tar.gz'
	git_dir = base_dir + dir_sep + 'korea_min_stock_price'
	fun._make_tar(today.strftime('%Y_%m_%d'), base_dir, tarfile_name)

	try:
		shutil.move(base_dir + dir_sep + tarfile_name, git_dir)
	except Exception as e:
		kis._send_slack(slack_url, 'Main Error! PLZ check log file.')
		print(e)

	try:
		repo = git.Repo(git_dir)
		index = repo.index
		remote = repo.remote()
	except:
		kis._send_slack(slack_url, 'Error in Git init')
	try:
		index.add(git_dir + dir_sep + tarfile_name)
		index.commit(f'add {tarfile_name} file')
		remote.push()
	except:
		kis._send_slack(slack_url, f'Error in Git push as {tarfile_name}')

	end_time = datetime.datetime.today()
	fun._send_slack(slack_url, f'Push Tar File \n End in {end_time.strftime("%Y년 %m월 %d일 %H시 %M분 %S초")}\n\n\n')
