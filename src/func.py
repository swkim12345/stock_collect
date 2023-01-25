import yaml
import csv
import platform
import requests
import tarfile
import os
import git

from openpyxl import load_workbook

def _dir_seperator_check():
	'''함수의 구분자를 os마다 판단해서 리턴하는 값'''
	if platform.system() == 'Windows':
		return '\\'
	else:
		return '/'

def _read_yaml(base_dir, file_name):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir) as f:
		ret = yaml.load(f, Loader=yaml.FullLoader)

	return ret

def _write_yaml(base_dir, file_name, data):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir, 'w') as f:
		yaml.dump(data=data, stream=f)

def _read_xlxs(base_dir, file_name):
	file_dir = base_dir + _dir_seperator_check() + file_name
	load_wb = load_workbook(file_dir, read_only=True)
	ret = load_wb['Sheet1']

	return ret

async def _writerow_csv(base_dir, file_name, data):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir + '.csv', 'w') as f:
		wr = csv.writer(f)
		wr.writerow(data)

async def _writerows_csv(base_dir, file_name, data):
	file_dir = base_dir + _dir_seperator_check() + file_name
	with open(file_dir + '.csv', 'a') as f:
		wr = csv.writer(f)
		wr.writerows(data)

def _create_folder(base_dir, folder_name):
	folder = base_dir + _dir_seperator_check() + folder_name
	if not os.path.exists(folder):
		os.makedirs(folder)
	return folder

def _make_tar(target, target_dir, tar_name):
	os.chdir(target_dir)
	if os.path.exists(target):
		with tarfile.open(tar_name, 'w:gz') as tar:
			tar.add(target)

def _send_slack(url, message):
	'''slack web hook으로 text인 메세지를 보내는 함수.'''
	try:
		data = {'text' : message}
		return requests.post(url=url, json=data)
	except:
		print('webhook error!')
		print(f'detail \n{message}')

def _unzip_tar(src_dir, dst_dir, tar_name):
	with tarfile.open(src_dir + _dir_seperator_check() + tar_name, mode='r') as tar:
		try:
			tar.extractall(path=dst_dir)
		except:
			print("Error : unzip tar")
