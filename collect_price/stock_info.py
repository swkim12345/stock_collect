from . import kis_develop as kis

import os
import platform
import datetime

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

def main():
	base_dir = os.path.dirname(__file__)

	target_date = datetime.datetime.today().strftime('%Y_%m_%d')
	dir_seperator = _dir_seperator_check()

	target_folder = _create_folder(base_dir + dir_seperator + target_date, 'kosdaq')

	kis.kosdaq_master_download(base_dir)

	df = kis.get_kosdaq_master_dataframe(base_dir)

	df.to_excel(target_folder + dir_seperator + 'kosdaq_code.xlsx', index=False)
	os.remove('kosdaq_code.mst')

	target_folder = _create_folder(base_dir + dir_seperator + target_date, 'kospi')
	kis.kospi_master_download(base_dir)

	df = kis.get_kospi_master_dataframe(base_dir)

	df.to_excel(target_folder + dir_seperator + 'kospi_code.xlsx', index=False)
	os.remove('kospi_code.mst')
