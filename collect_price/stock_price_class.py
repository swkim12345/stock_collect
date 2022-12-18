from openpyxl import load_workbook

import os
import yaml
import requests
import platform
from abc import * #Abstract base class, 추상클래스를 명시적으로 만들기 위한 라이브러리.

'''
StockMinPrice, ForeignStockMinPrice, DomStockMinPrice가 상속함.
'''
class KisDevelop(metaclass=ABCMeta):
	def __init__(self, base_dir, conf_dir, key_file, url_file):
		self.base_dir = base_dir
		self.conf_dir = conf_dir
		self.dir_seperator = self._dir_sepreator_check()
		self.key = self._read_yaml(conf_dir + self.dir_seperator + key_file)
		self.url = self._read_yaml(conf_dir + self.dir_seperator + url_file)


	def _dir_seperator_check(self):
		if platform.system() == 'Windows':
			return '\\'
		else:
			return '/'

	def _read_yaml(self, file_name):
		file_dir = self.base_dir + self._dir_seperator + file_name
		with open(file_dir) as f:
			ret = yaml.load(f, Loader=yaml.FullLoader)

		return ret

	def _read_xlxs(self, file_name):
		file_dir = self.base_dir + self._dir_seperator + file_name
		load_wb = load_workbook(file_dir, read_only=True)
		ret = load_wb['Sheet1']

		return ret

	def _write_yaml(self, file_name, data):
		file_dir = self.base_dir + self._dir_seperator + file_name
		with open(file_dir, 'w') as f:
			yaml.dump(data=data, stream=f)

	def stock_price(self):
		pass

class DomStockMinPrice(KisDevelop):
	


