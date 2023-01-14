from openpyxl import load_workbook

import os
import yaml
import requests
import platform
from abc import * #Abstract base class, 추상클래스를 명시적으로 만들기 위한 라이브러리.

import func as fun

'''
StockMinPrice, ForeignStockMinPrice, DomStockMinPrice가 상속함.
'''
class KisRestApi(metaclass=ABCMeta):
	def __init__(self, base_dir, key, secret):
		self.base_dir = base_dir
		self.dir_seperator = self._dir_sepreator_check()
		self.key = key
		self.secret = secret

	

class DomStockMinPrice(KisRestApi):



