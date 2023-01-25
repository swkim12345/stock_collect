from sqlalchemy import (
	create_engine,
	MetaData,
	text
)

from sqlalchemy.orm import (
	Session,
)
import os
import pandas as pd
from pandas import DataFrame
import csv
import datetime

'''
전일 대비 상승(원화), 전일 대비 부호( 2 : 상승, 3: 보합, 5 : 하락), 전일 대비율, 주식 전일 종가, 누적 거래량, 누적 거래 대금
한글 종목명, 주식 현재가(요청 시점 기준), 주식 영업일자, 주식 체결 시간, 누적 거래 대금, 주식 현재가(시간 기준), 주식 시가2
'''
min_price_column = ('prdy_vrss','prdy_vrss_sign','prdy_ctrt','stck_prdy_clpr','acml_vol',
'acml_tr_pbmn_now','hts_kor_isnm','stck_prpr_now','stck_bsop_date','stck_cntg_hour',
'stck_prpr','stck_oprc','stck_hgpr','stck_lwpr','cntg_vol','acml_tr_pbmn')

def database(database_name):
	dir = os.path.dirname(__file__)
	engine = create_engine(f'sqlite:////{dir}/{database_name}.db', echo=False)

	session = Session(engine)

	metadata = MetaData()

	metadata.reflect(engine)

	table_name = list(metadata.tables.keys())

	conn = engine.connect()

	return conn, table_name

def _table_want(conn, table_name, date) -> DataFrame:
	year, month, day = date.split(',')
	start_date = datetime.datetime(year=int(year), month=int(month), day=int(day))
	try:
		result = conn.execute(text(
			f"SELECT prdy_vrss, prdy_vrss_sign, prdy_ctrt, stck_prdy_clpr, acml_vol, acml_tr_pbmn_now, stck_prpr_now, stck_bsop_date, stck_cntg_hour, stck_prpr, stck_oprc, stck_hgpr, stck_lwpr, cntg_vol, acml_tr_pbmn \
			FROM '{table_name}'\
			WHERE stck_bsop_date = {start_date.strftime('%Y%m%d')} and stck_cntg_hour >= 090000 and stck_cntg_hour <= 180000"
		))
	except:
		raise Exception
	df = pd.DataFrame(result.all())
	return df

# turtle trading 방식으로 트레이딩을 진행함.
# def turtle_trading(df):
# 	result_file = open('result.csv', mode='w')
# 	file = csv.writer(result_file)
# 	moving_minutes = 1
# 	start_time = datetime.datetime(year=2023, month=1, day=1, hour=9, minute=0)
# 	seperate_time = datetime.datetime(year=2023, month=1, day=1, hour=9, minute=10)
# 	end_time = datetime.datetime(year=2023, month=1, day=1, hour=9, minute=21)
# 	for i in range(360):

def turtle_trading(df, start_date):
	result_file = open('result.csv', mode='w')
	file = csv.writer(result_file)
	moving_minutes = 1
	year, month, day = start_date.split(',')
	start_time = datetime.datetime(year=)

if __name__=='__main__':
	conn, table_name = database('diff_table_2022_12')
	print(len(table_name))
	df_list = []
	for table in table_name:
		if 'Q' or 'F' or 'J' in table:
			table_name.remove(table)
	print(len(table_name))
	for table in table_name:
		df = _table_want(conn, table, '2022,12,22')
		df['stck_name'] = float(table)
		df_list.append(df)
	for table in table_name:
		df = _table_want(conn, table, '2022,12,23')
		df['stck_name'] = float(table)
		df_list.append(df)

	df = pd.concat(df_list)
