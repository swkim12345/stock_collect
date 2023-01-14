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

def database():
	dir = os.path.dirname(__file__)
	engine = create_engine(f'sqlite:////{dir}/database.db', echo=False)

	session = Session(engine)

	metadata = MetaData()

	metadata.reflect(engine)

	table_name = list(metadata.tables.keys())

	conn = engine.connect()

	return conn, table_name

def main(conn, table_name):
	result_file = open('result.csv', mode='w')
	file = csv.writer(result_file)
	stock_info = []
	moving_minutes = 1
	start_time = datetime.datetime(year=2023, month=1, day=1, hour=9, minute=0)
	seperate_time = datetime.datetime(year=2023, month=1, day=1, hour=9, minute=10)
	end_time = datetime.datetime(year=2023, month=1, day=1, hour=9, minute=21)
	for i in range(360):
		stock_list = []
		for table in table_name:
			try:
				result = conn.execute(text
				(f"SELECT stck_cntg_hour, acml_tr_pbmn, stck_prpr, stck_hgpr, stck_lwpr FROM '{table}' WHERE stck_bsop_date = 20221222 and stck_cntg_hour >= '{start_time.strftime('%H%m00')}' and stck_cntg_hour <= '{end_time.strftime('%H%m00')}'"))
			except:
				print(f"Error in {table}!")
				continue
			df = pd.DataFrame(result.all())
			try:
				max_df = df['stck_cntg_hour'] >= start_time.strftime('%H%m00') and df['stck_cntg_hour'] < end_time.strftime('%H%m00')
				min_df = df['stck_cntg_hour'] >= seperate_time.strftime('%H%m00') and df['stck_cntg_hour'] < end_time.strftime('%H%m00')
				stock_now_price = df['stck_cntg_hour'][f"{end_time.strftime('%H%m00')}"]
				if max_df.max < stock_now_price:
					stock_list.append((max_df.max / stock_now_price, table, stock_now_price))
			except:
				print('Error')
				continue
		stock_list.sort()
		print(stock_list[0])
		start_time += datetime.timedelta(minutes=moving_minutes)
		seperate_time += datetime.timedelta(minutes=moving_minutes)
		end_time += datetime.timedelta(minutes=moving_minutes)

	initial_money = 100000000


(conn, table_name) = database()
main(conn,table_name)
# for table in table_name:
# 	#1000만원으로 주식 하나로 거래.
# 	if table[0] == 'J' or table[0] == 'Q' or table[0] == 'F':
# 		continue
# 	initial_money = 10000000
# 	stock = 0
# 	result = conn.execute(text(f"SELECT stck_bsop_date, stck_cntg_hour, stck_prpr, stck_hgpr, stck_lwpr, acml_tr_pbmn_now FROM '{table}' WHERE stck_cntg_hour < 152000"))
# 	df = pd.DataFrame(result.all())
# 	df.


	# twenty_max_price = df['stck_hgpr'].rolling(window=240).max()
	# ten_min_price = df['stck_lwpr'].rolling(window=120).min()
	# # print(twenty_max_price)
	# # print('\n\n')
	# # print(ten_min_price)
	# for i in range(10, len(df) - 11):
	# 	get_price = 0
	# 	try:
	# 		if df['stck_prpr'][i] == 0:
	# 			break
	# 		elif df['stck_prpr'][i + 1] > twenty_max_price[i]:
	# 			stock += initial_money / df['stck_prpr'][i + 1]
	# 			initial_money -= initial_money / df['stck_prpr'][i + 1] * df['stck_prpr'][i + 1]
	# 			get_price = df['stck_prpr'][i + 1]
	# 		elif df['stck_prpr'][i + 1] < ten_min_price[i]:
	# 			initial_money += stock * df['stck_prpr'][i + 1] * 0.995
	# 			stock = 0
	# 		# elif df['stck_prpr'][i + 1] < get_price:
	# 		# 	initial_money += stock * df['stck_prpr'][i + 1] * 0.995
	# 		# 	stock = 0
	# 	except:
	# 		stock = 0
	# 		initial_money = 10000000
	# 		break
	# output = stock * df.iloc[-1]['stck_prpr'] + initial_money
	# money += output
	# if output < 1000000:
	# 	print(output)
	# file.writerow(list((table, output, df['stck_prpr'][0], df['acml_tr_pbmn_now'][0])))

