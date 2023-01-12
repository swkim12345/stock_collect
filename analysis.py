'''
csv 파일을 분석하는 파이썬 프로그램
'''
import csv
import os
import pandas as pd

def _read_csv(dir, file_name):
	ret = []
	with open(dir + '/' + file_name, 'r') as f:
		p = csv.reader(f)
		for line in p:
			ret.append(line[1:4])
	return ret

dir = os.path.dirname(__file__)
df = pd.DataFrame(_read_csv(dir, 'result.csv'))
df = df.astype(float)

print(df)


import matplotlib.pyplot as plt
import numpy as np

X = df.iloc[:, 0].values
Y = df.iloc[:, 2].values * df.iloc[:,1].values

result = X.sum() / 3232
print(result)

# plt.scatter(X, Y)
# plt.show()

print(np.corrcoef(X, Y)[0, 1])
