import numpy as np
from bubble_model.useless.LPPLS_WYQ_old import LPPLS, data_in, lppls_compute
from matplotlib import pyplot as plt

dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\舍得酒业半小时复权数据.xls'
ydata, n = data_in('20200320', '20210604', dir)
sta = len(ydata)

# 数据切片
y = ydata[:sta]
x = np.linspace(1, len(y), len(y))
observations = np.array([x, y])

# 估计算法设定
model = LPPLS(observations=observations)
max_searches, minimiza_method = 100, 'SLSQP'

coef = model.fit(max_searches, minimiza_method)
x1 = np.linspace(1, len(x)+8*100, len(x)+8*100)
ydata_predict = lppls_compute(x1, coef[0], coef[1], coef[2], coef[3], coef[4], coef[5], coef[6])

f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x, y, c='b', ls='-')
ax.plot(x1, ydata_predict, c='r', ls='--')
ax.vlines(x=coef[0], ymin=min(ydata), ymax=max(ydata_predict), colors='#FFA500', linestyles='--')
plt.show()

print(coef[0] - len(y))