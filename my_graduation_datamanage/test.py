import pandas as pd


def fun(x_jt, w_jt):
    E = 1 / -0.049
    # w_jt = 0.61
    # x_jt = 23194362000

    e_ww = 0.046
    theta_j = 0.1

    a = E*(w_jt/x_jt)
    b = e_ww*(x_jt/(w_jt*(1.+theta_j/E)))
    c = theta_j/E*w_jt

    # delta_x = a * delta_w
    # delta_x = b * (delta_w + c)
    # b * (delta_w + c) - a*delta_w = 0
    # b * delta_w + b*c - a*delta_w = 0

    delta_w = -(b*c) / (b-a)
    delta_x = b * (delta_w + c)

    delta_ps = delta_w * x_jt - 0.5 * delta_w * delta_x
    delta_wi = theta_j/E*w_jt
    delta_cs = -delta_w * x_jt + 0.5 * delta_wi * delta_x

    DWL = delta_ps + delta_cs
    return delta_cs, delta_ps, DWL

dir = 'C:\\Users\\Administrator\\Desktop\\0326.xls'

data = pd.read_excel(dir)

result = []
for i in range(len(data)):
    result.append(fun(data.iloc[i, 1], data.iloc[i, 1]))

data[['cs', 'ps', 'DWL']] = result
data['cs_pro'] = data.iloc[:, 1] * data.iloc[:, 2] / data.cs

data.to_excel('C:\\Users\\Administrator\\Desktop\\0326_result.xls', index=False)


