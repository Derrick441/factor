import numpy as np
import pandas as pd
import lightgbm as lgb
import matplotlib.pyplot as plt
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_curve, roc_auc_score


# 查看最新挖掘出来特征的效果，并输出五折交叉验证下的五个模型
######################################################################################################################

# 输入：label+特征，输出：默认lgm模型的五折ks、ks均值和五折模型
def cv_ks(df, cats, cv_n=5):
    # dataframe数据（label+特征）转lgb格式数据
    data = lgb.Dataset(df.drop('label', axis=1), df['label'], categorical_feature=cats)  # 第三步

    # cv_n折交叉验证
    kf = StratifiedKFold(n_splits=cv_n, shuffle=True, random_state=2021)
    params = {
        'objective': 'binary',
        'metrics': ['auc'],
        'verbose': -1
    }
    rst = lgb.cv(params, data, folds=kf, num_boost_round=100, early_stopping_rounds=5, return_cvbooster=True, categorical_feature=cats)  # 第四步
    cvb = rst['cvbooster']  # 计算结果存储在这里

    # 计算五折的ks
    ks_list = []
    for idx, (train, val) in enumerate(kf.split(df, df['label'])):
        df_train = df.iloc[train]
        df_val = df.iloc[val]

        bst = cvb.boosters[idx]
        fpr, tpr, thres = roc_curve(df_val['label'], bst.predict(df_val.drop('label', axis=1)))
        ks_list.append((tpr - fpr).max())
    # 五折ks、ks均值和五折模型
    return ks_list, np.mean(ks_list), cvb


# 使用
# ks_l, ks_m, cvb = cv_ks(df, cats, 5)  # df=label+特征


# # 取第一折的数据，具体地观察（特征重要性、roc曲线、ks情况）
# ######################################################################################################################
# # dataframe数据（label+特征）转lgb格式数据
# data = lgb.Dataset(df.drop('label', axis=1), df['label'])
# # cv_n折交叉验证，评估模型性能
# kf = StratifiedKFold(n_splits=cv_n, shuffle=True, random_state=2021)
# params = {
#     'objective': 'binary',
#     'num_threads': 4,
#     'metrics': ['auc', 'binary_error'],
#     'first_metric_only': True,
#     'verbose': -1
# }
# # lightgbm五折交叉验证训练
# rst = lgb.cv(params, data, folds=kf, num_boost_round=1000, early_stopping_rounds=10, return_cvbooster=True)
# cvb = rst['cvbooster']  # 计算结果存储在这里
#
# # 取第一折模型的数据，可以查看数据
# for idx, (train, val) in enumerate(kf.split(df, df['label'])):
#     df_train = df.iloc[train]
#     df_val = df.iloc[val]
#     break
#
# # 取第一折模型的数据，展示其特征重要性
# bst = cvb.boosters[idx]
# fig = plt.figure()
# lgb.plot_importance(bst, ax=fig.gca(), max_num_features=300)
#
# # 取第一折模型的数据，展示其roc曲线和KS
# fpr, tpr, thres = roc_curve(df_val['label'], bst.predict(df_val.drop('label', axis=1)))
# # roc
# fig = plt.figure()
# plt.plot(fpr, tpr)
# plt.title('roc')
# # ks
# fig = plt.figure()
# plt.plot(fpr)
# plt.plot(tpr)
# plt.title('ks')
# plt.legend(['fpr', 'tpr'])


##############################################################################
# 测试1
from sklearn.datasets import load_wine
load = load_wine()
x = pd.DataFrame(load.data)
y = pd.DataFrame(load.target)

index = y[y[0] < 2].index
x = x.iloc[index, :]
y = y.iloc[index, :]
df = pd.DataFrame(x)
df['label'] = y
cats = []
ks_l, ks_m, cvb = cv_ks(df, cats, 5)  # df=label+特征
print(ks_l, ks_m)

#########################################################
# 测试2
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_wine
load = load_wine()
x = pd.DataFrame(load.data)
y = pd.DataFrame(load.target)

index = y[y[0] < 2].index
x = x.iloc[index, :]
y = y.iloc[index, :]
x[3] = x[3].apply(lambda z: str(z))
x[3] = x[3].astype('category')  # 第一步

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)
df1 = pd.DataFrame(x_train)
df1['label'] = y_train

cats = [3]  # 第二步
ks_l, ks_m, cvb = cv_ks(df1, cats, 10)

df2 = pd.DataFrame(x_test)
df2.reset_index(inplace=True)
df2.rename(columns={'index': 'cust_no'}, inplace=True)
np.mean(cvb.predict(df2.drop('cust_no', axis=1)))
