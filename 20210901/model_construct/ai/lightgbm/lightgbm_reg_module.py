# import pandas as pd
# from sklearn.model_selection import train_test_split
# import lightgbm as lgb
# from sklearn.metrics import mean_absolute_error
# from sklearn.impute import SimpleImputer
#
# # 1.读文件
# data = pd.read_csv('./dataset/train.csv')
#
# # 2.切分数据输入：特征 输出：预测目标变量
# y = data.SalePrice
# X = data.drop(['SalePrice'], axis=1).select_dtypes(exclude=['object'])
#
# # 3.切分训练集、测试集,切分比例7.5 : 2.5
# train_X, test_X, train_y, test_y = train_test_split(X.values, y.values, test_size=0.25)
#
# # 4.空值处理，默认方法：使用特征列的平均值进行填充
# my_imputer = SimpleImputer()
# train_X = my_imputer.fit_transform(train_X)
# test_X = my_imputer.transform(test_X)
#
# # 5.转换为Dataset数据格式
# lgb_train = lgb.Dataset(train_X, train_y)
# lgb_eval = lgb.Dataset(test_X, test_y, reference=lgb_train)
#
# # 6.参数
# params = {
#     'task': 'train',
#     'boosting_type': 'gbdt',  # 设置提升类型
#     'objective': 'regression',  # 目标函数
#     'metric': {'l2', 'auc'},  # 评估函数
#     'num_leaves': 31,  # 叶子节点数
#     'learning_rate': 0.05,  # 学习速率
#     'feature_fraction': 0.9,  # 建树的特征选择比例
#     'bagging_fraction': 0.8,  # 建树的样本采样比例
#     'bagging_freq': 5,  # k 意味着每 k 次迭代执行bagging
#     'verbose': -1  # <0 显示致命的, =0 显示错误 (警告), >0 显示信息
# }
#
# # 7.调用LightGBM模型，使用训练集数据进行训练（拟合）
# # Add verbosity=2 to print messages while running boosting
# my_model = lgb.train(params, lgb_train, num_boost_round=20, valid_sets=lgb_eval, early_stopping_rounds=5)
#
# # 8.使用模型对测试集数据进行预测
# predictions = my_model.predict(test_X, num_iteration=my_model.best_iteration)
#
# # 9.对模型的预测结果进行评判（平均绝对误差）
# print("Mean Absolute Error : " + str(mean_absolute_error(predictions, test_y)))


from lightgbm import LGBMRegressor
from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import GridSearchCV


# # 1.读文件（处理好的数据）
# import pandas as pd
# data = pd.read_csv('./dataset/train.csv')
#
# # 2.切分数据：y, X
# y = data.SalePrice
# X = data.drop(['SalePrice'], axis=1).select_dtypes(exclude=['object'])

# 1、加载数据
boston = load_boston()

# 2、切分数据：y, X
y = boston.target
X = boston.data

# 3.切分训练集、测试集
train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.3)

# 4.调用LightGBM模型，使用训练集数据进行训练（拟合）
gbm = LGBMRegressor(num_leaves=31, learning_rate=0.1, n_estimators=100)
gbm.fit(train_X, train_y, eval_set=[(test_X, test_y)], early_stopping_rounds=10, verbose=False)

# import joblib
# # 5.模型存储
# joblib.dump(gbm, 'loan_model.pkl')
# # 模型加载
# gbm = joblib.load('loan_model.pkl')

# 6.使用模型对测试集数据进行预测
predictions = gbm.predict(test_X)

# 7.对模型的预测结果进行评判（平均绝对误差）
print("Mean Absolute Error : " + str(mean_absolute_error(predictions, test_y)))
# （特征重要度）
print('Feature importances:', list(gbm.feature_importances_))

# 8、网格搜索，参数优化
estimator = LGBMRegressor(num_leaves=31)
param_grid = {
    'learning_rate': [0.01, 0.05, 0.1, 1],
    'n_estimators': [20, 40, 60, 80, 100]
}
gbm_ = GridSearchCV(estimator, param_grid)
gbm_.fit(train_X, train_y)
print('Best parameters found by grid search are:', gbm_.best_params_)
