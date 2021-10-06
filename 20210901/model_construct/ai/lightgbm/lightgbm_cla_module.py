# import lightgbm as lgb
# from sklearn import datasets
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import accuracy_score
#
#
# # 加载数据
# iris = datasets.load_iris()
# data = iris.data
# target = iris.target
#
# # 划分训练数据和测试数据
# X_train, X_test, y_train, y_test = train_test_split(data, target, test_size=0.2)
#
# # 转换为Dataset数据格式
# train_data = lgb.Dataset(X_train, label=y_train)
# validation_data = lgb.Dataset(X_test, label=y_test)
#
# # 参数
# params = {
#     'learning_rate': 0.1,
#     'lambda_l1': 0.1,
#     'lambda_l2': 0.2,
#     'max_depth': 4,
#     'objective': 'multiclass',  # 目标函数
#     'num_class': 3,
#     'verbose': -1
# }
#
# # 模型训练
# gbm = lgb.train(params, train_data, valid_sets=[validation_data])
#
# # 模型预测
# y_pred = gbm.predict(X_test)
# y_pred = [list(x).index(max(x)) for x in y_pred]
# print(y_pred)
#
# # 模型评估
# print(accuracy_score(y_test, y_pred))


from lightgbm import LGBMClassifier
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV


# 1、加载数据
iris = load_iris()

# 2、数据划分：data、target
data = iris.data
target = iris.target

# 3、数据划分：训练数据和测试数据
X_train, X_test, y_train, y_test = train_test_split(data, target, test_size=0.2)

# 4、模型训练
gbm = LGBMClassifier(num_leaves=31, learning_rate=0.05, n_estimators=80)
gbm.fit(X_train, y_train, eval_set=[(X_test, y_test)], early_stopping_rounds=5, verbose=False)

# import joblib
# # 5、模型存储
# joblib.dump(gbm, 'loan_model.pkl')
# # 模型加载
# gbm = joblib.load('loan_model.pkl')

# 6、模型预测
y_pred = gbm.predict(X_test, num_iteration=gbm.best_iteration_)

# 7、模型评估
print('The accuracy of prediction is:', accuracy_score(y_test, y_pred))
print('Feature importances:', list(gbm.feature_importances_))

# 8、网格搜索，参数优化
estimator = LGBMClassifier(num_leaves=31)
param_grid = {
    'learning_rate': [0.01, 0.05, 0.1, 1],
    'n_estimators': [20, 40, 60, 80, 100],
}
gbm = GridSearchCV(estimator, param_grid)
gbm.fit(X_train, y_train)
print('Best parameters found by grid search are:', gbm.best_params_)
