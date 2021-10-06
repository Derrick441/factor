# # 测试talib的使用
# import talib as ta
# import numpy as np
#
#
# c = np.random.randn(100)
# k, d = ta.STOCHRSI(c)


# # 测试pytorch的使用
# import torch
#
# torch.__version__
# torch.cuda.is_available()


# # 测试numba的加速
# import numpy as np
# import numba as nb
# from execute_time import print_execute_time
#
#
# @print_execute_time
# def loop_sum(num):
#     sum = 0
#     for i in range(int(num)):
#         sum += i
#
#
# @print_execute_time
# @nb.jit(nopython=True)
# def nb_sum(num):
#     sum = 0
#     for i in range(int(num)):
#         sum += i
#
#
# # 增加numba的gpu加速
# from numba import cuda
# print(cuda.gpus)
#
#
# @print_execute_time
# @nb.cuda.jit(nopython=True)
# def nb_cuda_sum(size):
#     sum = 0
#     for i in range(int(size)):
#         sum += i
#
#
# if __name__ == "__main__":
#     loop_sum(10e5)
#     nb_sum(10e5)
#     # nb_cuda_sum(10e8)

# # 查看每日表现
# ics_ = ics.reset_index()
# fig, ax = plt.subplots(1, 1, figsize=(10, 5))
# ax.bar(ics_['date'], ics_[fac_name + '_ic005'])
# plt.show()
#
# # 查看均值
# print(ics.mean())
#
# 查看累积ic
# ics_cumsum.plot()
# plt.show()
