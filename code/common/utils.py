import time
import pandas as pd
from datetime import datetime, timedelta
from math import floor


def retry_wrapper(func, params={}, func_name='', retry_times=5, sleep_seconds=5, enable_retry=True):
    """
    和外部交易所API进行交互时，用wrapper进行自动重试
    :param func:
    :param params:
    :param func_name:
    :param retry_times:
    :param sleep_seconds:
    :param enable_retry:
    :return:
    """
    if not enable_retry:
        return func(params)

    for _ in range(retry_times):
        try:
            return func(**params)
        except Exception as e:
            print(f'{func_name} 报错，错误信息：{e}')
            time.sleep(sleep_seconds)
    else:
        raise Exception(f'{func_name} 报错次数达到上限，程序退出')


def next_run_time(time_interval, ahead_seconds=5, cheat_seconds=0):
    """
    根据time_interval，计算下次运行的时间
    :param time_interval: 时间间隔，单位：秒
    :param ahead_seconds: 时间间隔，单位：秒
    :param cheat_seconds: 时间间隔，单位：秒
    :return:

    比如：
    15m  当前时间为：12:50:51  返回时间为：13:00:00
    15m  当前时间为：12:39:51  返回时间为：12:45:00

    10m  当前时间为：12:38:51  返回时间为：12:40:00
    10m  当前时间为：12:11:01  返回时间为：12:20:00

    5m  当前时间为：12:33:51  返回时间为：12:35:00
    5m  当前时间为：12:34:51  返回时间为：12:40:00

    30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00
    30m  当前时间为：14:37:51  返回时间为：14:56:00

    1h  当前时间为：14:37:51  返回时间为：15:00:00

    """
    if time_interval.endswith('m') or time_interval.endswith('h'):
        pass
    elif time_interval.endswith('T'):
        time_interval = time_interval.replace('T', 'm')
    elif time_interval.endswith('H'):
        time_interval = time_interval.replace('H', 'h')
    else:
        print('time_interval格式不符合规范。程序退出')
        exit()

    ti = pd.to_timedelta(time_interval)
    now_time = datetime.now()
    # 计算当日时间的零时 00：00：00
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    # 此时距零时已过去多久
    time_passed = now_time - this_midnight
    # 将过去的时间转换为以 time_interval 为单位，那么下一个单位就是下次运行时间
    next_passed = floor(time_passed.total_seconds() / ti.total_seconds()) + 1

    next_time = this_midnight + timedelta(seconds=next_passed * ti.total_seconds())
    if (next_time - datetime.now()).seconds < ahead_seconds:
        next_time += ti

    if cheat_seconds != 0:
        next_time = next_time - timedelta(seconds=cheat_seconds)

    return next_time


def sleep_until_run_time(time_interval, ahead_seconds=1, if_sleep=True, cheat_seconds=0):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param time_interval:
    :param ahead_seconds:
    :param if_sleep:
    :param cheat_seconds:
    :return:
    """
    run_time = next_run_time(time_interval, ahead_seconds, cheat_seconds)
    if if_sleep:
        # sleep 可以传入一个实数作为秒参数
        time.sleep(max(0, (run_time - datetime.now()).total_seconds()))
        # 但是醒来以后不一定完全准确，例如早于目标时间 run_time 醒来，那么需要循环等待
        while True:
            if datetime.now() > run_time:
                break

    return run_time
