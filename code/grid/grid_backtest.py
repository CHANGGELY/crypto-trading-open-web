import time
from datetime import datetime, timedelta

import pandas as pd
from pytz import timezone
from enum import Enum
from api.binance import fetch_candle_data

class Interval_mode(Enum):
    AS = "arithmetic_sequence"
    GS = "geometric_sequence"

eps = 0.000001

class Grid:
    grid_dict = {}  # 网格参数
    account_dict = {}  # 网格状态

    def __init__(self,
                 symbol,
                 money,
                 leverage,
                 interval_mode,
                 interval=0,
                 num_steps_up=0,
                 num_steps_down=0,
                 num_steps=0,
                 num_hours=0,
                 end_time=0,
                 min_price=0,
                 max_price=0,
                 price_range=0,):
        self.symbol = symbol
        self.money = money
        self.leverage = leverage
        self.interval_mode = interval_mode

        self.interval = interval
        self.num_steps_up = num_steps_up
        self.num_steps_down = num_steps_down

        self.num_steps = num_steps
        self.num_kline = int(num_hours * 60)
        self.end_time = end_time
        self.min_price = min_price
        self.max_price = max_price
        self.price_range = price_range
        self.curr_price = 0

        self.max_loss = 0
        self.max_profit = 0

    '''------------------------------ 策略计算工具函数 ------------------------------'''

    def get_down_price(self, price):
        '''计算下一格价格'''

        if self.interval_mode == Interval_mode.GS:
            down_price = price / (1 + self.grid_dict["interval"])
        elif self.interval_mode == Interval_mode.AS:
            down_price = price - self.grid_dict["interval"]

        return down_price

    def get_up_price(self, price):
        '''计算上一格价格'''

        if self.interval_mode == Interval_mode.GS:
            up_price = price * (1 + self.grid_dict["interval"])
        elif self.interval_mode == Interval_mode.AS:
            up_price = price + self.grid_dict["interval"]

        return up_price

    def get_positions_cost(self):
        '''计算持仓成本'''

        total = 0
        p = self.account_dict["positions_grids"]
        c = self.grid_dict["price_central"]

        if p < 0:
            for i in range(-p):
                c = self.get_up_price(c)
                total += c
        elif p > 0:
            for i in range(p):
                c = self.get_down_price(c)
                total += c
        elif p == 0:
            return 0

        return abs(total / p)

    def get_positions_profit(self, price):
        '''计算持仓浮动盈亏'''
        positions_profit = (price - self.account_dict["positions_cost"]) * \
                           self.account_dict["positions_grids"] * \
                           self.grid_dict["one_grid_quantity"]

        return positions_profit

    def get_interval(self):
        '''不传interval时，计算 interval'''

        max_value = self.max_price
        min_value = self.min_price
        num_elements = self.num_steps

        if self.interval_mode == Interval_mode.GS:
            interval = (max_value / min_value) ** (1 / num_elements) - 1
        elif self.interval_mode == Interval_mode.AS:
            interval = (max_value - min_value) / num_elements

        return interval

    def get_price_central(self, new_price):
        '''不传interval时，计算 price_central'''

        max_value = self.max_price
        min_value = self.min_price
        num_elements = self.num_steps

        if self.interval_mode == Interval_mode.GS:
            interval = (max_value / min_value) ** (1 / num_elements)
            price_list = [min_value * (interval ** i) for i in range(num_elements + 1)]
        elif self.interval_mode == Interval_mode.AS:
            interval = (max_value - min_value) / num_elements
            price_list = [min_value + (interval * i) for i in range(num_elements + 1)]

        price_central = min(price_list, key=lambda x: abs(x - new_price))

        return price_central

    def get_one_grid_quantity(self):
        max_value = self.max_price
        min_value = self.min_price
        num_elements = self.num_steps
        if self.interval_mode == Interval_mode.GS:
            interval = (max_value / min_value) ** (1 / num_elements)
            price_list = [min_value * (interval ** i) for i in range(num_elements + 1)]
        elif self.interval_mode == Interval_mode.AS:
            interval = (max_value - min_value) / num_elements
            price_list = [min_value + (interval * i) for i in range(num_elements + 1)]

        return self.money * self.leverage * 0.8 / sum(price_list)



    def get_pair_profit(self, price, side):
        if self.interval_mode == Interval_mode.GS:
            if side == "SELL":
                pair_profit = (price / (1 + self.grid_dict["interval"])) * self.grid_dict["interval"] * self.grid_dict[
                    "one_grid_quantity"]
            elif side == "BUY":
                pair_profit = price * self.grid_dict["interval"] * self.grid_dict["one_grid_quantity"]
        elif self.interval_mode == Interval_mode.AS:
            pair_profit = self.grid_dict["interval"] * self.grid_dict["one_grid_quantity"]

        return pair_profit

    '''------------------------------ 策略初始化函数 ------------------------------'''

    def _init_strategy(self):

        self.grid_dict = {
            "interval": 0,  # 网格间隔
            "price_central": 0,  # 价格中枢
            "one_grid_quantity": 0,  # 每格数量
            "max_price": 0,  # 网格上限
            "min_price": 0,  # 网格下限
        }

        self.account_dict = {
            "positions_grids": 0,  # 当前持仓格数（正数为多，负数为空）
            "pairing_count": 0,  # 配对次数
            "pair_profit": 0,  # 配对收益
            "positions_cost": 0,  # 持仓成本
            "positions_profit": 0,  # 持仓浮动盈亏
            "pending_prders": [],  # 当前挂单列表
            "up_price": 0,  # 上一格价格
            "down_price": 0,  # 下一格价格
        }

        # 初始化 grid_dict
        if self.price_range != 0:
            self.max_price = self.curr_price * (1 + self.price_range)
            self.min_price = self.curr_price * (1 - self.price_range)
        self.grid_dict["interval"] = self.get_interval()
        self.grid_dict["price_central"] = self.get_price_central(self.curr_price)
        self.grid_dict["one_grid_quantity"] = self.get_one_grid_quantity()
        self.grid_dict["max_price"] = self.max_price
        self.grid_dict["min_price"] = self.min_price
        self.account_dict["up_price"] = self.get_up_price(self.grid_dict["price_central"])
        self.account_dict["down_price"] = self.get_down_price(self.grid_dict["price_central"])

        print(self.grid_dict)
        print(self.account_dict)

    '''----------------------------- 信号执行函数 ---------------------------------'''

    def reset_grid(self, side):
        '''破网处理'''

        print('破网终止')
        print(f'已配对盈利 {self.account_dict["pair_profit"]}')
        print(f'未配对盈利 {self.account_dict["positions_profit"]}')
        print(f'总盈利 {self.account_dict["pair_profit"] + self.account_dict["positions_profit"]}')
        exit()

    def update_order(self, ts, price, side):
        '''价格在price触发了挂单，更新配对和挂单信息'''

        if side == "BUY":
            self.account_dict["positions_grids"] += 1
        elif side == "SELL":
            self.account_dict["positions_grids"] -= 1

        # 更新account_dict
        self.account_dict["positions_cost"] = self.get_positions_cost()
        self.account_dict["positions_profit"] = self.get_positions_profit(price)
        if side == "BUY" and self.account_dict["positions_grids"] <= 0:
            self.account_dict["pairing_count"] += 1
            self.account_dict["pair_profit"] += self.get_pair_profit(price, side)
            print(f'{ts} 配对成功收益 {self.account_dict["pair_profit"]}')
        elif side == "SELL" and self.account_dict["positions_grids"] >= 0:
            self.account_dict["pairing_count"] += 1
            self.account_dict["pair_profit"] += self.get_pair_profit(price, side)
            print(f'{ts} 配对成功收益 {self.account_dict["pair_profit"]}')

        #print(f'{ts} 在 {price} 触网，账户持仓 {self.account_dict}')

        pl = self.account_dict["positions_profit"] + self.account_dict["pair_profit"]
        self.max_loss = min(pl, self.max_loss)
        self.max_profit = max(pl, self.max_profit)

        # 新建委托
        if price >= self.grid_dict["max_price"] or price <= self.grid_dict["min_price"]:
            self.reset_grid()
        else:
            self.account_dict["down_price"] = self.get_down_price(price)
            self.account_dict["up_price"] = self.get_up_price(price)

    def update_price(self, ts, new_price):

        while True:
            up_price = self.account_dict["up_price"]
            down_price = self.account_dict["down_price"]
            # 价格没动
            if abs(new_price - self.curr_price) < eps:
                return

            # 价格上涨，但并未触网，无需更新持仓和挂单
            if new_price > self.curr_price and new_price < up_price - eps:
                self.curr_price = new_price
                return

            # 价格下跌，但并未触网，无需更新持仓和挂单
            if new_price < self.curr_price and new_price > down_price + eps:
                self.curr_price = new_price
                return

            # 触网，更新持仓和配对信息
            if new_price > self.curr_price:
                self.update_order(ts, up_price, 'SELL')
            else:
                self.update_order(ts, down_price, 'BUY')

    '''----------------------------- 策略启动停止函数 -----------------------------'''

    def main(self):
        df = fetch_candle_data(self.symbol, self.end_time, '1m', self.num_kline)
        # 转换时区
        utc_offset = int(time.localtime().tm_gmtoff / 60 / 60)
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + timedelta(hours=utc_offset)

        # 设置当前价格
        self.curr_price = df['open'].iloc[0]
        self._init_strategy()

        for index, row in df.iterrows():
            self.update_price(row['candle_begin_time'], row['open'])

            # 如果这根k线收跌，那么先运动到最高价，再运动到最低价
            if row['close'] < row['open']:
                self.update_price(row['candle_begin_time'], row['high'])
                self.update_price(row['candle_begin_time'], row['low'])
            # 如果这根k线收涨，那么先运动到最低价，再运动到最高价
            else:
                self.update_price(row['candle_begin_time'], row['low'])
                self.update_price(row['candle_begin_time'], row['high'])

            self.update_price(row['candle_begin_time'], row['close'])

        print(f'最大盈利 {self.max_profit}')
        print(f'最大亏损 {self.max_loss}')
        print(f'每笔数量 {self.grid_dict["one_grid_quantity"]}')
        print(f'已配对次数 {self.account_dict["pairing_count"]}')
        pl = self.account_dict["pair_profit"]
        print(f'已配对盈利 {pl} ({pl/self.money * 100})')
        pl = self.account_dict["positions_profit"]
        print(f'未配对盈利 {pl} ({pl/self.money * 100})')
        pl = self.account_dict["pair_profit"] + self.account_dict["positions_profit"]
        print(f'总盈利 {pl} ({pl/self.money * 100})')

if __name__ == "__main__":
    """============================= 网格策略 =================================="""
    # 初始投入资金
    money = 1163.88
    # 杠杆
    leverage = 10
    # 交易对
    symbol = "TRBUSDT"
    # 间隔模式（AS表示等差，GS表示等比）
    interval_mode = Interval_mode.GS
    # 回测时长6小时
    num_hours = 12

    beijing = timezone('Asia/Shanghai')
    # 回测的最后时间，精确到分钟
    end_time = beijing.localize(datetime(2023, 11, 5, 10, 48))
    #end_time = datetime.now()
    # 网格总数量
    num_steps = 64
    # 网格最低阶
    min_price = 72.847
    # 网格最高价
    max_price = 151.299
    # 网格最高和最低离现价的距离
    price_range = 0

    arbitrage = Grid(
        symbol=symbol,
        money=money,
        leverage=leverage,
        interval_mode=interval_mode,
        num_steps=num_steps,
        num_hours=num_hours,
        end_time=end_time,
        price_range=price_range,
        min_price=min_price,
        max_price=max_price,
    )

    arbitrage.main()