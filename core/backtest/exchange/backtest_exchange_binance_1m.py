from __future__ import annotations
import csv
import math
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class 网格参数:
    最低价: float
    最高价: float
    档距: float
    初始数量: float
    费用_maker_bps: float = 0.0
    费用_taker_bps: float = 0.0
    撮合模式: str = "maker"


@dataclass
class 账户:
    网格: 网格参数
    当前位置: float = 0.0
    持仓格数: int = 0
    上一档价格: float = 0.0
    下一档价格: float = 0.0
    配对次数: int = 0
    配对收益: float = 0.0
    持仓成本: float = 0.0
    持仓浮盈: float = 0.0
    最大亏损: float = 0.0
    最大盈利: float = 0.0
    多头持仓价: List[float] = field(default_factory=list)
    空头持仓价: List[float] = field(default_factory=list)
    容差: float = 1e-9

    def 初始化(self, 初始价: float) -> None:
        self.当前位置 = 初始价
        self.下一档价格 = self.下一档计算(初始价)
        self.上一档价格 = self.上一档计算(初始价)

    def 下一档计算(self, 价: float) -> float:
        步 = self.网格.档距
        值 = math.floor((价 - self.网格.最低价) / 步) * 步 + 步 + self.网格.最低价
        if 值 > self.网格.最高价:
            值 = self.网格.最高价
        return 值

    def 上一档计算(self, 价: float) -> float:
        步 = self.网格.档距
        值 = math.ceil((价 - self.网格.最低价) / 步) * 步 - 步 + self.网格.最低价
        if 值 < self.网格.最低价:
            值 = self.网格.最低价
        return 值

    def 费用计算(self, 成交价: float, 方向: str) -> float:
        单边 = self.网格.费用_maker_bps if self.网格.撮合模式 == "maker" else self.网格.费用_taker_bps
        return 成交价 * self.网格.初始数量 * 单边 / 10000.0

    def 更新持仓统计(self) -> None:
        多盈 = sum(self.当前位置 - p for p in self.多头持仓价) * self.网格.初始数量
        空盈 = sum(p - self.当前位置 for p in self.空头持仓价) * self.网格.初始数量
        self.持仓浮盈 = 多盈 + 空盈
        self.持仓成本 = sum(self.多头持仓价) * self.网格.初始数量 + sum(self.空头持仓价) * self.网格.初始数量

    def 记录极值(self) -> None:
        总盈 = self.持仓浮盈 + self.配对收益
        if 总盈 < self.最大亏损:
            self.最大亏损 = 总盈
        if 总盈 > self.最大盈利:
            self.最大盈利 = 总盈

    def 重建网格(self) -> None:
        self.下一档价格 = self.下一档计算(self.当前位置)
        self.上一档价格 = self.上一档计算(self.当前位置)

    def 更新订单(self, 时间戳: str, 价格: float, 方向: str) -> None:
        if 方向 == "BUY":
            if len(self.空头持仓价) > 0:
                空入 = self.空头持仓价.pop(0)
                利 = (空入 - 价格) * self.网格.初始数量
                成本 = self.费用计算(空入, "SELL") + self.费用计算(价格, "BUY")
                self.配对收益 += 利 - 成本
                self.配对次数 += 1
            else:
                self.多头持仓价.append(价格)
            self.持仓格数 += 1
        elif 方向 == "SELL":
            if len(self.多头持仓价) > 0:
                多入 = self.多头持仓价.pop(0)
                利 = (价格 - 多入) * self.网格.初始数量
                成本 = self.费用计算(多入, "BUY") + self.费用计算(价格, "SELL")
                self.配对收益 += 利 - 成本
                self.配对次数 += 1
            else:
                self.空头持仓价.append(价格)
            self.持仓格数 -= 1
        self.更新持仓统计()
        self.记录极值()
        if 价格 >= self.网格.最高价 or 价格 <= self.网格.最低价:
            self.重建网格()
        else:
            self.下一档价格 = self.下一档计算(价格)
            self.上一档价格 = self.上一档计算(价格)

    def 更新价格(self, 时间戳: str, 新价: float) -> None:
        while True:
            上价 = self.上一档价格
            下价 = self.下一档价格
            if abs(新价 - self.当前位置) < self.容差:
                self.当前位置 = 新价
                return
            if 新价 > self.当前位置 and 新价 < 上价 - self.容差:
                self.当前位置 = 新价
                self.更新持仓统计()
                return
            if 新价 < self.当前位置 and 新价 > 下价 + self.容差:
                self.当前位置 = 新价
                self.更新持仓统计()
                return
            if 新价 > self.当前位置:
                self.更新订单(时间戳, 上价, "SELL")
                self.当前位置 = 上价
            else:
                self.更新订单(时间戳, 下价, "BUY")
                self.当前位置 = 下价


class 币安一分回测交易所:
    def __init__(self, 参数: 网格参数):
        self.参数 = 参数
        self.账户 = 账户(网格=参数)
        self.数据: List[Dict[str, str]] = []

    def 加载CSV(self, 路径: str) -> None:
        with open(路径, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            必要 = {"timestamp", "open", "high", "low", "close", "volume"}
            if not 必要.issubset(set(r.fieldnames or [])):
                raise ValueError("CSV缺少必要列")
            self.数据 = list(r)

    def 运行(self) -> Dict[str, float]:
        if not self.数据:
            raise ValueError("无数据")
        首开 = float(self.数据[0]["open"])
        self.账户.初始化(首开)
        for 行 in self.数据:
            ts = 行["timestamp"]
            o = float(行["open"])
            h = float(行["high"])
            l = float(行["low"])
            c = float(行["close"])
            self.账户.更新价格(ts, o)
            if c < o:
                self.账户.更新价格(ts, h)
                self.账户.更新价格(ts, l)
            else:
                self.账户.更新价格(ts, l)
                self.账户.更新价格(ts, h)
            self.账户.更新价格(ts, c)
        总盈 = self.账户.持仓浮盈 + self.账户.配对收益
        return {
            "pairing_count": float(self.账户.配对次数),
            "pair_profit": self.账户.配对收益,
            "positions_pnl": self.账户.持仓浮盈,
            "total_pnl": 总盈,
            "max_profit": self.账户.最大盈利,
            "max_loss": self.账户.最大亏损,
        }