from __future__ import annotations
import csv
import math
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime
import time


@dataclass
class 网格参数:
    最低价: float
    最高价: float
    档距: float
    初始数量: float
    初始资金: float = 10000.0
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
    多头持仓数: int = 0
    空头持仓数: int = 0
    多头价格总和: float = 0.0
    空头价格总和: float = 0.0
    容差: float = 1e-9
    交易额: float = 0.0
    净值序列: List[Dict[str, float]] = field(default_factory=list)
    配对收益列表: List[Dict[str, float]] = field(default_factory=list)
    交易标记: List[Dict[str, object]] = field(default_factory=list)

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
        多盈 = (self.多头持仓数 * self.当前位置 - self.多头价格总和) * self.网格.初始数量
        空盈 = (self.空头价格总和 - self.空头持仓数 * self.当前位置) * self.网格.初始数量
        self.持仓浮盈 = 多盈 + 空盈
        self.持仓成本 = (self.多头价格总和 + self.空头价格总和) * self.网格.初始数量

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
                self.空头持仓数 -= 1
                self.空头价格总和 -= 空入
                利 = (空入 - 价格) * self.网格.初始数量
                费卖 = self.费用计算(空入, "SELL")
                费买 = self.费用计算(价格, "BUY")
                成本 = 费卖 + 费买
                self.配对收益 += 利 - 成本
                self.配对次数 += 1
                self.配对收益列表.append({"ts": 时间戳, "profit": 利 - 成本})
                self.交易标记.append({"ts": 时间戳, "price": 价格, "side": "BUY", "offset": "CLOSE", "qty": self.网格.初始数量, "fee": 成本, "mode": self.网格.撮合模式, "pair_profit": 利 - 成本})
            else:
                self.多头持仓价.append(价格)
                self.多头持仓数 += 1
                self.多头价格总和 += 价格
                单费 = self.费用计算(价格, "BUY")
                self.交易标记.append({"ts": 时间戳, "price": 价格, "side": "BUY", "offset": "OPEN", "qty": self.网格.初始数量, "fee": 单费, "mode": self.网格.撮合模式, "pair_profit": 0.0})
            self.持仓格数 += 1
            self.交易额 += 价格 * self.网格.初始数量
        elif 方向 == "SELL":
            if len(self.多头持仓价) > 0:
                多入 = self.多头持仓价.pop(0)
                self.多头持仓数 -= 1
                self.多头价格总和 -= 多入
                利 = (价格 - 多入) * self.网格.初始数量
                费买 = self.费用计算(多入, "BUY")
                费卖 = self.费用计算(价格, "SELL")
                成本 = 费买 + 费卖
                self.配对收益 += 利 - 成本
                self.配对次数 += 1
                self.配对收益列表.append({"ts": 时间戳, "profit": 利 - 成本})
                self.交易标记.append({"ts": 时间戳, "price": 价格, "side": "SELL", "offset": "CLOSE", "qty": self.网格.初始数量, "fee": 成本, "mode": self.网格.撮合模式, "pair_profit": 利 - 成本})
            else:
                self.空头持仓价.append(价格)
                self.空头持仓数 += 1
                self.空头价格总和 += 价格
                单费 = self.费用计算(价格, "SELL")
                self.交易标记.append({"ts": 时间戳, "price": 价格, "side": "SELL", "offset": "OPEN", "qty": self.网格.初始数量, "fee": 单费, "mode": self.网格.撮合模式, "pair_profit": 0.0})
            self.持仓格数 -= 1
            self.交易额 += 价格 * self.网格.初始数量
        self.更新持仓统计()
        self.记录极值()
        if 价格 >= self.网格.最高价 or 价格 <= self.网格.最低价:
            self.重建网格()
        else:
            self.下一档价格 = self.下一档计算(价格)
            self.上一档价格 = self.上一档计算(价格)

    def 更新价格(self, 时间戳: str, 新价: float) -> None:
        步 = self.网格.档距
        if abs(新价 - self.当前位置) < self.容差:
            self.当前位置 = 新价
            return
        if 新价 > self.当前位置:
            起触 = self.上一档价格
            if 新价 < 起触 - self.容差:
                self.当前位置 = 新价
                self.更新持仓统计()
                return
            可跨 = 新价 - 起触
            次数 = int(math.floor(可跨 / 步)) + 1
            for i in range(次数):
                触价 = 起触 + i * 步
                if 触价 > self.网格.最高价:
                    break
                self.更新订单(时间戳, 触价, "SELL")
                self.当前位置 = 触价
            if self.当前位置 < 新价:
                self.当前位置 = 新价
                self.更新持仓统计()
        else:
            起触 = self.下一档价格
            if 新价 > 起触 + self.容差:
                self.当前位置 = 新价
                self.更新持仓统计()
                return
            可跨 = 起触 - 新价
            次数 = int(math.floor(可跨 / 步)) + 1
            for i in range(次数):
                触价 = 起触 - i * 步
                if 触价 < self.网格.最低价:
                    break
                self.更新订单(时间戳, 触价, "BUY")
                self.当前位置 = 触价
            if self.当前位置 > 新价:
                self.当前位置 = 新价
                self.更新持仓统计()


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

    def 加载H5(self, 路径: str, 键: Optional[str] = None) -> None:
        try:
            import pandas as pd
        except Exception:
            raise RuntimeError("读取H5需要pandas与tables库，请安装后重试")
        if 键 is None:
            with pd.HDFStore(路径, mode="r") as store:
                keys = store.keys()
                if not keys:
                    raise ValueError("H5文件无数据键")
                键 = keys[0]
        print("加载H5数据(全集)……", flush=True)
        df = pd.read_hdf(路径, key=键)
        # 列标准化
        colmap = {
            "timestamp": "timestamp",
            "time": "timestamp",
            "candle_begin_time": "timestamp",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "vol": "volume",
        }
        标准 = {v for v in colmap.values()}
        现列 = set(df.columns)
        重命名: Dict[str, str] = {}
        for 原, 标 in colmap.items():
            if 原 in 现列:
                重命名[原] = 标
        dfr = df.rename(columns=重命名)
        缺 = [c for c in ["timestamp", "open", "high", "low", "close", "volume"] if c not in dfr.columns]
        if 缺:
            raise ValueError(f"H5数据缺少必要列: {缺}")
        # 统一timestamp为字符串
        ts = dfr["timestamp"]
        if ts.dtype.kind in ("i", "u"):
            dfr["timestamp"] = ts.astype(int).astype(str)
        else:
            dfr["timestamp"] = ts.astype(str)
        self.数据 = [
            {
                "timestamp": str(row["timestamp"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            }
            for _, row in dfr.iterrows()
        ]

    def 加载H5区间(self, 路径: str, 键: Optional[str], 起: Optional[str], 止: Optional[str]) -> None:
        try:
            import pandas as pd
        except Exception:
            raise RuntimeError("读取H5需要pandas与tables库，请安装后重试")
        if 键 is None:
            with pd.HDFStore(路径, mode="r") as store:
                keys = store.keys()
                if not keys:
                    raise ValueError("H5文件无数据键")
                键 = keys[0]
        start_ms = self._to_epoch_ms(起) if 起 else None
        end_ms = self._to_epoch_ms(止) if 止 else None
        where_clauses = []
        if start_ms is not None:
            where_clauses.append(f"timestamp >= {start_ms}")
        if end_ms is not None:
            where_clauses.append(f"timestamp <= {end_ms}")
        where_expr = None
        if where_clauses:
            where_expr = " and ".join(where_clauses)
        print("尝试按时间范围选择H5数据……", flush=True)
        try:
            df = pd.read_hdf(路径, key=键, where=where_expr, start=None, stop=None)
        except Exception:
            # 可能列名不是timestamp，尝试candle_begin_time或time
            try_cols = ["candle_begin_time", "time"]
            df = None
            for col in try_cols:
                clauses = []
                if start_ms is not None:
                    clauses.append(f"{col} >= {start_ms}")
                if end_ms is not None:
                    clauses.append(f"{col} <= {end_ms}")
                expr = " and ".join(clauses) if clauses else None
                try:
                    df = pd.read_hdf(路径, key=键, where=expr)
                    break
                except Exception:
                    df = None
            if df is None:
                # 尝试只读取末尾近似范围的行，再按时间裁剪
                try:
                    with pd.HDFStore(路径, mode="r") as store:
                        storer = store.get_storer(键)
                        nrows = getattr(storer, "nrows", None)
                        if nrows is None:
                            raise RuntimeError("无法获取行数，回退到全集加载")
                        # 估算需要的行数（7天≈10080），放大系数1.5避免边界误差
                        估算 = int(10080 * 1.5) if start_ms and end_ms else int(50000)
                        start_row = max(nrows - 估算, 0)
                        print(f"按行区间读取: rows [{start_row}:{nrows}]", flush=True)
                        df = store.select(键, start=start_row, stop=nrows)
                except Exception:
                    print("按范围选择失败，回退到全集加载后裁剪", flush=True)
                    self.加载H5(路径, 键)
                    self.裁剪时间范围(起, 止)
                    return
        # 标准化列
        colmap = {
            "timestamp": "timestamp",
            "time": "timestamp",
            "candle_begin_time": "timestamp",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "vol": "volume",
        }
        重命名 = {k: v for k, v in colmap.items() if k in df.columns}
        dfr = df.rename(columns=重命名)
        缺 = [c for c in ["timestamp", "open", "high", "low", "close", "volume"] if c not in dfr.columns]
        if 缺:
            raise ValueError(f"H5数据缺少必要列: {缺}")
        ts = dfr["timestamp"]
        if ts.dtype.kind in ("i", "u"):
            dfr["timestamp"] = ts.astype(int).astype(str)
        else:
            dfr["timestamp"] = ts.astype(str)
        self.数据 = [
            {
                "timestamp": str(row["timestamp"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            }
            for _, row in dfr.iterrows()
        ]

    def _to_epoch_ms(self, x: str) -> int:
        try:
            if x.isdigit():
                return int(x)
            dt = datetime.fromisoformat(x)
            return int(dt.timestamp() * 1000)
        except Exception:
            return 0

    def 裁剪时间范围(self, 起: Optional[str], 止: Optional[str]) -> None:
        if not self.数据:
            return
        起ms = self._to_epoch_ms(起) if 起 else None
        止ms = self._to_epoch_ms(止) if 止 else None
        def 选(d: Dict[str, str]) -> bool:
            t = self._to_epoch_ms(str(d["timestamp"]))
            if 起ms is not None and t < 起ms:
                return False
            if 止ms is not None and t > 止ms:
                return False
            return True
        self.数据 = [d for d in self.数据 if 选(d)]

    def 运行(self) -> Dict[str, float]:
        if not self.数据:
            raise ValueError("无数据")
        首开 = float(self.数据[0]["open"])
        self.账户.初始化(首开)
        总数 = len(self.数据)
        进度条 = None
        t0 = time.perf_counter()
        计数 = 0
        try:
            from tqdm import tqdm
            进度条 = tqdm(self.数据, total=总数, desc="回测进行中")
            迭代器 = 进度条
        except Exception:
            迭代器 = self.数据
        for 行 in 迭代器:
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
            当前净值 = self.参数.初始资金 + self.账户.配对收益 + self.账户.持仓浮盈
            self.账户.净值序列.append({"ts": ts, "nav": 当前净值})
            计数 += 1
            if 计数 % 200 == 0:
                t1 = time.perf_counter()
                每条耗时 = (t1 - t0) / max(计数, 1)
                剩余 = 总数 - 计数
                预计秒 = 每条耗时 * 剩余
                百分比 = 计数 / 总数 * 100 if 总数 else 0.0
                try:
                    if 进度条 is not None:
                        进度条.set_postfix({"ETA(s)": f"{预计秒:.0f}"})
                    print(f"进度 {计数}/{总数} ({百分比:.2f}%) 预计剩余 {预计秒:.0f}s", flush=True)
                except Exception:
                    pass
        总盈 = self.账户.持仓浮盈 + self.账户.配对收益
        return {
            "pairing_count": float(self.账户.配对次数),
            "pair_profit": self.账户.配对收益,
            "positions_pnl": self.账户.持仓浮盈,
            "total_pnl": 总盈,
            "max_profit": self.账户.最大盈利,
            "max_loss": self.账户.最大亏损,
            "turnover": self.账户.交易额 / max(self.参数.初始资金, 1e-9),
            "nav_series": self.账户.净值序列,
            "pair_profits": self.账户.配对收益列表,
            "loaded_rows": len(self.数据),
            "trade_marks": self.账户.交易标记,
        }

    def 导出已加载数据为DF(self):
        try:
            import pandas as pd
        except Exception:
            return None
        if not self.数据:
            return None
        return pd.DataFrame(self.数据)