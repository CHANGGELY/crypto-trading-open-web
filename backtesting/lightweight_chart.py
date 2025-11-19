from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class Offset(Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"

@dataclass
class TradeData:
    symbol: str
    direction: Direction
    offset: Offset
    price: float
    qty: float
    time: datetime

class PriceChart:
    """
    价格图表封装
    参数：
    - history_data：包含`candle_begin_time, open, high, low, close, volume`的DataFrame
    - default_offset：可选重采样频率，如`15T/1H/4H`
    - maximize：是否最大化窗口
    """
    def __init__(self, history_data: pd.DataFrame, default_offset: str = "15T", maximize: bool = False):
        from lightweight_charts import Chart
        df = history_data.copy()
        列映射 = {
            "candle_begin_time": "time",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
        df = df.rename(columns={k: v for k, v in 列映射.items() if k in df.columns})
        if "time" not in df.columns:
            raise ValueError("缺少candle_begin_time列")
        if not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"])
        必要 = {"open", "high", "low", "close"}
        if not 必要.issubset(set(df.columns)):
            raise ValueError("缺少必要OHLC列")
        if default_offset:
            df = self._重采样(df, default_offset)
        self._df = df
        self._chart = Chart()
        self._chart.set(df)
        if maximize:
            self._chart.resize(1, 1)

    def add_indicator(self, fn, color: str = "red", column_name: str = "indicator"):
        """
        一行添加指标，fn接收DataFrame返回Series或DataFrame
        """
        s = fn(self._df)
        if isinstance(s, pd.DataFrame):
            if column_name in s.columns:
                line_df = s.rename(columns={column_name: column_name})
            else:
                c0 = s.columns[0]
                line_df = s.rename(columns={c0: column_name})
        else:
            line_df = pd.DataFrame({"time": self._df["time"], column_name: s.values})
        if "time" not in line_df.columns:
            line_df["time"] = self._df["time"]
        line_df = line_df.dropna()
        line = self._chart.create_line(column_name, color=color, style="solid", width=2, price_line=False, price_label=False)
        line.set(line_df[["time", column_name]])
        return line

    def add_trade_marks(self, trades: list[TradeData]):
        """
        批量添加交易标记
        """
        标记 = []
        for t in trades:
            位置 = "belowBar" if t.direction == Direction.LONG else "aboveBar"
            形状 = "arrowUp" if t.direction == Direction.LONG else "arrowDown"
            颜色 = "#2ecc71" if t.direction == Direction.LONG else "#e74c3c"
            文本 = f"{t.offset.value} {t.qty}@{round(t.price, 4)}"
            标记.append({"time": t.time, "position": 位置, "shape": 形状, "color": 颜色, "text": 文本})
        return self._chart.marker_list(标记)

    def add_subchart(self, sub_df: pd.DataFrame, value_col: str, color: str = "blue", name: str = "Sub"):
        """
        添加子图表面板并绘制折线
        sub_df需包含`candle_begin_time`
        """
        if "candle_begin_time" not in sub_df.columns:
            raise ValueError("子图数据需包含candle_begin_time列")
        ldf = pd.DataFrame({"time": pd.to_datetime(sub_df["candle_begin_time"]), name: sub_df[value_col]})
        面板 = self._chart.create_subchart(width=1, height=0.25, sync=True)
        线 = 面板.create_line(name, color=color, style="solid", width=2, price_line=False, price_label=False)
        线.set(ldf.dropna())
        return 面板

    def set_visible_range(self, start: datetime, end: datetime):
        self._chart.set_visible_range(start, end)

    def show(self, block: bool = True):
        self._chart.show(block=block)

    def _重采样(self, df: pd.DataFrame, rule: str) -> pd.DataFrame:
        x = df.set_index("time")
        o = x["open"].resample(rule).first()
        h = x["high"].resample(rule).max()
        l = x["low"].resample(rule).min()
        c = x["close"].resample(rule).last()
        v = (x["volume"].resample(rule).sum()) if "volume" in x.columns else None
        out = pd.DataFrame({"time": o.index, "open": o.values, "high": h.values, "low": l.values, "close": c.values})
        if v is not None:
            out["volume"] = v.values
        return out.dropna()