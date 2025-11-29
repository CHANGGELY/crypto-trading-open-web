# -*- coding: utf-8 -*-
from __future__ import annotations
import math
from typing import List, Dict, Tuple, Optional


def 计算净值与收益(nav_series: List[Dict[str, float]]) -> Tuple[List[float], List[str]]:
    r = []
    t = []
    for i in range(1, len(nav_series)):
        p0 = nav_series[i - 1]["nav"]
        p1 = nav_series[i]["nav"]
        if p0 <= 0:
            r.append(0.0)
        else:
            r.append(p1 / p0 - 1.0)
        t.append(str(nav_series[i]["ts"]))
    return r, t


def 计算最大回撤(nav_series: List[Dict[str, float]]) -> Dict[str, float]:
    m = -1e18
    dd = 0.0
    for x in nav_series:
        v = x["nav"]
        if v > m:
            m = v
        if m > 0:
            d = (m - v) / m
            if d > dd:
                dd = d
    return {"max_drawdown": dd}


def 计算Sharpe(returns: List[float], annual_factor: float = 365.0 * 24.0 * 60.0, rf: float = 0.0) -> float:
    if not returns:
        return 0.0
    mu = sum(returns) / len(returns)
    var = sum((x - mu) * (x - mu) for x in returns) / len(returns)
    sd = math.sqrt(var)
    if sd <= 1e-12:
        return 0.0
    return (mu - rf) / sd * math.sqrt(annual_factor)


def 计算胜率(pair_profits: List[Dict[str, float]]) -> float:
    if not pair_profits:
        return 0.0
    w = sum(1 for x in pair_profits if x["profit"] > 0)
    return w / len(pair_profits)


def 计算循环效率(pair_profits: List[Dict[str, float]], grid_step: float, base_qty: float) -> Dict[str, float]:
    if not pair_profits or grid_step <= 0 or base_qty <= 0:
        return {"avg_cycle_profit": 0.0, "cycle_efficiency": 0.0}
    avgp = sum(x["profit"] for x in pair_profits) / len(pair_profits)
    theo = grid_step * base_qty
    eff = avgp / theo if theo > 0 else 0.0
    return {"avg_cycle_profit": avgp, "cycle_efficiency": eff}


def 生成可视化(nav_series: List[Dict[str, float]], pair_profits: List[Dict[str, float]], out_path: str) -> Optional[str]:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return None
    x = [s["ts"] for s in nav_series]
    y = [s["nav"] for s in nav_series]
    dd = []
    m = -1e18
    for v in y:
        if v > m:
            m = v
        if m > 0:
            dd.append((m - v) / m)
        else:
            dd.append(0.0)
    fig = plt.figure(figsize=(10, 6))
    ax1 = fig.add_subplot(2, 1, 1)
    ax1.plot(x, y)
    ax1.set_title("净值曲线")
    ax2 = fig.add_subplot(2, 1, 2)
    ax2.plot(x, dd)
    ax2.set_title("回撤")
    fig.tight_layout()
    fig.savefig(out_path)
    return out_path