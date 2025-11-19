import argparse
import json
import importlib.util
import os
import sys

_EX_PATH = os.path.join(os.path.dirname(__file__), "core", "backtest", "exchange", "backtest_exchange_binance_1m.py")
_MT_PATH = os.path.join(os.path.dirname(__file__), "core", "backtest", "metrics", "绩效指标.py")

def _load_module(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)  # type: ignore
    return m

_ex = _load_module(_EX_PATH, "backtest_exchange_binance_1m")
_mt = _load_module(_MT_PATH, "metrics_perf")
币安一分回测交易所 = _ex.币安一分回测交易所
网格参数 = _ex.网格参数
计算净值与收益 = _mt.计算净值与收益
计算最大回撤 = _mt.计算最大回撤
计算Sharpe = _mt.计算Sharpe
计算胜率 = _mt.计算胜率
计算循环效率 = _mt.计算循环效率
生成可视化 = _mt.生成可视化


def 解析配置() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--csv")
    p.add_argument("--min_price", type=float)
    p.add_argument("--max_price", type=float)
    p.add_argument("--grid_step", type=float)
    p.add_argument("--base_qty", type=float)
    p.add_argument("--maker_fee_bps", type=float, default=0.0)
    p.add_argument("--taker_fee_bps", type=float, default=0.0)
    p.add_argument("--match_mode", choices=["maker", "taker"], default="maker")
    p.add_argument("--out", default="results.json")
    p.add_argument("-c", "--config")
    p.add_argument("--initial_capital", type=float, default=10000.0)
    p.add_argument("--plot_out", default="results.png")
    p.add_argument("--h5")
    p.add_argument("--h5_key")
    p.add_argument("--start")
    p.add_argument("--end")
    return p.parse_args()


def 主():
    a = 解析配置()
    if a.config:
        cfg = {}
        with open(a.config, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                if ":" in s:
                    k, v = s.split(":", 1)
                    k = k.strip()
                    v = v.strip()
                    cfg[k] = v
        a.min_price = float(cfg.get("min_price"))
        a.max_price = float(cfg.get("max_price"))
        a.grid_step = float(cfg.get("grid_step"))
        a.base_qty = float(cfg.get("base_qty"))
        a.maker_fee_bps = float(cfg.get("maker_fee_bps", a.maker_fee_bps))
        a.taker_fee_bps = float(cfg.get("taker_fee_bps", a.taker_fee_bps))
        a.match_mode = cfg.get("match_mode", a.match_mode)
        a.csv = cfg.get("csv", a.csv)
        a.out = cfg.get("out", a.out)
        if "initial_capital" in cfg:
            a.initial_capital = float(cfg.get("initial_capital"))
        if "plot_out" in cfg:
            a.plot_out = cfg.get("plot_out")
    参数 = 网格参数(
        最低价=a.min_price,
        最高价=a.max_price,
        档距=a.grid_step,
        初始数量=a.base_qty,
        初始资金=a.initial_capital,
        费用_maker_bps=a.maker_fee_bps,
        费用_taker_bps=a.taker_fee_bps,
        撮合模式=a.match_mode,
    )
    交易所 = 币安一分回测交易所(参数)
    if a.h5:
        # 自动检测并转换为table
        base, ext = os.path.splitext(a.h5)
        table_path = base + ".table" + ext
        if not os.path.exists(table_path):
            print("检测到fixed格式，开始一次性转换为table……")
            conv = _load_module(os.path.join(os.path.dirname(__file__), "core", "backtest", "tools", "h5_table_converter.py"), "h5_table_converter")
            table_path = conv.convert_fixed_to_table(a.h5, table_path, a.h5_key, 500_000)
        # 优先按窗口从table读取
        if a.start or a.end:
            交易所.加载H5区间(table_path, a.h5_key, a.start, a.end)
        else:
            交易所.加载H5(table_path, a.h5_key)
    else:
        交易所.加载CSV(a.csv)
        交易所.裁剪时间范围(a.start, a.end)
    结果 = 交易所.运行()
    返回率, 时间戳 = 计算净值与收益(结果.get("nav_series", []))
    指标_dd = 计算最大回撤(结果.get("nav_series", []))
    指标_sharpe = 计算Sharpe(返回率)
    指标_win = 计算胜率(结果.get("pair_profits", []))
    指标_eff = 计算循环效率(结果.get("pair_profits", []), a.grid_step, a.base_qty)
    结果["sharpe"] = 指标_sharpe
    结果.update(指标_dd)
    结果["win_rate"] = 指标_win
    结果.update(指标_eff)
    可视化路径 = 生成可视化(结果.get("nav_series", []), 结果.get("pair_profits", []), a.plot_out)
    if 可视化路径:
        结果["plot"] = 可视化路径
    # 窗口缓存（Parquet优先，其次CSV）
    缓存名 = None
    if a.h5 and (a.start or a.end):
        base = os.path.splitext(os.path.basename(a.h5))[0]
        win = f"{a.start or 'NA'}_{a.end or 'NA'}"
        缓存名 = os.path.join(os.path.dirname(__file__), "data", "cache", f"{base}_{win}.parquet")
        os.makedirs(os.path.dirname(缓存名), exist_ok=True)
        df_loaded = 交易所.导出已加载数据为DF()
        if df_loaded is not None:
            try:
                df_loaded.to_parquet(缓存名, index=False)
                结果["cache"] = 缓存名
            except Exception:
                缓存名_csv = 缓存名.replace(".parquet", ".csv")
                df_loaded.to_csv(缓存名_csv, index=False)
                结果["cache"] = 缓存名_csv
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(结果, f, ensure_ascii=False, indent=2)
    print(json.dumps(结果, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    主()