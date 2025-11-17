import argparse
import json
from core.backtest.exchange.backtest_exchange_binance_1m import 币安一分回测交易所, 网格参数


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
    参数 = 网格参数(
        最低价=a.min_price,
        最高价=a.max_price,
        档距=a.grid_step,
        初始数量=a.base_qty,
        费用_maker_bps=a.maker_fee_bps,
        费用_taker_bps=a.taker_fee_bps,
        撮合模式=a.match_mode,
    )
    交易所 = 币安一分回测交易所(参数)
    交易所.加载CSV(a.csv)
    结果 = 交易所.运行()
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(结果, f, ensure_ascii=False, indent=2)
    print(json.dumps(结果, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    主()