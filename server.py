import json
import os
from urllib.parse import urlparse, parse_qs
from http.server import HTTPServer, SimpleHTTPRequestHandler

根 = os.path.dirname(__file__)
前端目录 = os.path.join(根, "web")

class 接口处理(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        p = urlparse(path).path
        if p.startswith("/api/"):
            return os.path.join(根, "null")
        if p == "/":
            return os.path.join(前端目录, "index.html")
        静态 = os.path.join(前端目录, p.lstrip("/"))
        if os.path.exists(静态):
            return 静态
        备选 = os.path.join(根, p.lstrip("/"))
        return 备选

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/api/list":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            结果 = []
            for 目录 in [根, os.path.join(根, "data", "cache")]:
                if not os.path.exists(目录):
                    continue
                for 名 in os.listdir(目录):
                    if 名.endswith(".json"):
                        路 = os.path.join(目录, 名)
                        try:
                            信息 = os.stat(路)
                            结果.append({"name": 名, "path": 路.replace(根+os.sep, ""), "mtime": int(信息.st_mtime)})
                        except Exception:
                            pass
            结果.sort(key=lambda x: x["mtime"], reverse=True)
            self.wfile.write(json.dumps({"files": 结果}, ensure_ascii=False).encode("utf-8"))
            return
        if u.path == "/api/result":
            qs = parse_qs(u.query)
            名 = qs.get("name", [None])[0]
            if not 名:
                self.send_response(400)
                self.end_headers()
                return
            候选 = [os.path.join(根, 名), os.path.join(根, "data", "cache", 名)]
            路 = None
            for c in 候选:
                if os.path.exists(c):
                    路 = c
                    break
            if not 路:
                self.send_response(404)
                self.end_headers()
                return
            try:
                with open(路, "r", encoding="utf-8") as f:
                    内容 = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(内容.encode("utf-8"))
            except Exception:
                self.send_response(500)
                self.end_headers()
            return
        if u.path == "/api/ohlc":
            qs = parse_qs(u.query)
            名 = qs.get("name", [None])[0]
            tf = qs.get("tf", ["1m"])[0]
            if not 名:
                self.send_response(400)
                self.end_headers()
                return
            结果路 = None
            for c in [os.path.join(根, 名), os.path.join(根, "data", "cache", 名)]:
                if os.path.exists(c):
                    结果路 = c
                    break
            if not 结果路:
                self.send_response(404)
                self.end_headers()
                return
            with open(结果路, "r", encoding="utf-8") as f:
                j = json.load(f)
            缓存 = j.get("cache")
            if not 缓存 or not os.path.exists(缓存):
                self.send_response(404)
                self.end_headers()
                return
            df = None
            try:
                if 缓存.endswith(".parquet"):
                    df = pd.read_parquet(缓存)
                else:
                    df = pd.read_csv(缓存)
            except Exception:
                self.send_response(500)
                self.end_headers()
                return
            tcol = None
            for c in ["timestamp", "time", "candle_begin_time"]:
                if c in df.columns:
                    tcol = c
                    break
            if tcol is None:
                self.send_response(500)
                self.end_headers()
                return
            df[tcol] = pd.to_datetime(df[tcol], unit="ms", errors="ignore")
            if not pd.api.types.is_datetime64_any_dtype(df[tcol]):
                df[tcol] = pd.to_datetime(df[tcol])
            df = df.rename(columns={tcol: "time"})
            need = {"time", "open", "high", "low", "close"}
            if not need.issubset(set(df.columns)):
                self.send_response(500)
                self.end_headers()
                return
            x = df.set_index("time")
            o = x["open"].resample(tf).first()
            h = x["high"].resample(tf).max()
            l = x["low"].resample(tf).min()
            c = x["close"].resample(tf).last()
            v = x["volume"].resample(tf).sum() if "volume" in x.columns else None
            out = pd.DataFrame({"time": o.index, "open": o.values, "high": h.values, "low": l.values, "close": c.values})
            if v is not None:
                out["volume"] = v.values
            out = out.dropna()
            js = [{"time": int(pd.Timestamp(t).timestamp()), "open": float(a), "high": float(b), "low": float(d), "close": float(e), "volume": float(out.loc[i, "volume"]) if "volume" in out.columns else 0.0} for i, (t, a, b, d, e) in enumerate(zip(out["time"], out["open"], out["high"], out["low"], out["close"]))]
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"bars": js}, ensure_ascii=False).encode("utf-8"))
            return
        if u.path == "/api/trades":
            qs = parse_qs(u.query)
            名 = qs.get("name", [None])[0]
            if not 名:
                self.send_response(400)
                self.end_headers()
                return
            路 = None
            for c in [os.path.join(根, 名), os.path.join(根, "data", "cache", 名)]:
                if os.path.exists(c):
                    路 = c
                    break
            if not 路:
                self.send_response(404)
                self.end_headers()
                return
            with open(路, "r", encoding="utf-8") as f:
                j = json.load(f)
            tsf = lambda x: int(pd.Timestamp(int(x), unit="ms").timestamp()) if isinstance(x, (int, float)) else int(pd.Timestamp(x).timestamp())
            out = []
            for t in j.get("trade_marks", []):
                out.append({
                    "time": tsf(t.get("ts")),
                    "position": "belowBar" if t.get("side") == "BUY" else "aboveBar",
                    "shape": "arrowUp" if t.get("side") == "BUY" else "arrowDown",
                    "color": "#2ecc71" if t.get("side") == "BUY" else "#e74c3c",
                    "text": f"{t.get('offset')} {t.get('qty')}@{round(float(t.get('price', 0.0)),4)}\nfee:{round(float(t.get('fee',0.0)),4)} {t.get('mode','')}\npair:{round(float(t.get('pair_profit',0.0)),4)}"
                })
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"markers": out}, ensure_ascii=False).encode("utf-8"))
            return
        if u.path == "/api/nav":
            qs = parse_qs(u.query)
            名们 = qs.get("names", [""])[0]
            名列 = [x.strip() for x in 名们.split(",") if x.strip()]
            out = []
            for 名 in 名列:
                路 = None
                for c in [os.path.join(根, 名), os.path.join(根, "data", "cache", 名)]:
                    if os.path.exists(c):
                        路 = c
                        break
                if not 路:
                    continue
                with open(路, "r", encoding="utf-8") as f:
                    j = json.load(f)
                series = []
                for s in j.get("nav_series", []):
                    ts = s.get("ts")
                    tsec = int(pd.Timestamp(int(ts), unit="ms").timestamp()) if isinstance(ts, (int, float)) else int(pd.Timestamp(ts).timestamp())
                    series.append({"time": tsec, "value": float(s.get("nav", 0.0))})
                out.append({"name": 名, "series": series})
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"navs": out}, ensure_ascii=False).encode("utf-8"))
            return
        return super().do_GET()

def 启动(端口=8000):
    os.chdir(根)
    httpd = HTTPServer(("0.0.0.0", 端口), 接口处理)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    启动()
import pandas as pd
import json