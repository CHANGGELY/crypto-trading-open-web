from __future__ import annotations
import os
import math
from typing import Optional


def _to_epoch_ms_any(x):
    import pandas as pd
    import numpy as np
    if np.issubdtype(x.dtype, np.integer):
        return x.astype(int)
    try:
        dt = pd.to_datetime(x, utc=True, errors='coerce')
        return (dt.view('int64') // 1_000_000).astype('int64')
    except Exception:
        return pd.Series([0] * len(x), dtype='int64')


def convert_fixed_to_table(src_path: str, dst_path: Optional[str] = None, key: Optional[str] = None, chunk_rows: int = 500_000) -> str:
    import pandas as pd
    from tqdm import tqdm
    if dst_path is None:
        base, ext = os.path.splitext(src_path)
        dst_path = base + ".table" + ext
    # 读取原始（fixed）
    with pd.HDFStore(src_path, mode='r') as store:
        keys = store.keys()
        if not keys:
            raise ValueError('H5文件无数据键')
        if key is None:
            key = keys[0]
        df = store.get(key)
    # 标准化列
    colmap = {
        'timestamp': 'timestamp',
        'time': 'timestamp',
        'candle_begin_time': 'timestamp',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume',
        'vol': 'volume',
    }
    rename = {k: v for k, v in colmap.items() if k in df.columns}
    df = df.rename(columns=rename)
    need = ['timestamp', 'open', 'high', 'low', 'close']
    for c in need:
        if c not in df.columns:
            raise ValueError(f'缺少必要列: {c}')
    if 'volume' not in df.columns:
        df['volume'] = 0.0
    # 统一毫秒UTC
    df['timestamp'] = _to_epoch_ms_any(df['timestamp'])
    n = len(df)
    # 分块写入table
    if os.path.exists(dst_path):
        os.remove(dst_path)
    prog = tqdm(total=n, desc='转换为table写入')
    start = 0
    while start < n:
        end = min(start + chunk_rows, n)
        chunk = df.iloc[start:end]
        chunk.to_hdf(dst_path, key=key, mode='a', format='table', append=True, data_columns=['timestamp'])
        prog.update(len(chunk))
        # 简单ETA由tqdm显示
        start = end
    prog.close()
    return dst_path


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--src', required=True)
    p.add_argument('--dst')
    p.add_argument('--key')
    p.add_argument('--chunk_rows', type=int, default=500_000)
    a = p.parse_args()
    path = convert_fixed_to_table(a.src, a.dst, a.key, a.chunk_rows)
    print(f'转换完成: {path}')