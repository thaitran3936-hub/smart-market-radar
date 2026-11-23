"""Chẩn đoán các dòng bị gắn cờ trong data/market.db. Chỉ đọc, không sửa dữ liệu.

Chạy:  python scripts/diagnose_quality.py
"""
import os
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src import config  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
con = sqlite3.connect(config.DB_PATH)


def q(sql, params=()):
    return pd.read_sql_query(sql, con, params=params)


def with_ret(table, key):
    df = q(f"SELECT {key} AS k, trade_date, close, high, low, price_change_pct, match_vol, match_val, "
           f"data_status, flags FROM {table} ORDER BY {key}, trade_date")
    df["prev_close"] = df.groupby("k")["close"].shift(1)
    df["ret_close"] = (df["close"] / df["prev_close"] - 1) * 100
    df["diff"] = (df["ret_close"] - df["price_change_pct"]).abs()
    return df


print("=== CHỈ SỐ (index_daily) ===")
ix = with_ret("index_daily", "index_code")
print(ix.groupby("k")["data_status"].value_counts().unstack(fill_value=0))
for fl in ("ret_mismatch", "bad_price", "vol_val_conflict", "zero_volume"):
    print(f"\n-- {fl}: {int(ix['flags'].str.contains(fl).sum())} dòng")
m = ix[ix["flags"].str.contains("ret_mismatch")]
print("\n5 dòng ret_mismatch đầu tiên của chỉ số (so ret_close với price_change_pct):")
print(m[["k", "trade_date", "close", "prev_close", "ret_close", "price_change_pct", "diff"]].head(5).to_string(index=False))
print("\nPhân bố |diff| của chỉ số (phân vị):", ix["diff"].describe(percentiles=[.5, .9, .99]).round(3).to_dict())

print("\n=== CỔ PHIẾU (prices_daily) ===")
px = with_ret("prices_daily", "symbol")
for fl in ("ret_mismatch", "bad_price", "vwap_outlier", "vol_val_conflict"):
    sub = px[px["flags"].str.contains(fl)]
    print(f"\n-- {fl}: {len(sub)} dòng, {sub['k'].nunique()} mã. Top mã: "
          f"{sub['k'].value_counts().head(6).to_dict()}")
    if len(sub):
        print(sub[["k", "trade_date", "close", "high", "low", "prev_close", "ret_close",
                   "price_change_pct", "match_vol"]].head(4).to_string(index=False))

mm = px[px["flags"].str.contains("ret_mismatch")].dropna(subset=["diff"])
print("\nret_mismatch cổ phiếu: phân bố |diff| (điểm %):",
      pd.cut(mm["diff"], [1, 2, 5, 10, 20, 1e9]).value_counts().sort_index().to_dict())
print("Số dòng ret_mismatch chỉ do ngày ngay SAU ngày đầu của mã (prev_close thiếu):",
      int(px[px["flags"].str.contains("ret_mismatch")]["prev_close"].isna().sum()))
only_mm = px[(px["data_status"] == "suspect")]
only = only_mm[only_mm["flags"].apply(lambda s: {f for f in s.split("|") if f in
               ("bad_price", "vol_val_conflict", "vwap_outlier", "ret_mismatch")} == {"ret_mismatch"})]
print(f"\nDòng suspect CHỈ vì ret_mismatch: {len(only)} / {len(only_mm)}")
print("\nPhiên có >= 30 mã cùng ret_mismatch (nghi lỗi theo ngày của nguồn dữ liệu):")
print(mm.groupby("trade_date").size().loc[lambda s: s >= 30].sort_values(ascending=False).head(8).to_string())
