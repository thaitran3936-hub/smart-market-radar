"""Chất lượng và thanh khoản theo từng sàn; liệt kê lệnh gọi API lỗi. Chỉ đọc, không sửa dữ liệu.

Chạy:  python scripts/diagnose_by_board.py
"""
import os
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src import config  # noqa: E402

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)
con = sqlite3.connect(config.DB_PATH)
q = lambda sql: pd.read_sql_query(sql, con)  # noqa: E731

print("=== 1. Theo sàn: số mã, số dòng, tỷ lệ trạng thái (%) ===")
st = q("SELECT COALESCE(s.board,'?') AS board, p.data_status AS st, COUNT(*) AS n "
       "FROM prices_daily p LEFT JOIN symbols s ON s.symbol = p.symbol GROUP BY 1, 2")
t = st.pivot(index="board", columns="st", values="n").fillna(0)
t["rows"] = t.sum(axis=1)
nsym = q("SELECT COALESCE(s.board,'?') AS board, COUNT(DISTINCT p.symbol) AS symbols FROM prices_daily p "
         "LEFT JOIN symbols s ON s.symbol = p.symbol GROUP BY 1").set_index("board")
for c in ("ok", "no_trade", "suspect", "missing"):
    if c in t:
        t[c + "_%"] = (100 * t[c] / t["rows"]).round(1)
print(nsym.join(t[["rows"] + [c for c in t.columns if c.endswith("_%")]]))

print("\n=== 2. Theo sàn: tỷ lệ mỗi cờ (% số dòng) ===")
flags = ["zero_volume", "ret_mismatch_large", "vwap_outlier", "bad_price", "jump", "vol_val_conflict"]
sel = ", ".join(f"100.0*AVG(p.flags LIKE '%{f}%') AS {f}" for f in flags)
print(q(f"SELECT COALESCE(s.board,'?') AS board, {sel} FROM prices_daily p "
        "LEFT JOIN symbols s ON s.symbol = p.symbol GROUP BY 1").round(2).to_string(index=False))

print("\n=== 3. Thanh khoản: số mã có trung vị giá trị khớp 60 phiên gần nhất ≥ ngưỡng ===")
last = q("SELECT symbol, match_val FROM prices_daily WHERE data_status IN ('ok','no_trade') AND trade_date >= "
         "(SELECT date(MAX(trade_date), '-90 day') FROM prices_daily)")
med = last.groupby("symbol")["match_val"].median().rename("median_val")
b = q("SELECT symbol, MAX(board) AS board FROM symbols GROUP BY symbol").set_index("symbol")
m = b.join(med, how="right").fillna({"board": "?"})
rows = []
for th in (0, 1e8, 5e8, 1e9, 5e9, 1e10):
    rows.append({"ngưỡng (đồng/phiên)": f"{th:,.0f}", **m[m["median_val"] >= th].groupby("board").size().to_dict(),
                 "tổng": int((m["median_val"] >= th).sum())})
print(pd.DataFrame(rows).fillna(0).to_string(index=False))

print("\n=== 4. Lệnh gọi API không thành công (fetch_log) ===")
err = q("SELECT fetch_time, endpoint, params, http, error FROM fetch_log WHERE http IS NULL OR http != 200")
print(err.assign(params=err["params"].str.slice(0, 70), error=err["error"].fillna("").str.slice(0, 60)).to_string(index=False))

print("\n=== 5. Chỉ số ===")
print(q("SELECT index_code, COUNT(*) AS rows, MIN(trade_date) AS tu, MAX(trade_date) AS den, "
        "SUM(data_status='suspect') AS suspect FROM index_daily GROUP BY 1").to_string(index=False))
