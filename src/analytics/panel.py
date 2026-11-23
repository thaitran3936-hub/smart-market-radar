"""Đọc dữ liệu từ SQLite thành các bảng rộng (ngày x mã) dùng cho phân tích.

Quy tắc dùng dữ liệu theo cờ chất lượng (xem src/data/cleaning.py):
- close: đặt NaN ở dòng thiếu giá hoặc giá không hợp lệ (bad_price), giữ nguyên các dòng khác.
- giá trị khớp (value): chỉ dùng dòng ok/no_trade; dòng suspect bị loại (NaN). Phiên không giao dịch = 0.
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class Panel:
    close: pd.DataFrame
    value: pd.DataFrame
    board: pd.Series          # mã -> sàn
    index_close: pd.Series    # VN-Index
    index_value: pd.Series


def _wide(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df.pivot(index="trade_date", columns="symbol", values=col).sort_index()


def load_panel(con, bench: str = "VNINDEX", start: str | None = None, min_peak_value: float | None = None) -> Panel:
    """Nạp bảng giá. Nếu có `min_peak_value`, chỉ nạp các mã CÓ THỂ đủ điều kiện ở ngưỡng đó: để trung vị 60 phiên
    (tối thiểu 48 quan sát) ≥ ngưỡng thì phải có ≥ 24 phiên có giá trị khớp ≥ ngưỡng. Đây là điều kiện cần, nên kết quả
    của các mã đủ điều kiện không đổi, còn bộ nhớ giảm nhiều (mã kém thanh khoản chiếm phần lớn số mã)."""
    conds = []
    if start:
        conds.append(f"p.trade_date >= '{start}'")
    if min_peak_value:
        conds.append("p.symbol IN (SELECT symbol FROM prices_daily WHERE data_status IN ('ok','no_trade') "
                     f"AND match_val >= {float(min_peak_value)!r} GROUP BY symbol HAVING COUNT(*) >= 24)")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    df = pd.read_sql_query(
        "SELECT p.symbol, p.trade_date, p.close, p.match_val, p.data_status, p.flags, s.board "
        f"FROM prices_daily p LEFT JOIN symbols s ON s.symbol = p.symbol {where}", con)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    bad_close = df["data_status"].eq("missing") | df["flags"].fillna("").str.contains("bad_price")
    df["close"] = df["close"].where(~bad_close & (df["close"] > 0))
    df["match_val"] = df["match_val"].where(df["data_status"].isin(["ok", "no_trade"]))
    board = df.groupby("symbol")["board"].first()
    ix = pd.read_sql_query(
        "SELECT trade_date, close, match_val FROM index_daily WHERE index_code = ? ORDER BY trade_date",
        con, params=[bench])
    ix["trade_date"] = pd.to_datetime(ix["trade_date"])
    ix = ix.set_index("trade_date")
    return Panel(close=_wide(df, "close"), value=_wide(df, "match_val"), board=board,
                 index_close=ix["close"].where(ix["close"] > 0),
                 index_value=ix["match_val"])


def eligible_mask(value: pd.DataFrame, min_median_value: float = 1e9, window: int = 60,
                  min_traded_share: float = 0.8) -> pd.DataFrame:
    """Mã đủ điều kiện được xếp hạng tại từng ngày (chỉ dùng dữ liệu tới ngày đó):
    (1) trung vị giá trị khớp `window` phiên ≥ ngưỡng (đồng), và
    (2) có giao dịch (giá trị > 0) trong ≥ `min_traded_share` số phiên của cửa sổ.
    Điều kiện (2) loại mã ít giao dịch có giá "đứng yên", nếu không sẽ trông như biến động thấp."""
    min_p = int(window * 0.8)
    med = value.rolling(window, min_periods=min_p).median()
    traded = (value > 0).where(value.notna()).rolling(window, min_periods=min_p).mean()
    return (med >= min_median_value) & (traded >= min_traded_share)
