"""SQLite: lược đồ và hàm đọc/ghi. Thời gian lưu theo múi giờ Việt Nam (ISO 8601, +07:00)."""
import json
import sqlite3
from datetime import datetime
import time
from pathlib import Path

import pandas as pd

SOURCE = "ssi_v3/securitiesSummary"
VALUE_COLS = ["open", "high", "low", "close", "average", "price_change", "price_change_pct",
              "match_vol", "match_val", "deal_vol", "deal_val",
              "foreign_buy_vol", "foreign_buy_val", "foreign_sell_vol", "foreign_sell_val",
              "foreign_room_remain", "foreign_room_total"]


def _series_table(table: str, key: str) -> str:
    cols = ",\n  ".join(f"{c} REAL" for c in VALUE_COLS)
    return f"""CREATE TABLE IF NOT EXISTS {table}(
  {key} TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  {cols},
  source TEXT, adjusted_flag INTEGER, fetch_time TEXT NOT NULL,
  data_status TEXT NOT NULL, flags TEXT,
  PRIMARY KEY ({key}, trade_date))"""


SCHEMA = [
    _series_table("prices_daily", "symbol"),
    _series_table("index_daily", "index_code"),
    """CREATE TABLE IF NOT EXISTS symbols(
  symbol TEXT PRIMARY KEY, board TEXT, stock_type TEXT, name_vi TEXT,
  icb_code TEXT, icb_name TEXT, listed_share REAL, fetch_time TEXT)""",
    """CREATE TABLE IF NOT EXISTS fetch_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, fetch_time TEXT, endpoint TEXT, params TEXT,
  http INTEGER, seconds REAL, rows INTEGER, items_count INTEGER, error TEXT)""",
    """CREATE TABLE IF NOT EXISTS stream_raw(
  id INTEGER PRIMARY KEY AUTOINCREMENT, recv_time TEXT NOT NULL,
  msg_type TEXT NOT NULL, symbol TEXT, payload TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS index_summary_daily(
  index_code TEXT NOT NULL, trade_date TEXT NOT NULL, snapshot_time TEXT,
  index_value REAL, advances INTEGER, declines INTEGER, no_change INTEGER, ceilings INTEGER, floors INTEGER,
  total_match_value REAL, net_foreign_value REAL, raw TEXT, PRIMARY KEY(index_code, trade_date))""",
    "CREATE INDEX IF NOT EXISTS ix_stream_raw_time ON stream_raw(recv_time)",
    "CREATE INDEX IF NOT EXISTS ix_stream_raw_sym ON stream_raw(symbol, recv_time)",
]


def connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _migrate_stream_raw(con: sqlite3.Connection) -> None:
    """Bảng stream_raw kiểu cũ (thiếu msg_type/symbol) được đổi tên để giữ lại, không xóa."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(stream_raw)")}
    if cols and not {"recv_time", "msg_type", "symbol", "payload"} <= cols:
        con.execute(f"ALTER TABLE stream_raw RENAME TO stream_raw_legacy_{int(time.time())}")
        con.commit()


def init_db(con: sqlite3.Connection) -> None:
    _migrate_stream_raw(con)
    with con:
        for sql in SCHEMA:
            con.execute(sql)


def _replace(con, table: str, key_col: str, key: str, df: pd.DataFrame, fetch_time: str) -> int:
    """Thay toàn bộ lịch sử của một mã. Phải thay hết (không chỉ thêm ngày mới) vì giá đã điều chỉnh
    ngược: khi có sự kiện doanh nghiệp, giá của mọi ngày cũ đều đổi."""
    d = df.copy()
    d["trade_date"] = d["trade_date"].dt.strftime("%Y-%m-%d")
    cols = ["trade_date"] + VALUE_COLS
    sub = d[cols].astype(object).where(d[cols].notna(), None)
    rows = [(key,) + tuple(rec) + (SOURCE, 1, fetch_time, st, fl)
            for rec, st, fl in zip(sub.itertuples(index=False, name=None),
                                   d["data_status"], d["flags"])]
    ph = ",".join("?" * (1 + len(cols) + 5))
    with con:
        con.execute(f"DELETE FROM {table} WHERE {key_col} = ?", (key,))
        con.executemany(
            f"INSERT INTO {table} ({key_col}, {', '.join(cols)}, source, adjusted_flag, "
            f"fetch_time, data_status, flags) VALUES ({ph})", rows)
    return len(rows)


def replace_prices(con, symbol: str, df: pd.DataFrame, fetch_time: str) -> int:
    return _replace(con, "prices_daily", "symbol", symbol, df, fetch_time)


def replace_index(con, index_code: str, df: pd.DataFrame, fetch_time: str) -> int:
    return _replace(con, "index_daily", "index_code", index_code, df, fetch_time)


def upsert_symbols(con, df: pd.DataFrame, fetch_time: str) -> None:
    rows = []
    for r in df.to_dict("records"):
        try:
            share = float(r.get("listedShare")) if r.get("listedShare") not in (None, "") else None
        except (TypeError, ValueError):
            share = None
        rows.append((r.get("symbol"), r.get("board"), r.get("stockType"), r.get("symbolNameVi"),
                     r.get("icbCode"), r.get("icbName"), share, fetch_time))
    with con:
        con.executemany("INSERT OR REPLACE INTO symbols VALUES (?,?,?,?,?,?,?,?)", rows)


def log_calls(con, entries: list[dict]) -> None:
    if not entries:
        return
    with con:
        con.executemany(
            "INSERT INTO fetch_log (fetch_time, endpoint, params, http, seconds, rows, items_count, error) "
            "VALUES (:fetch_time, :endpoint, :params, :http, :seconds, :rows, :items_count, :error)", entries)


def last_fetch_time(con, table: str, key_col: str, key: str) -> str | None:
    row = con.execute(f"SELECT MAX(fetch_time) FROM {table} WHERE {key_col} = ?", (key,)).fetchone()
    return row[0] if row else None


def load_prices(con, symbols: list[str] | None = None, only_ok: bool = False) -> pd.DataFrame:
    sql = "SELECT * FROM prices_daily"
    cond, args = [], []
    if symbols:
        cond.append(f"symbol IN ({','.join('?' * len(symbols))})")
        args += symbols
    if only_ok:
        cond.append("data_status IN ('ok','no_trade')")
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    df = pd.read_sql_query(sql + " ORDER BY symbol, trade_date", con, params=args)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _num(x):
    try:
        v = float(x)
        return None if v != v else v
    except (TypeError, ValueError):
        return None


def _parse_date(x) -> str | None:
    for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(x).strip()[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def upsert_index_summary(con, index_code: str, row: dict, snapshot_time: str) -> bool:
    """Lưu ảnh chụp indexSummary của phiên gần nhất (khóa: chỉ số + ngày). Trả False nếu thiếu ngày giao dịch."""
    d = _parse_date(row.get("tradingDate"))
    if not d:
        return False
    toint = lambda v: None if _num(v) is None else int(_num(v))  # noqa: E731
    with con:
        con.execute(
            "INSERT OR REPLACE INTO index_summary_daily(index_code, trade_date, snapshot_time, index_value, advances,"
            " declines, no_change, ceilings, floors, total_match_value, net_foreign_value, raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (index_code, d, snapshot_time, _num(row.get("indexValue")), toint(row.get("totalAdvanceStock")),
             toint(row.get("totalDeclineStock")), toint(row.get("totalNoChangeStock")),
             toint(row.get("totalCeilingStock")), toint(row.get("totalFloorStock")),
             _num(row.get("totalMatchValue")), _num(row.get("netPurchasesForeignValue")),
             json.dumps(row, ensure_ascii=False, default=str)))
    return True
