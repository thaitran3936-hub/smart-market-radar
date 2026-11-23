"""Collector: tải dữ liệu SSI v3, làm sạch, lưu SQLite.

Chạy:
    python -m src.data.collector --update                 # tải/cập nhật toàn bộ (HOSE + VN-Index, VN30)
    python -m src.data.collector --update --limit 20      # thử với 20 mã
    python -m src.data.collector --update --symbols SSI,VNM
    python -m src.data.collector --update --boards HOSE,HNX,UPCOM   # toàn thị trường
    python -m src.data.collector --update --full          # ép tải lại toàn bộ lịch sử
    python -m src.data.collector --reflag                 # gắn lại cờ chất lượng, không gọi API
    python -m src.data.collector --report                 # tóm tắt dữ liệu và chất lượng

Cập nhật tăng dần: tải cửa sổ gần nhất (30 ngày) và so giá đóng cửa với dữ liệu đã lưu. Nếu giá cũ
đổi (SSI điều chỉnh ngược khi có chia cổ tức/tách cổ phiếu) thì tải lại toàn bộ lịch sử của mã đó.
Chỉ lấy phiên đã kết thúc: trước 15:00 giờ Việt Nam thì không lấy ngày hôm nay.
"""
import argparse
import logging
import sys
from datetime import date, datetime, time as dtime, timedelta

import pandas as pd

from src import config
from src.data import cleaning, db
from src.ssi_api.client import SSIClient, SSIError

log = logging.getLogger("collector")

INDEXES = ["VNINDEX", "VN30"]
ALL_BOARDS = ["HOSE", "HNX", "UPCOM"]
# Chỉ số theo sàn; mã nào SSI không có thì bị ghi vào danh sách lỗi, không làm hỏng cả đợt
BOARD_INDEXES = {"HOSE": ["VNINDEX", "VN30"], "HNX": ["HNXINDEX", "HNX30"], "UPCOM": ["UPCOMINDEX"]}
OVERLAP_DAYS = 30
ADJ_TOL = 1e-3
MARKET_CLOSE = dtime(15, 0)  # HOSE đóng cửa 14:45


def effective_end(now: datetime) -> date:
    """Ngày cuối cùng được phép lấy: hôm nay nếu đã sau 15:00, ngược lại là hôm qua."""
    return now.date() if now.time() >= MARKET_CLOSE else now.date() - timedelta(days=1)


def _load_series(con, table: str, key_col: str, key: str) -> pd.DataFrame:
    cols = ", ".join(["trade_date"] + db.VALUE_COLS)
    df = pd.read_sql_query(f"SELECT {cols} FROM {table} WHERE {key_col} = ? ORDER BY trade_date",
                           con, params=[key])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _adjusted(stored: pd.DataFrame, new: pd.DataFrame, tol: float = ADJ_TOL) -> bool:
    """True nếu giá đóng cửa của các ngày trùng nhau đã bị đổi (dấu hiệu điều chỉnh ngược)."""
    if stored.empty or new.empty:
        return False
    m = stored[["trade_date", "close"]].merge(new[["trade_date", "close"]], on="trade_date",
                                              suffixes=("_old", "_new")).dropna()
    m = m[m["close_old"] > 0]
    if m.empty:
        return False
    return bool(((m["close_new"] / m["close_old"] - 1).abs() > tol).any())


def sync_series(con, client: SSIClient, *, kind: str, key: str, end: date,
                full: bool = False, start_hist: date | None = None) -> dict:
    """Đồng bộ 1 mã (kind='symbol') hoặc 1 chỉ số (kind='index')."""
    table, key_col = ("prices_daily", "symbol") if kind == "symbol" else ("index_daily", "index_code")
    start_hist = start_hist or date.fromisoformat(config.HISTORY_START)
    fetch_time = config.now_vn().isoformat(timespec="seconds")
    stored = _load_series(con, table, key_col, key)

    def fetch(start: date) -> pd.DataFrame:
        kw = {"symbol": key} if kind == "symbol" else {"index": key}
        return cleaning.normalize_summary(client.summary(start=start, end=end, **kw))

    if full or stored.empty:
        mode, merged = "full", fetch(start_hist)
    else:
        last = stored["trade_date"].max().date()
        start = max(start_hist, last - timedelta(days=OVERLAP_DAYS))
        new = fetch(start)
        if new.empty:
            return {"key": key, "mode": "unchanged", "rows": len(stored)}
        if _adjusted(stored, new):
            mode, merged = "full(adjusted)", fetch(start_hist)
        else:
            mode = "incremental"
            keep = stored[stored["trade_date"] < pd.Timestamp(start)]
            merged = pd.concat([keep, new], ignore_index=True)

    if merged.empty:
        return {"key": key, "mode": mode, "rows": 0}
    flagged = cleaning.add_quality_flags(merged, check_vwap=(kind == "symbol"), check_ret=(kind == "symbol"))
    n = db.replace_prices(con, key, flagged, fetch_time) if kind == "symbol" \
        else db.replace_index(con, key, flagged, fetch_time)
    return {"key": key, "mode": mode, "rows": n,
            "suspect": int((flagged["data_status"] == "suspect").sum())}


def _is_fresh(con, kind: str, key: str, hours: float) -> bool:
    if hours <= 0:
        return False
    table, key_col = ("prices_daily", "symbol") if kind == "symbol" else ("index_daily", "index_code")
    ts = db.last_fetch_time(con, table, key_col, key)
    if not ts:
        return False
    return config.now_vn() - datetime.fromisoformat(ts) < timedelta(hours=hours)


def active_symbols(con, min_value: float = 1e8, days: int = 100) -> set[str]:
    """Mã còn giao dịch đáng kể: trung vị giá trị khớp (các phiên ok/no_trade) trong `days` ngày lịch gần nhất ≥ ngưỡng.
    Dùng để cập nhật hằng ngày nhanh hơn; mã kém thanh khoản được cập nhật trong lần chạy toàn bộ hằng tuần."""
    df = pd.read_sql_query(
        "SELECT symbol, match_val FROM prices_daily WHERE data_status IN ('ok','no_trade') AND trade_date >= "
        "(SELECT date(MAX(trade_date), ?) FROM prices_daily)", con, params=[f"-{days} day"])
    if df.empty:
        return set()
    med = df.groupby("symbol")["match_val"].median()
    return set(med[med >= min_value].index)


def snapshot_index_summaries(con, client: SSIClient, indexes: list[str]) -> tuple[list[str], list[tuple]]:
    """Lưu indexSummary phiên gần nhất của từng chỉ số. SSI không có lịch sử nên PHẢI lưu mỗi ngày.
    Lỗi một chỉ số không làm hỏng cả đợt cập nhật."""
    saved, failed = [], []
    for code in indexes:
        try:
            row = client.index_summary(code)
            if row and db.upsert_index_summary(con, code, row, config.now_vn().isoformat(timespec="seconds")):
                saved.append(code)
            else:
                failed.append((code, "không có dữ liệu hoặc thiếu ngày giao dịch"))
        except Exception as e:
            failed.append((code, str(e)[:120]))
        try:
            db.log_calls(con, client.drain_calls())
        except Exception:
            pass
    return saved, failed


def run_update(con, client: SSIClient, *, symbols: list[str] | None = None, limit: int | None = None,
               full: bool = False, skip_fresh_hours: float = 0, end: date | None = None,
               boards: list[str] | None = None, snapshot: bool = True, active_only: bool = False) -> dict:
    end = end or effective_end(config.now_vn())
    summary = {"end": end.isoformat(), "ok": 0, "skipped": 0, "failed": [], "snapshot": [], "snapshot_failed": []}

    # 1) Danh mục mã (nhóm API tham chiếu)
    boards = [b.upper() for b in (boards or ["HOSE"])]
    found: list[str] = []
    for b in boards:
        listing = client.securities_by_board(b)
        if listing.empty or "symbol" not in listing:
            log.warning("Sàn %s: không có danh sách mã", b)
            continue
        listing["board"] = b
        db.upsert_symbols(con, listing, config.now_vn().isoformat(timespec="seconds"))
        stocks = listing[listing["stockType"] == "Stock"]
        log.info("Sàn %s: %d dòng, %d cổ phiếu (stockType=Stock)", b, len(listing), len(stocks))
        found += list(stocks["symbol"].dropna().unique())
    db.log_calls(con, client.drain_calls())
    if not symbols:
        symbols = sorted(set(found))
    if active_only:
        existing = {r[0] for r in con.execute("SELECT DISTINCT symbol FROM prices_daily")}
        act = active_symbols(con)
        # mã còn hoạt động + mã mới niêm yết (chưa có trong DB); mã kém thanh khoản chờ lần chạy toàn bộ hằng tuần
        symbols = sorted((set(symbols) & act) | (set(symbols) - existing)) if act else symbols
    if limit:
        symbols = symbols[:limit]
    log.info("Universe: %d mã (sàn %s); ngày cuối: %s", len(symbols), ",".join(boards), end)

    # 2) Chỉ số
    indexes = [i for b in boards for i in BOARD_INDEXES.get(b, [])]
    todo = [("index", k) for k in indexes] + [("symbol", s) for s in symbols]
    for i, (kind, key) in enumerate(todo, 1):
        if _is_fresh(con, kind, key, skip_fresh_hours):
            summary["skipped"] += 1
            continue
        try:
            r = sync_series(con, client, kind=kind, key=key, end=end, full=full)
            summary["ok"] += 1
            log.info("[%d/%d] %s %s: %s, %d dòng", i, len(todo), kind, key, r["mode"], r["rows"])
        except (SSIError, Exception) as e:  # một mã lỗi không làm hỏng cả đợt
            summary["failed"].append((key, str(e)[:120]))
            log.error("[%d/%d] %s %s lỗi: %s", i, len(todo), kind, key, e)
        db.log_calls(con, client.drain_calls())
    if snapshot:
        summary["snapshot"], summary["snapshot_failed"] = snapshot_index_summaries(con, client, indexes)
        log.info("indexSummary: đã lưu %s; lỗi %s", summary["snapshot"], summary["snapshot_failed"])
    return summary


def reflag(con) -> dict:
    """Gắn lại cờ chất lượng cho dữ liệu ĐÃ LƯU (không gọi API). Dùng khi đổi quy tắc trong cleaning.py."""
    out = {}
    print("Đang gắn lại cờ chất lượng (không gọi API, có thể mất vài phút)...", flush=True)
    for table, key_col, kind in (("prices_daily", "symbol", "symbol"), ("index_daily", "index_code", "index")):
        keys = [r[0] for r in con.execute(f"SELECT DISTINCT {key_col} FROM {table}")]
        changed = 0
        for n, key in enumerate(keys, 1):
            if n % 100 == 0 or n == len(keys):
                print(f"  {table}: {n}/{len(keys)} mã", flush=True)
            df = _load_series(con, table, key_col, key)
            old = {r[0]: (r[1], r[2]) for r in con.execute(
                f"SELECT trade_date, data_status, flags FROM {table} WHERE {key_col} = ?", (key,))}
            fl = cleaning.add_quality_flags(df, check_vwap=(kind == "symbol"), check_ret=(kind == "symbol"))
            rows = []
            for d, st, f in zip(fl["trade_date"].dt.strftime("%Y-%m-%d"), fl["data_status"], fl["flags"]):
                o_st, o_fl = old.get(d, (None, ""))
                if "dup_resolved" in (o_fl or "") and "dup_resolved" not in f:  # giữ dấu vết ngày trùng đã xử lý
                    f = f + ("|" if f else "") + "dup_resolved"
                rows.append((st, f, key, d))
                changed += (st, f) != (o_st, o_fl)
            with con:
                con.executemany(f"UPDATE {table} SET data_status=?, flags=? WHERE {key_col}=? AND trade_date=?", rows)
        out[table] = changed
    return out


def report(con) -> str:
    lines = []
    for table, key_col in (("prices_daily", "symbol"), ("index_daily", "index_code")):
        n, k, d0, d1, ft = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT {key_col}), MIN(trade_date), MAX(trade_date), "
            f"MAX(fetch_time) FROM {table}").fetchone()
        lines.append(f"{table}: {n} dòng, {k} mã, {d0} → {d1}, cập nhật lần cuối {ft}")
        for st, c in con.execute(f"SELECT data_status, COUNT(*) FROM {table} GROUP BY 1 ORDER BY 2 DESC"):
            lines.append(f"    {st}: {c}")
    lines.append("Cờ chất lượng (prices_daily):")
    counts: dict[str, int] = {}
    for (fl,) in con.execute("SELECT flags FROM prices_daily WHERE flags != ''"):
        for f in fl.split("|"):
            counts[f] = counts.get(f, 0) + 1
    for f, c in sorted(counts.items(), key=lambda x: -x[1]):
        lines.append(f"    {f}: {c}")
    n_err = con.execute("SELECT COUNT(*) FROM fetch_log WHERE http IS NULL OR http != 200").fetchone()[0]
    n_all = con.execute("SELECT COUNT(*) FROM fetch_log").fetchone()[0]
    lines.append(f"fetch_log: {n_all} lệnh gọi, {n_err} lệnh không thành công (kể cả lần thử lại)")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--update", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--snapshot", action="store_true", help="chỉ lưu indexSummary hôm nay (số mã tăng/giảm chính thức)")
    ap.add_argument("--reflag", action="store_true", help="gắn lại cờ chất lượng cho dữ liệu đã lưu (không gọi API)")
    ap.add_argument("--active-only", action="store_true",
                    help="chỉ cập nhật mã còn giao dịch (nhanh hơn ~3 lần); mã kém thanh khoản để lần chạy toàn bộ")
    ap.add_argument("--full", action="store_true", help="ép tải lại toàn bộ lịch sử")
    ap.add_argument("--limit", type=int, help="chỉ lấy N mã đầu (để thử)")
    ap.add_argument("--symbols", help="danh sách mã, cách nhau dấu phẩy")
    ap.add_argument("--boards", default="HOSE",
                    help="sàn cần tải, cách nhau dấu phẩy: HOSE,HNX,UPCOM (toàn thị trường = HOSE,HNX,UPCOM)")
    ap.add_argument("--skip-fresh-hours", type=float, default=0,
                    help="bỏ qua mã đã tải trong N giờ gần đây (để chạy tiếp khi bị ngắt)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    con = db.connect(config.DB_PATH)
    db.init_db(con)
    if args.reflag:
        print("Đã cập nhật cờ:", reflag(con))
    if args.snapshot and not args.update:
        saved, failed = snapshot_index_summaries(con, SSIClient(), [i for v in BOARD_INDEXES.values() for i in v])
        print("Đã lưu:", saved, "| lỗi:", failed)
    if args.update:
        client = SSIClient()
        syms = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None
        res = run_update(con, client, symbols=syms, limit=args.limit, full=args.full,
                         skip_fresh_hours=args.skip_fresh_hours,
                         boards=[b.strip() for b in args.boards.split(",") if b.strip()],
                         active_only=args.active_only)
        print(f"\nHoàn tất: {res['ok']} thành công, {res['skipped']} bỏ qua, {len(res['failed'])} lỗi "
              f"(ngày cuối {res['end']})")
        for key, msg in res["failed"]:
            print(f"  LỖI {key}: {msg}")
        print(f"indexSummary đã lưu: {res['snapshot']} | lỗi: {res['snapshot_failed']}")
    if args.report or args.reflag or not args.update:
        print(report(con))
    return 0


if __name__ == "__main__":
    sys.exit(main())
