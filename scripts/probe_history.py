"""Khảo sát vòng 3 (Ngày 0): lịch sử, giá điều chỉnh, thứ tự dữ liệu, indexSummary theo ngày, ohlc.

Chạy:  python scripts/probe_history.py
Chỉ in kết quả ra màn hình (và lưu data/SSI_history.csv, data/ đã nằm trong .gitignore).
"""
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

import api_survey as S


def d(x: date) -> str:
    return x.strftime("%Y/%m/%d")


def fetch_all(tok, path, params, max_pages=10):
    """Lấy tất cả các trang của securitiesSummary. Trả về (DataFrame, meta)."""
    rows, meta = [], {}
    for page in range(1, max_pages + 1):
        time.sleep(S.GAP)
        r = requests.get(f"{S.BASE}{path}", params=dict(params, pageNumber=page),
                         headers={"Authorization": f"Bearer {tok}"}, timeout=60)
        if r.status_code != 200:
            meta["error"] = f"HTTP {r.status_code}: {r.text[:150]}"
            break
        body = r.json()
        rows += body.get("data") or []
        meta.update({k: body.get(k) for k in ("itemsCount", "pageCount", "pageSize")})
        if page >= int(body.get("pageCount") or 1):
            break
    return pd.DataFrame(rows), meta


def analyse(df: pd.DataFrame, label: str) -> pd.DataFrame:
    print(f"\n=== {label} ===")
    if df.empty:
        print("Không có dữ liệu.")
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["tradingDate"], format="%Y/%m/%d", errors="coerce")
    num = ["open", "high", "low", "close", "average", "priceChange", "priceChangePercentage",
           "totalMatch", "totalMatchValue", "totalForeignBuyValue", "totalForeignSellValue"]
    for c in num:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    ordered = "tăng dần" if df["date"].is_monotonic_increasing else (
        "giảm dần" if df["date"].is_monotonic_decreasing else "KHÔNG theo thứ tự ngày")
    print(f"Số dòng: {len(df)} | từ {df['date'].min().date()} đến {df['date'].max().date()} | "
          f"thứ tự trả về: {ordered} | ngày trùng: {int(df['date'].duplicated().sum())}")
    print("Số giá trị thiếu:", df[["close", "totalMatch", "totalMatchValue"]].isna().sum().to_dict())
    print(f"Số phiên totalMatch = 0: {int((df['totalMatch'] == 0).sum())}")
    return df.sort_values("date").reset_index(drop=True)


def check_adjusted(df: pd.DataFrame) -> None:
    """So sánh close với giá bình quân thực tế (giá trị/khối lượng) để biết close đã điều chỉnh chưa."""
    x = df[df["totalMatch"] > 0].copy()
    x["vwap"] = x["totalMatchValue"] / x["totalMatch"]
    x["ratio"] = x["close"] / x["vwap"]
    print("\nTỷ lệ close / (giá trị khớp / khối lượng khớp):")
    print("  5 phiên cũ nhất:", [round(v, 3) for v in x["ratio"].head(5)])
    print("  5 phiên mới nhất:", [round(v, 3) for v in x["ratio"].tail(5)])
    print(f"  min={x['ratio'].min():.3f} trung vị={x['ratio'].median():.3f} max={x['ratio'].max():.3f}")
    ret = df["close"].pct_change() * 100
    diff = (ret - df["priceChangePercentage"]).abs()
    big = df.loc[diff > 1.0, ["tradingDate", "close", "priceChangePercentage"]].assign(
        ret_tu_close=ret[diff > 1.0].round(2))
    print(f"\nSố phiên lợi suất tính từ close lệch > 1 điểm % so với priceChangePercentage: {len(big)}")
    if len(big):
        print(big.head(10).to_string(index=False))


def main() -> None:
    if not S.API_KEY or not S.API_SECRET:
        sys.exit("Thiếu key/secret trong .env")
    tok = S.get_token()
    print("Token: OK")
    today = date.today()
    ss = "/api/v3/data/securitiesSummary"

    # 1. Lịch sử cổ phiếu SSI từ 2018 (xem lịch sử xa nhất, phân trang, thứ tự, giá điều chỉnh)
    df, meta = fetch_all(tok, ss, {"symbol": "SSI", "from": d(date(2018, 1, 1)),
                                   "to": d(today), "pageSize": 1000})
    print("\nSSI từ 2018:", meta)
    df = analyse(df, "securitiesSummary SSI từ 2018")
    if not df.empty:
        check_adjusted(df)
        Path("data").mkdir(exist_ok=True)
        df.to_csv("data/SSI_history.csv", index=False)

    # 2. VN-Index qua securitiesSummary(index=...)
    dfi, meta = fetch_all(tok, ss, {"index": "VNINDEX", "from": d(date(2018, 1, 1)),
                                    "to": d(today), "pageSize": 1000})
    print("\nVNINDEX từ 2018:", meta)
    analyse(dfi, "securitiesSummary index=VNINDEX từ 2018")

    # 3. indexSummary: có lấy được theo ngày chỉ định / theo khoảng không?
    print("\n=== indexSummary: thử ngày chỉ định ===")
    base = {"index": "VNINDEX", "pageNumber": 1, "pageSize": 100}
    tests = [("ngày 2026/11/10", d(date(2026, 11, 10)), d(date(2026, 11, 10))),
             ("khoảng 2026/11/02-2026/11/10", d(date(2026, 11, 2)), d(date(2026, 11, 10))),
             ("ngày 2026/01/15", d(date(2026, 1, 15)), d(date(2026, 1, 15)))]
    for name, f, t in tests:
        r = S.call(tok, f"indexSummary {name}", "/api/v3/data/indexSummary",
                   dict(base, **{"from": f, "to": t}))
        got = (r.get("first_row") or {}).get("tradingDate")
        print(f"  {name}: HTTP {r.get('http')}, rows={r.get('rows')}, tradingDate trả về={got}, "
              f"{r.get('error') or ''}")

    # 4. ohlc: thử thêm vài định dạng ngày
    print("\n=== ohlc: thử định dạng ngày ===")
    o_from, o_to = date(2026, 11, 2), date(2026, 11, 16)
    cands = [("yyyy/mm/dd HH:MM:SS", o_from.strftime("%Y/%m/%d 00:00:00"), o_to.strftime("%Y/%m/%d 23:59:59")),
             ("yyyy-mm-dd HH:MM:SS", o_from.strftime("%Y-%m-%d 00:00:00"), o_to.strftime("%Y-%m-%d 23:59:59")),
             ("yyyy/mm/dd (chỉ pageIndex)", d(o_from), d(o_to)),
             ("dd/mm/yyyy HH:MM:SS", o_from.strftime("%d/%m/%Y 00:00:00"), o_to.strftime("%d/%m/%Y 23:59:59"))]
    for name, f, t in cands:
        r = S.call(tok, f"ohlc {name}", "/api/v3/data/ohlc",
                   {"symbol": "SSI", "from": f, "to": t, "timeFrame": "1d",
                    "pageIndex": 1, "pageSize": 10})
        print(f"  {name}: HTTP {r.get('http')}, rows={r.get('rows')}, {r.get('error') or ''}")
        if r.get("http") == 200 and r.get("fields"):
            print("    trường:", r["fields"])

    print("\nXong. File lịch sử SSI (nếu có): data/SSI_history.csv")


if __name__ == "__main__":
    main()
