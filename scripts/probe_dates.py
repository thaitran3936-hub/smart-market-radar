"""Khảo sát vòng 2 (Ngày 0): dò định dạng ngày + thử tải hàng loạt theo chỉ số.

Chạy:  python scripts/probe_dates.py
       python scripts/probe_dates.py --probe-rate   (thử thêm giới hạn tần suất)

Dùng lại hàm call()/get_token() của api_survey.py (cùng thư mục scripts/).
Kết quả ghi đè docs/api_survey_output.json (đã nằm trong .gitignore, không chứa key).
"""
import argparse
import json
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import api_survey as S

TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def variants(d: date) -> dict:
    ts = int(datetime(d.year, d.month, d.day, tzinfo=TZ).timestamp())
    return {
        "iso": d.strftime("%Y-%m-%d"),
        "slash(yyyy/mm/dd)": d.strftime("%Y/%m/%d"),
        "dmy(dd/mm/yyyy)": d.strftime("%d/%m/%Y"),
        "compact(yyyymmdd)": d.strftime("%Y%m%d"),
        "isoT": d.strftime("%Y-%m-%dT00:00:00"),
        "epoch_s": str(ts),
        "epoch_ms": str(ts * 1000),
    }


def last_workday(d: date) -> date:
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def find_format(tok, label, path, base_params, d_from, d_to, want_rows=1):
    """Thử lần lượt các định dạng ngày; trả về tên định dạng đầu tiên chạy được."""
    vf, vt = variants(d_from), variants(d_to)
    winner = None
    for name in vf:
        params = dict(base_params, **{"from": vf[name], "to": vt[name]})
        r = S.call(tok, f"{label} [fmt={name}]", path, params, "dò định dạng ngày")
        if winner is None and r.get("http") == 200 and (r.get("rows") or 0) >= want_rows:
            winner = name
    return winner


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe-rate", action="store_true")
    args = ap.parse_args()
    if not S.API_KEY or not S.API_SECRET:
        sys.exit("Thiếu key/secret trong .env")

    tok = S.get_token()
    print("Token: OK")
    today = date.today()
    day = last_workday(today)
    short_from = today - timedelta(days=30)

    # ---- A. Dò định dạng ngày (khoảng ngắn 30 ngày) ----
    fm = {}
    fm["securitiesSummary"] = find_format(
        tok, "securitiesSummary SSI", "/api/v3/data/securitiesSummary",
        {"symbol": "SSI", "pageNumber": 1, "pageSize": 10}, short_from, today)
    fm["ohlc"] = find_format(
        tok, "ohlc SSI 1d", "/api/v3/data/ohlc",
        {"symbol": "SSI", "timeFrame": "1d", "pageIndex": 1, "pageNumber": 1, "pageSize": 10},
        short_from, today)
    fm["indexSummary"] = find_format(
        tok, "indexSummary VNINDEX", "/api/v3/data/indexSummary",
        {"index": "VNINDEX", "pageNumber": 1, "pageSize": 100}, short_from, today, want_rows=2)
    print("\nĐịnh dạng ngày chạy được:", json.dumps(fm, ensure_ascii=False))

    def f(ep: str, d: date) -> str:
        return variants(d)[fm[ep]] if fm.get(ep) else d.isoformat()

    # ---- B. Các bài thử còn lại (dùng định dạng thắng; nếu không có thì dùng iso) ----
    y1 = today - timedelta(days=365)
    ss = "/api/v3/data/securitiesSummary"
    S.call(tok, "securitiesSummary SSI 1 năm (size 100)", ss,
           {"symbol": "SSI", "from": f("securitiesSummary", y1), "to": f("securitiesSummary", today),
            "pageNumber": 1, "pageSize": 100})
    S.call(tok, "securitiesSummary SSI 1 năm (size 1000)", ss,
           {"symbol": "SSI", "from": f("securitiesSummary", y1), "to": f("securitiesSummary", today),
            "pageNumber": 1, "pageSize": 1000}, "pageSize tối đa?")
    one = {"from": f("securitiesSummary", day), "to": f("securitiesSummary", day)}
    S.call(tok, "securitiesSummary index=VNINDEX, 1 ngày", ss,
           dict(one, index="VNINDEX", pageNumber=1, pageSize=1000),
           "có trả toàn bộ cổ phiếu của chỉ số trong 1 ngày không?")
    S.call(tok, "securitiesSummary index=VN30, 1 ngày", ss,
           dict(one, index="VN30", pageNumber=1, pageSize=100))
    S.call(tok, "securitiesSummary board=HOSE, 1 ngày", ss,
           dict(one, board="HOSE", pageNumber=1, pageSize=1000), "thử tham số board")

    oh = "/api/v3/data/ohlc"
    S.call(tok, "ohlc SSI 1d từ 2015", oh,
           {"symbol": "SSI", "from": f("ohlc", date(2015, 1, 1)), "to": f("ohlc", today),
            "timeFrame": "1d", "pageIndex": 1, "pageNumber": 1, "pageSize": 1000},
           "lịch sử xa nhất, số dòng/lần")
    S.call(tok, "ohlc symbol=VNINDEX 1d", oh,
           {"symbol": "VNINDEX", "from": f("ohlc", y1), "to": f("ohlc", today),
            "timeFrame": "1d", "pageIndex": 1, "pageNumber": 1, "pageSize": 1000})
    S.call(tok, "ohlc index=VNINDEX 1d", oh,
           {"index": "VNINDEX", "from": f("ohlc", y1), "to": f("ohlc", today),
            "timeFrame": "1d", "pageIndex": 1, "pageNumber": 1, "pageSize": 1000})

    ix = "/api/v3/data/indexSummary"
    S.call(tok, "indexSummary VNINDEX 1 năm (size 1000)", ix,
           {"index": "VNINDEX", "from": f("indexSummary", y1), "to": f("indexSummary", today),
            "pageNumber": 1, "pageSize": 1000})
    S.call(tok, "indexSummary VNINDEX từ 2018", ix,
           {"index": "VNINDEX", "from": f("indexSummary", date(2018, 1, 1)),
            "to": f("indexSummary", today), "pageNumber": 1, "pageSize": 1000},
           "lịch sử xa nhất của chỉ số")
    S.call(tok, "indexSummary board=HOSE, 1 ngày", ix,
           {"board": "HOSE", "from": f("indexSummary", day), "to": f("indexSummary", day),
            "pageNumber": 1, "pageSize": 100}, "tất cả chỉ số HOSE trong 1 ngày")

    if args.probe_rate:
        print("\nThử tần suất: tối đa 40 lệnh liên tiếp không giãn cách...")
        S.GAP = 0.0
        for i in range(40):
            r = S.call(tok, f"rate-probe #{i+1}", "/api/v3/data/indexList", {}, "thử quota")
            if r.get("http") == 429:
                print(f"  Bị giới hạn (429) ở lần {i+1}. Header: {r.get('rate_limit')}")
                break
        else:
            print("  40 lệnh liên tiếp không bị chặn.")

    S.OUT.parent.mkdir(parents=True, exist_ok=True)
    S.OUT.write_text(json.dumps({"date_formats": fm, "results": S.results},
                                ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n| # | Tên | HTTP | rows | itemsCount | lỗi |")
    print("|---|---|---|---|---|---|")
    for i, r in enumerate(S.results, 1):
        print(f"| {i} | {r['name']} | {r.get('http','-')} | {r.get('rows')} | "
              f"{r.get('itemsCount')} | {(r.get('error') or '')[:70]} |")
    print(f"\nChi tiết: {S.OUT}")


if __name__ == "__main__":
    main()
