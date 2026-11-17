"""Khảo sát SSI FastConnect Data (Ngày 0).

Chạy:  python scripts/api_survey.py            (khảo sát cơ bản)
       python scripts/api_survey.py --probe-rate  (thử thêm giới hạn tần suất)

Kết quả: in bảng markdown ra màn hình + lưu docs/api_survey_output.json
(file này đã nằm trong .gitignore). KHÔNG in ra key/secret/token.
"""
import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
BASE = os.getenv("SSI_API_BASE", "https://fc-data.ssi.com.vn/api/v2/Market").rstrip("/")
CID = os.getenv("SSI_CONSUMER_ID", "")
CSEC = os.getenv("SSI_CONSUMER_SECRET", "")
GAP = float(os.getenv("SSI_MIN_INTERVAL", "1.0"))
OUT = Path("docs/api_survey_output.json")

results = []
_last = 0.0


def fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def last_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def get_token() -> str:
    r = requests.post(f"{BASE}/AccessToken",
                      json={"consumerID": CID, "consumerSecret": CSEC}, timeout=30)
    r.raise_for_status()
    body = r.json()
    tok = (body.get("data") or {}).get("accessToken")
    if not tok:
        sys.exit(f"Không lấy được token. message={body.get('message')} status={body.get('status')}")
    return tok


def call(token: str, name: str, path: str, params: dict, note: str = "") -> dict:
    """Gọi 1 endpoint (có giãn cách), ghi tóm tắt vào results."""
    global _last
    wait = GAP - (time.time() - _last)
    if wait > 0:
        time.sleep(wait)
    t0 = time.time()
    rec = {"name": name, "path": path, "params": params, "note": note}
    try:
        r = requests.get(f"{BASE}/{path}", params=params,
                         headers={"Authorization": f"Bearer {token}"}, timeout=60)
        _last = time.time()
        rec["http"] = r.status_code
        rec["seconds"] = round(_last - t0, 2)
        try:
            body = r.json()
        except ValueError:
            body = {}
            rec["error"] = r.text[:200]
        rec["status"] = body.get("status")
        rec["message"] = body.get("message")
        rec["totalRecord"] = body.get("totalRecord")
        data = body.get("data")
        rec["rows"] = len(data) if isinstance(data, list) else None
        if isinstance(data, list) and data:
            rec["fields"] = sorted(data[0].keys())
            rec["first_row"] = data[0]
            rec["last_row"] = data[-1]
    except requests.RequestException as e:
        _last = time.time()
        rec["error"] = type(e).__name__
    results.append(rec)
    return rec


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe-rate", action="store_true")
    args = ap.parse_args()
    if not CID or not CSEC:
        sys.exit("Thiếu SSI_CONSUMER_ID / SSI_CONSUMER_SECRET trong file .env")

    tok = get_token()
    print("Token: OK")
    today = date.today()
    day = last_weekday(today - timedelta(days=1))  # phiên gần nhất đã đóng (có thể là ngày lễ)

    call(tok, "IndexList HOSE", "IndexList",
         {"exchange": "HOSE", "pageIndex": 1, "pageSize": 100})
    call(tok, "Securities HOSE p1", "Securities",
         {"market": "HOSE", "pageIndex": 1, "pageSize": 1000}, "đếm số mã HOSE")
    call(tok, "IndexComponents VN30", "IndexComponents",
         {"indexCode": "VN30", "pageIndex": 1, "pageSize": 100})
    call(tok, "DailyStockPrice SSI 1 năm", "DailyStockPrice",
         {"symbol": "SSI", "market": "HOSE", "fromDate": fmt(today - timedelta(days=365)),
          "toDate": fmt(today), "pageIndex": 1, "pageSize": 1000},
         "kiểm tra close vs closepriceadjusted, số dòng")
    call(tok, "DailyStockPrice SSI từ 2021", "DailyStockPrice",
         {"symbol": "SSI", "market": "HOSE", "fromDate": "01/01/2021",
          "toDate": fmt(today), "pageIndex": 1, "pageSize": 1000},
         "kiểm tra khoảng ngày tối đa / lịch sử tối đa")
    for p in (1, 2, 3):
        call(tok, f"DailyStockPrice HOSE toàn sàn 1 ngày p{p}", "DailyStockPrice",
             {"market": "HOSE", "fromDate": fmt(day), "toDate": fmt(day),
              "pageIndex": p, "pageSize": 1000},
             "không truyền symbol: có trả toàn sàn không?")
    call(tok, "DailyOhlc SSI 1 năm", "DailyOhlc",
         {"symbol": "SSI", "fromDate": fmt(today - timedelta(days=365)),
          "toDate": fmt(today), "pageIndex": 1, "pageSize": 1000})
    for idx in ("VNINDEX", "VN-Index", "VN30"):
        call(tok, f"DailyIndex {idx} 1 năm", "DailyIndex",
             {"indexId": idx, "fromDate": fmt(today - timedelta(days=365)),
              "toDate": fmt(today), "pageIndex": 1, "pageSize": 1000},
             "thử các mã chỉ số để biết mã đúng")
    call(tok, "DailyIndex VNINDEX từ 2021", "DailyIndex",
         {"indexId": "VNINDEX", "fromDate": "01/01/2021", "toDate": fmt(today),
          "pageIndex": 1, "pageSize": 1000}, "lịch sử tối đa của chỉ số")

    if args.probe_rate:
        print("Thử tần suất: 12 lệnh gọi liên tiếp không giãn cách...")
        global GAP
        GAP = 0.0
        for i in range(12):
            r = call(tok, f"rate-probe #{i+1}", "IndexList",
                     {"exchange": "HOSE", "pageIndex": 1, "pageSize": 10}, "thử quota")
            if r.get("http") != 200 or r.get("status") not in ("Success", 200):
                print(f"  Dừng ở lần {i+1}: http={r.get('http')} message={r.get('message')}")
                break
            time.sleep(0.05)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n| # | Tên | HTTP | status | rows | totalRecord | message/lỗi |")
    print("|---|---|---|---|---|---|---|")
    for i, r in enumerate(results, 1):
        print(f"| {i} | {r['name']} | {r.get('http','-')} | {r.get('status')} | "
              f"{r.get('rows')} | {r.get('totalRecord')} | {r.get('message') or r.get('error') or ''} |")
    print(f"\nChi tiết (kèm dòng đầu/cuối, danh sách trường): {OUT}")


if __name__ == "__main__":
    main()
