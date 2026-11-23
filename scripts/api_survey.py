"""Khảo sát SSI FastConnect v3 (Ngày 0).

Chạy:  python scripts/api_survey.py
       python scripts/api_survey.py --probe-rate   (thử thêm giới hạn tần suất)

Chỉ dùng endpoint DỮ LIỆU THỊ TRƯỜNG (token không kèm OTP nên không có quyền giao dịch).
Kết quả: bảng markdown trên màn hình + docs/api_survey_output.json (đã nằm trong .gitignore).
Script KHÔNG in key/secret/token.
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
BASE = os.getenv("SSI_API_BASE", "https://api.ssi.com.vn").rstrip("/")
# Chấp nhận cả tên biến cũ (SSI_CONSUMER_*) để bạn không phải sửa .env ngay
API_KEY = os.getenv("SSI_API_KEY") or os.getenv("SSI_CONSUMER_ID", "")
API_SECRET = os.getenv("SSI_API_SECRET") or os.getenv("SSI_CONSUMER_SECRET", "")
GAP = float(os.getenv("SSI_MIN_INTERVAL", "1.0"))
OUT = Path("docs/api_survey_output.json")
RL_HEADERS = ("X-RATELIMIT-LIMIT", "X-RATELIMIT-REMAINING", "X-RATELIMIT-RESET")

results = []
_last = 0.0


def get_token() -> str:
    r = requests.post(f"{BASE}/api/v3/auth/token",
                      json={"apiKey": API_KEY, "apiSecret": API_SECRET}, timeout=30)
    if r.status_code != 200:
        sys.exit(f"Không lấy được token. HTTP {r.status_code}: {r.text[:300]}")
    tok = r.json().get("accessToken")
    if not tok:
        sys.exit("Phản hồi không có accessToken (kiểm tra lại key/secret).")
    return tok


def call(token: str, name: str, path: str, params: dict | None = None, note: str = "") -> dict:
    """Gọi 1 endpoint GET (có giãn cách), ghi tóm tắt vào results."""
    global _last
    wait = GAP - (time.time() - _last)
    if wait > 0:
        time.sleep(wait)
    t0 = time.time()
    rec = {"name": name, "path": path, "params": params or {}, "note": note}
    try:
        r = requests.get(f"{BASE}{path}", params=params,
                         headers={"Authorization": f"Bearer {token}",
                                  "Content-Type": "application/json"}, timeout=60)
        _last = time.time()
        rec["http"] = r.status_code
        rec["seconds"] = round(_last - t0, 2)
        rec["rate_limit"] = {h: r.headers.get(h) for h in RL_HEADERS if r.headers.get(h)}
        try:
            body = r.json()
        except ValueError:
            body = None
            rec["error"] = r.text[:200]
        data = None
        if isinstance(body, dict):
            for k in ("itemsCount", "pageCount", "pageSize", "pageNumber", "message"):
                if k in body:
                    rec[k] = body[k]
            data = body.get("data")
            if data is None and r.status_code != 200:
                rec["error"] = json.dumps(body, ensure_ascii=False)[:200]
        elif isinstance(body, list):
            data = body
        rec["rows"] = len(data) if isinstance(data, list) else None
        if isinstance(data, list) and data and isinstance(data[0], dict):
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
    if not API_KEY or not API_SECRET:
        sys.exit("Thiếu API key/secret trong file .env (SSI_API_KEY, SSI_API_SECRET)")

    tok = get_token()
    print("Token: OK")
    today = date.today()
    iso = lambda d: d.isoformat()  # noqa: E731
    year_ago = today - timedelta(days=365)
    day = today - timedelta(days=1)
    while day.weekday() >= 5:  # lùi về ngày làm việc gần nhất (có thể trùng ngày lễ)
        day -= timedelta(days=1)

    # --- Danh mục / tham chiếu ---
    call(tok, "securitiesByBoard HOSE", "/api/v3/data/securitiesByBoard",
         {"board": "HOSE"}, "đếm số mã HOSE")
    call(tok, "securitiesByBoard HNX", "/api/v3/data/securitiesByBoard", {"board": "HNX"})
    call(tok, "indexList (không tham số)", "/api/v3/data/indexList", {},
         "lấy mã VN-Index đúng")
    call(tok, "indexList board=HOSE", "/api/v3/data/indexList", {"board": "HOSE"})
    call(tok, "masterdata (không tham số)", "/api/v3/data/masterdata", {})

    # --- Dữ liệu thị trường ---
    call(tok, "securitiesSummary SSI 1 năm (size 100)", "/api/v3/data/securitiesSummary",
         {"symbol": "SSI", "from": iso(year_ago), "to": iso(today),
          "pageNumber": 1, "pageSize": 100}, "tham số theo tài liệu")
    call(tok, "securitiesSummary SSI 1 năm (size 1000)", "/api/v3/data/securitiesSummary",
         {"symbol": "SSI", "from": iso(year_ago), "to": iso(today),
          "pageNumber": 1, "pageSize": 1000}, "kiểm tra pageSize tối đa")
    call(tok, "securitiesSummary không symbol, 1 ngày", "/api/v3/data/securitiesSummary",
         {"from": iso(day), "to": iso(day), "pageNumber": 1, "pageSize": 1000},
         "có trả toàn thị trường 1 ngày không?")
    call(tok, "ohlc SSI 1d, 1 năm", "/api/v3/data/ohlc",
         {"symbol": "SSI", "from": iso(year_ago), "to": iso(today), "timeFrame": "1d",
          "pageIndex": 1, "pageSize": 1000})
    call(tok, "ohlc SSI 1d, từ 2015", "/api/v3/data/ohlc",
         {"symbol": "SSI", "from": "2015-01-01", "to": iso(today), "timeFrame": "1d",
          "pageIndex": 1, "pageSize": 1000}, "lịch sử xa nhất / khoảng ngày tối đa")
    call(tok, "ohlc VNINDEX 1d", "/api/v3/data/ohlc",
         {"symbol": "VNINDEX", "from": iso(year_ago), "to": iso(today), "timeFrame": "1d",
          "pageIndex": 1, "pageSize": 1000}, "ohlc có nhận mã chỉ số không?")
    # indexSummary: tên tham số chưa rõ, thử vài biến thể
    for key in ("indexCode", "index", "symbol"):
        call(tok, f"indexSummary ({key}=VNINDEX)", "/api/v3/data/indexSummary",
             {key: "VNINDEX", "from": iso(year_ago), "to": iso(today),
              "pageNumber": 1, "pageSize": 100}, "thử tên tham số")

    if args.probe_rate:
        print("Thử tần suất: tối đa 30 lệnh liên tiếp không giãn cách...")
        global GAP
        GAP = 0.0
        for i in range(30):
            r = call(tok, f"rate-probe #{i+1}", "/api/v3/data/securitiesByBoard",
                     {"board": "HOSE"}, "thử quota")
            if r.get("http") == 429:
                print(f"  Bị giới hạn (429) ở lần {i+1}. Header: {r.get('rate_limit')}")
                break

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n| # | Tên | HTTP | rows | itemsCount | rate-limit | lỗi |")
    print("|---|---|---|---|---|---|---|")
    for i, r in enumerate(results, 1):
        rl = r.get("rate_limit", {})
        rl_s = "/".join(str(rl.get(h, "-")) for h in RL_HEADERS) if rl else "-"
        print(f"| {i} | {r['name']} | {r.get('http','-')} | {r.get('rows')} | "
              f"{r.get('itemsCount')} | {rl_s} | {(r.get('error') or '')[:80]} |")
    print(f"\nChi tiết (kèm trường dữ liệu, dòng đầu/cuối): {OUT}")
    print("Cột rate-limit = LIMIT/REMAINING/RESET")


if __name__ == "__main__":
    main()
