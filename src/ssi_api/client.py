"""Client SSI FastConnect v3 (chỉ dữ liệu thị trường, không có chức năng giao dịch).

Điểm chính:
- Token tự lấy/làm mới; 401 thì xác thực lại.
- Giãn cách tối thiểu giữa các lệnh gọi (rate limit); 429 và lỗi 5xx thì chờ rồi thử lại.
- Dữ liệu lịch sử được lấy theo từng khoảng ngày đủ nhỏ để mỗi lệnh chỉ cần 1 trang.
  Lý do: khảo sát cho thấy thứ tự dòng trả về không ổn định, nên không tin vào phân trang.
- Ghi nhật ký mọi lệnh gọi (self.calls) để lưu vào bảng fetch_log; không bao giờ ghi key/secret/token.
"""
import json
import logging
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from src import config

log = logging.getLogger("ssi")

DATE_FMT = "%Y/%m/%d"  # SSI chỉ nhận ngày dạng yyyy/mm/dd (đã khảo sát)
PAGE_SIZE = 1000
MAX_CHUNK_DAYS = 1095  # ~3 năm ≈ 750 phiên < 1000 dòng/trang


class SSIError(Exception):
    def __init__(self, message: str, http: int | None = None):
        super().__init__(message)
        self.http = http


def split_range(start: date, end: date, max_days: int = MAX_CHUNK_DAYS) -> list[tuple[date, date]]:
    """Chia [start, end] thành các khoảng liên tiếp, không chồng lấn, mỗi khoảng ≤ max_days ngày."""
    out, cur = [], start
    while cur <= end:
        stop = min(cur + timedelta(days=max_days - 1), end)
        out.append((cur, stop))
        cur = stop + timedelta(days=1)
    return out


class SSIClient:
    def __init__(self, api_key=None, api_secret=None, base=None, min_interval=None,
                 session=None, sleep=time.sleep):
        self.api_key = api_key if api_key is not None else config.API_KEY
        self.api_secret = api_secret if api_secret is not None else config.API_SECRET
        self.base = (base or config.API_BASE).rstrip("/")
        self.min_interval = config.MIN_INTERVAL if min_interval is None else min_interval
        self.session = session or requests.Session()
        self._sleep = sleep
        self._token: str | None = None
        self._token_exp = 0.0
        self._last = 0.0
        self.calls: list[dict] = []

    # ---------- xác thực ----------
    def _authenticate(self) -> None:
        if not self.api_key or not self.api_secret:
            raise SSIError("Thiếu API key/secret (kiểm tra file .env).")
        r = None
        for attempt in range(1, 6):  # rớt mạng thoáng qua thì thử lại, không dừng cả đợt tải
            try:
                r = self.session.post(f"{self.base}/api/v3/auth/token",
                                      json={"apiKey": self.api_key, "apiSecret": self.api_secret},
                                      timeout=30)
            except requests.RequestException as e:
                wait = min(5 * attempt, 30)
                log.warning("Lấy token lỗi mạng (%s), thử lại sau %ss (lần %d/5)",
                            type(e).__name__, wait, attempt)
                self._sleep(wait)
                continue
            if r.status_code >= 500 or r.status_code == 429:
                self._sleep(min(5 * attempt, 30))
                continue
            break
        if r is None:
            raise SSIError("Không kết nối được tới SSI để lấy token (kiểm tra mạng/VPN/tường lửa).")
        if r.status_code != 200:
            raise SSIError(f"Xác thực thất bại: HTTP {r.status_code} {r.text[:150]}", http=r.status_code)
        body = r.json()
        self._token = body["accessToken"]
        self._token_exp = float(body.get("expiresAt") or time.time() + 3000) - 60
        log.info("Đã lấy access token mới")

    # ---------- gọi HTTP ----------
    def _pace(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last)
        if wait > 0:
            self._sleep(wait)
        self._last = time.monotonic()

    @staticmethod
    def _retry_after(resp, attempt: int) -> float:
        raw = resp.headers.get("X-RATELIMIT-RESET")
        try:
            v = float(raw)
            wait = v - time.time() if v > 1e9 else v
        except (TypeError, ValueError):
            wait = 2.0 * attempt
        return min(max(wait, 1.0), 60.0)

    def _record(self, path, params, http, seconds, body=None, error=None) -> None:
        rows = items = None
        if isinstance(body, dict):
            data = body.get("data")
            rows = len(data) if isinstance(data, list) else None
            items = body.get("itemsCount")
        elif isinstance(body, list):
            rows = len(body)
        self.calls.append({
            "fetch_time": config.now_vn().isoformat(timespec="seconds"),
            "endpoint": path, "params": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "http": http, "seconds": round(seconds, 2), "rows": rows,
            "items_count": items, "error": error})

    def drain_calls(self) -> list[dict]:
        out, self.calls = self.calls, []
        return out

    def get(self, path: str, params: dict | None = None, retries: int = 5):
        params = {k: v for k, v in (params or {}).items() if v is not None}
        for attempt in range(1, retries + 1):
            if not self._token or time.time() >= self._token_exp:
                self._authenticate()
            self._pace()
            t0 = time.time()
            try:
                r = self.session.get(f"{self.base}{path}", params=params, timeout=60,
                                     headers={"Authorization": f"Bearer {self._token}"})
            except requests.RequestException as e:
                self._record(path, params, None, time.time() - t0, error=type(e).__name__)
                wait = min(2 ** attempt, 30)
                log.warning("%s lỗi mạng (%s), thử lại sau %ss", path, type(e).__name__, wait)
                self._sleep(wait)
                continue
            if r.status_code == 200:
                body = r.json()
                self._record(path, params, 200, time.time() - t0, body=body)
                return body
            self._record(path, params, r.status_code, time.time() - t0, error=r.text[:150])
            if r.status_code == 401:
                log.warning("%s: 401, xác thực lại", path)
                self._token = None
                continue
            if r.status_code == 429:
                wait = self._retry_after(r, attempt)
                log.warning("%s: 429 (vượt giới hạn), chờ %.0fs", path, wait)
                self._sleep(wait)
                continue
            if r.status_code >= 500:
                wait = min(2 ** attempt, 30)
                log.warning("%s: HTTP %s, thử lại sau %ss", path, r.status_code, wait)
                self._sleep(wait)
                continue
            raise SSIError(f"{path}: HTTP {r.status_code} {r.text[:200]}", http=r.status_code)
        raise SSIError(f"{path}: hết số lần thử")

    # ---------- dữ liệu ----------
    def securities_by_board(self, board: str) -> pd.DataFrame:
        body = self.get("/api/v3/data/securitiesByBoard", {"board": board})
        rows = body if isinstance(body, list) else (body.get("data") or [])
        return pd.DataFrame(rows)

    def index_summary(self, index: str) -> dict:
        """indexSummary: ảnh chụp phiên gần nhất của chỉ số (số mã tăng/giảm/trần/sàn chính thức). Không có lịch sử."""
        body = self.get("/api/v3/data/indexSummary", {"index": index, "pageNumber": 1, "pageSize": 100})
        rows = body if isinstance(body, list) else (body.get("data") or [])
        return rows[0] if rows else {}

    def summary(self, *, symbol: str | None = None, index: str | None = None,
                start: date, end: date) -> pd.DataFrame:
        """securitiesSummary của một mã hoặc một chỉ số trong [start, end] (dữ liệu thô)."""
        if (symbol is None) == (index is None):
            raise ValueError("Chỉ truyền đúng một trong hai: symbol hoặc index")
        frames = [self._summary_range(symbol, index, s, e) for s, e in split_range(start, end)]
        frames = [f for f in frames if not f.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _summary_range(self, symbol, index, start: date, end: date) -> pd.DataFrame:
        body = self.get("/api/v3/data/securitiesSummary", {
            "symbol": symbol, "index": index, "from": start.strftime(DATE_FMT),
            "to": end.strftime(DATE_FMT), "pageNumber": 1, "pageSize": PAGE_SIZE})
        rows = body.get("data") or []
        total = int(body.get("itemsCount") or len(rows))
        if total > len(rows):
            # Không phân trang: chia đôi khoảng ngày cho tới khi vừa một trang
            span = (end - start).days
            if span < 30:
                raise SSIError(f"{symbol or index}: {total} dòng trong {span} ngày, không thể chia nhỏ hơn")
            mid = start + timedelta(days=span // 2)
            left = self._summary_range(symbol, index, start, mid)
            right = self._summary_range(symbol, index, mid + timedelta(days=1), end)
            parts = [p for p in (left, right) if not p.empty]
            return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        return pd.DataFrame(rows)


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)
