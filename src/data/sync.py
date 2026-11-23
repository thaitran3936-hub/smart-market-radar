"""Đồng bộ cơ sở dữ liệu từ máy chủ (GitHub Release) về máy người dùng.

Quy trình cập nhật tự động chạy trên GitHub Actions (xem .github/workflows/daily_update.yml) và đăng bản DB nén
(market.db.gz) lên Release có thẻ `data-latest`. Module này tải bản mới nhất về, kiểm tra toàn vẹn rồi thay thế
tệp DB cục bộ bằng một thao tác nguyên tử. Dữ liệu streaming và indexSummary đã ghi CỤC BỘ mà bản trên máy chủ chưa
có sẽ được GIỮ LẠI (gộp vào bản mới), nên không mất bằng chứng streaming.

Cần biến môi trường GITHUB_REPO (dạng chu-so-huu/ten-kho) và GITHUB_TOKEN (quyền chỉ đọc nội dung kho).
"""
import gzip
import os
import shutil
import sqlite3

import requests

from src.data import db

API = "https://api.github.com"


class SyncError(Exception):
    """Lỗi đồng bộ (thông báo thân thiện, không chứa token)."""


def _headers(token: str, accept: str = "application/vnd.github+json") -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": accept, "X-GitHub-Api-Version": "2022-11-28"}


def _verify(path: str) -> int:
    """Kiểm tra tệp DB tải về: mở được, quick_check ổn và có dữ liệu giá. Trả về số dòng prices_daily."""
    con = None
    try:
        con = sqlite3.connect(path)
        ok = con.execute("PRAGMA quick_check").fetchone()[0]
        n = con.execute("SELECT COUNT(*) FROM prices_daily").fetchone()[0]
    except sqlite3.Error as e:
        raise SyncError(f"Tệp tải về không phải cơ sở dữ liệu hợp lệ: {e}") from e
    finally:
        if con is not None:
            con.close()  # Windows không xóa được tệp còn đang mở
    if ok != "ok" or n == 0:
        raise SyncError("Tệp tải về hỏng hoặc không có dữ liệu giá; giữ nguyên dữ liệu cũ.")
    return n


def merge_local_extras(new_path: str, old_path: str) -> dict:
    """Giữ lại những gì CHỈ có ở bản cục bộ: dòng streaming mới hơn và ảnh chụp indexSummary chưa có ở bản mới."""
    out = {"stream_rows": 0, "index_summary": 0}
    if not os.path.exists(old_path):
        return out
    con = sqlite3.connect(new_path)
    try:
        db.init_db(con)
        con.execute("ATTACH DATABASE ? AS old", (old_path,))
        old_cols = {r[1] for r in con.execute("PRAGMA old.table_info(stream_raw)")}
        if {"recv_time", "msg_type", "symbol", "payload"} <= old_cols:
            cur = con.execute(
                "INSERT INTO stream_raw(recv_time, msg_type, symbol, payload) SELECT recv_time, msg_type, symbol, payload "
                "FROM old.stream_raw WHERE recv_time > COALESCE((SELECT MAX(recv_time) FROM main.stream_raw), '') ORDER BY id")
            out["stream_rows"] = cur.rowcount
        if con.execute("SELECT 1 FROM old.sqlite_master WHERE name='index_summary_daily'").fetchone():
            cur = con.execute("INSERT OR IGNORE INTO index_summary_daily SELECT * FROM old.index_summary_daily")
            out["index_summary"] = cur.rowcount
        con.commit()
        con.execute("DETACH DATABASE old")
    finally:
        con.close()
    return out


def download_latest(repo: str, token: str, dest: str, tag: str = "data-latest", asset: str = "market.db.gz",
                    force: bool = False, session=None) -> dict:
    """Tải bản DB mới nhất từ Release. Trả về {'status': 'up_to_date' | 'updated', ...}."""
    if not repo or not token:
        raise SyncError("Thiếu GITHUB_REPO hoặc GITHUB_TOKEN (xem README, mục 'Cập nhật tự động').")
    s = session or requests.Session()
    try:
        r = s.get(f"{API}/repos/{repo}/releases/tags/{tag}", headers=_headers(token), timeout=30)
    except requests.RequestException as e:
        raise SyncError(f"Không kết nối được tới GitHub: {type(e).__name__}") from e
    if r.status_code == 404:
        raise SyncError("Chưa có bản dữ liệu trên máy chủ (hoặc token không có quyền xem kho).")
    if r.status_code in (401, 403):
        raise SyncError("Token GitHub không hợp lệ hoặc không có quyền đọc kho.")
    if r.status_code != 200:
        raise SyncError(f"GitHub trả về HTTP {r.status_code}.")
    a = next((x for x in r.json().get("assets", []) if x.get("name") == asset), None)
    if not a:
        raise SyncError(f"Release '{tag}' chưa có tệp {asset}.")
    stamp = str(a.get("updated_at"))
    marker = dest + ".sync"
    if not force and os.path.exists(dest) and os.path.exists(marker) and open(marker).read().strip() == stamp:
        return {"status": "up_to_date", "published": stamp}

    os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)
    tmp_gz, tmp_db = dest + ".download.gz", dest + ".download"
    try:
        with s.get(a["url"], headers=_headers(token, "application/octet-stream"), stream=True, timeout=120) as resp:
            if resp.status_code != 200:
                raise SyncError(f"Tải tệp dữ liệu thất bại (HTTP {resp.status_code}).")
            with open(tmp_gz, "wb") as f:
                for chunk in resp.iter_content(1 << 20):
                    f.write(chunk)
        try:
            with gzip.open(tmp_gz, "rb") as g, open(tmp_db, "wb") as f:
                shutil.copyfileobj(g, f)
        except (OSError, EOFError) as e:
            raise SyncError(f"Tệp nén tải về bị hỏng: {e}") from e
        rows = _verify(tmp_db)
        extras = merge_local_extras(tmp_db, dest)
        os.replace(tmp_db, dest)           # thay nguyên tử: lỗi giữa chừng thì bản cũ vẫn nguyên
        with open(marker, "w") as f:
            f.write(stamp)
    finally:
        for p in (tmp_gz, tmp_db):
            if os.path.exists(p):
                os.remove(p)
    return {"status": "updated", "published": stamp, "prices_rows": rows, **extras}
