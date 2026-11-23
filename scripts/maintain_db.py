"""Bảo trì cơ sở dữ liệu: dọn dữ liệu streaming thô cũ và thu gọn tệp. KHÔNG bao giờ xóa dữ liệu giá.

    python scripts/maintain_db.py                       # chỉ báo kích thước từng bảng
    python scripts/maintain_db.py --prune-stream-days 60 --vacuum
Chỉ bảng stream_raw (thông điệp WebSocket thô, tăng nhanh nhất) bị dọn; giữ lại các phiên trong N ngày gần nhất.
Nên giữ nguyên đến khi nộp bài vì dữ liệu streaming là bằng chứng của đồ án.
"""
import argparse
import os
import sqlite3
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import config  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prune-stream-days", type=int, help="xóa stream_raw cũ hơn N ngày")
    ap.add_argument("--vacuum", action="store_true", help="thu gọn tệp sau khi dọn (cần trống ~ bằng kích thước tệp)")
    a = ap.parse_args()
    con = sqlite3.connect(config.DB_PATH)
    print(f"Tệp: {config.DB_PATH} ({os.path.getsize(config.DB_PATH) / 1e6:.0f} MB)")
    for t in ("prices_daily", "index_daily", "index_summary_daily", "symbols", "stream_raw", "fetch_log"):
        try:
            print(f"  {t}: {con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]:,} dòng")
        except sqlite3.Error:
            pass
    if a.prune_stream_days is not None:
        cutoff = (config.now_vn() - timedelta(days=a.prune_stream_days)).isoformat()
        with con:
            n = con.execute("DELETE FROM stream_raw WHERE recv_time < ?", (cutoff,)).rowcount
        print(f"Đã xóa {n:,} dòng stream_raw cũ hơn {a.prune_stream_days} ngày")
    if a.vacuum:
        con.execute("VACUUM")
        print(f"Sau thu gọn: {os.path.getsize(config.DB_PATH) / 1e6:.0f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
