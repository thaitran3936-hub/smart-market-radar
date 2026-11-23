"""Cập nhật dữ liệu hằng ngày: giá cuối ngày (tăng dần) + ảnh chụp indexSummary của phiên mới nhất.

Chạy tay:   python scripts/daily_update.py
Chạy tự động (Windows Task Scheduler, 15:30 các ngày làm việc): xem README, mục "Cập nhật tự động".

- Chạy sau 15:00 (HOSE đóng cửa 14:45) thì lấy được phiên hôm nay; trước 15:00 chỉ lấy tới hôm qua.
- Ngày nghỉ lễ không có phiên: script vẫn chạy bình thường, không có dữ liệu mới.
- Nhật ký: data/daily_update.log. Thoát mã 1 nếu > 5% mã bị lỗi (để Task Scheduler báo thất bại).
"""
import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import config  # noqa: E402
from src.data import collector, db  # noqa: E402
from src.ssi_api.client import SSIClient  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--boards", default="HOSE,HNX,UPCOM", help="sàn cần cập nhật (mặc định: toàn thị trường)")
    ap.add_argument("--active-only", action="store_true",
                    help="chỉ cập nhật mã còn giao dịch (nhanh hơn); chạy không cờ này mỗi tuần để cập nhật mọi mã")
    ap.add_argument("--max-fail-share", type=float, default=0.05)
    args = ap.parse_args(argv)

    Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    log_file = Path(config.DB_PATH).parent / "daily_update.log"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()])
    log = logging.getLogger("daily_update")
    t0 = time.time()
    con = db.connect(config.DB_PATH)
    db.init_db(con)
    boards = [b.strip() for b in args.boards.split(",") if b.strip()]
    try:
        res = collector.run_update(con, SSIClient(), boards=boards, active_only=args.active_only)
    except Exception as e:  # lỗi chung (mất mạng, sai key...): ghi nhật ký và báo thất bại
        log.error("Cập nhật thất bại: %s: %s", type(e).__name__, e)
        return 2
    total = res["ok"] + res["skipped"] + len(res["failed"])
    share = len(res["failed"]) / total if total else 0.0
    log.info("Xong sau %.0f giây: %d thành công, %d lỗi (%.1f%%), ngày cuối %s, indexSummary %s",
             time.time() - t0, res["ok"], len(res["failed"]), share * 100, res["end"], res["snapshot"])
    for key, msg in res["failed"][:20]:
        log.warning("  LỖI %s: %s", key, msg)
    return 1 if share > args.max_fail_share else 0


if __name__ == "__main__":
    sys.exit(main())
