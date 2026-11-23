"""Tải bản dữ liệu mới nhất từ máy chủ (GitHub Release) về data/market.db.

    python scripts/sync_db.py            # chỉ tải nếu có bản mới
    python scripts/sync_db.py --force    # tải lại dù đã mới nhất
Cần GITHUB_REPO và GITHUB_TOKEN trong .env (hoặc biến môi trường). Streaming/indexSummary ghi cục bộ được giữ lại.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import config  # noqa: E402
from src.data import sync  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    try:
        r = sync.download_latest(config.GITHUB_REPO, config.GITHUB_TOKEN, config.DB_PATH, force=a.force)
    except sync.SyncError as e:
        print("Không đồng bộ được:", e)
        return 1
    print(r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
