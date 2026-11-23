"""Cấu hình đọc từ biến môi trường (.env). File này không chứa bí mật."""
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

API_BASE = os.getenv("SSI_API_BASE", "https://api.ssi.com.vn").rstrip("/")
# Chấp nhận cả tên biến cũ (SSI_CONSUMER_*) để không phải sửa .env
API_KEY = os.getenv("SSI_API_KEY") or os.getenv("SSI_CONSUMER_ID", "")
API_SECRET = os.getenv("SSI_API_SECRET") or os.getenv("SSI_CONSUMER_SECRET", "")
MIN_INTERVAL = float(os.getenv("SSI_MIN_INTERVAL", "0.2"))
DB_PATH = os.getenv("DB_PATH", "data/market.db")
# Đồng bộ dữ liệu từ máy chủ (GitHub Release) – chỉ dùng cho scripts/sync_db.py
GITHUB_REPO = os.getenv("GITHUB_REPO", "")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
TZ_NAME = os.getenv("TZ_NAME", "Asia/Ho_Chi_Minh")
HISTORY_START = os.getenv("HISTORY_START", "2018-01-01")
TZ = ZoneInfo(TZ_NAME)


def now_vn() -> datetime:
    """Thời điểm hiện tại theo múi giờ Việt Nam (UTC+7)."""
    return datetime.now(TZ)
