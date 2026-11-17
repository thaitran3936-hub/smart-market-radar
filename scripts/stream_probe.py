"""Thử streaming SSI và LƯU xuống SQLite (bằng chứng cho điều kiện WebSocket có lưu DB).

Chạy trong giờ giao dịch (9:00-11:30, 13:00-15:00), ví dụ:
    python scripts/stream_probe.py --channel MI:VNINDEX --seconds 60
    python scripts/stream_probe.py --channel X-QUOTE:ACB-VCB-SSI --seconds 60

Kênh theo tài liệu: MI:<chỉ số> | X-QUOTE:<mã-mã> | X-TRADE:<mã-mã> | X:<mã> | R:<mã> | B:<mã>
Tên mã chỉ số (VNINDEX hay VN-Index?) hãy lấy từ kết quả IndexList trong api_survey.
Cuối chạy in số dòng đã lưu: chụp màn hình làm bằng chứng (đừng để lộ key).
"""
import argparse
import os
import sqlite3
import time
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()
TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Ho_Chi_Minh"))
DB = os.getenv("DB_PATH", "data/market.db")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--seconds", type=int, default=60)
    args = ap.parse_args()

    from ssi_fc_data.fc_md_client import MarketDataClient
    from ssi_fc_data.fc_md_stream import MarketDataStream

    config = SimpleNamespace(
        auth_type="Bearer",
        consumerID=os.environ["SSI_CONSUMER_ID"],
        consumerSecret=os.environ["SSI_CONSUMER_SECRET"],
        url="https://fc-data.ssi.com.vn/",
        stream_url=os.getenv("SSI_STREAM_URL", "https://fc-datahub.ssi.com.vn/"),
    )

    os.makedirs(os.path.dirname(DB) or ".", exist_ok=True)
    con = sqlite3.connect(DB, check_same_thread=False)
    con.execute("""CREATE TABLE IF NOT EXISTS stream_raw(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        recv_time TEXT NOT NULL,      -- giờ VN (UTC+7), ISO
        channel TEXT NOT NULL,
        payload TEXT NOT NULL)""")
    count = {"n": 0, "err": 0}

    def on_msg(message):
        con.execute("INSERT INTO stream_raw(recv_time, channel, payload) VALUES (?,?,?)",
                    (datetime.now(TZ).isoformat(timespec="seconds"), args.channel, str(message)))
        con.commit()
        count["n"] += 1

    def on_err(error):
        count["err"] += 1
        print("Lỗi stream:", error)

    stream = MarketDataStream(config, MarketDataClient(config))
    stream.start(on_msg, on_err, args.channel)
    print(f"Đang nghe kênh {args.channel} trong {args.seconds}s ...")
    time.sleep(args.seconds)

    n = con.execute("SELECT COUNT(*), MIN(recv_time), MAX(recv_time) FROM stream_raw "
                    "WHERE channel=?", (args.channel,)).fetchone()
    print(f"Nhận {count['n']} thông điệp, lỗi {count['err']}.")
    print(f"Trong DB: {n[0]} dòng, từ {n[1]} đến {n[2]}  ({DB})")
    if count["n"] == 0:
        print("Không nhận được dữ liệu: kiểm tra giờ giao dịch, tên kênh, IP đã đăng ký, hoặc quota.")
    os._exit(0)  # luồng nền của thư viện có thể không tự thoát


if __name__ == "__main__":
    main()
