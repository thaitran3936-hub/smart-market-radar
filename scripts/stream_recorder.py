"""Bộ ghi streaming SSI vào SQLite (bằng chứng cho điều kiện "WebSocket có lưu dữ liệu").

Cách dùng: đặt file này CẠNH sample_10_websocket_data.py (trong thư mục python của repo
ssi-fastconnect-v3-tutorials) để dùng chung config.py / auth_helper.py của SSI, rồi chạy:

    python stream_recorder.py --db "C:\\duong\\dan\\du-an-smart-market-radar\\data\\market.db" --minutes 5

Chỉ SUBSCRIBE dữ liệu thị trường (không OTP, không đặt lệnh).
Dữ liệu ghi vào bảng stream_raw(recv_time, msg_type, symbol, payload JSON), giờ Asia/Ho_Chi_Minh.
Kết thúc in thống kê để chụp màn hình làm bằng chứng.
"""
import argparse
import dataclasses
import json
import os
import sqlite3
import threading
import time
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

from auth_helper import ensure_auth
from config import config
from ssi_sdk import Auth, Stream

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
SCHEMA = """
CREATE TABLE IF NOT EXISTS stream_raw(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_time TEXT NOT NULL,     -- giờ Việt Nam (UTC+7), ISO, mili giây
    msg_type TEXT NOT NULL,      -- TradeMessage / IndexTickMessage / QuoteMessage / ForeignRoomMessage ...
    symbol TEXT,
    payload TEXT NOT NULL        -- toàn bộ trường của thông điệp, dạng JSON
);
CREATE INDEX IF NOT EXISTS ix_stream_raw_time ON stream_raw(recv_time);
CREATE INDEX IF NOT EXISTS ix_stream_raw_sym ON stream_raw(symbol, recv_time);
"""


def to_dict(msg) -> dict:
    """Chuyển thông điệp của SDK (pydantic/dataclass/object) thành dict, không làm mất trường nào."""
    if isinstance(msg, dict):  # thông điệp điều khiển (vd phản hồi subscribe) đến dưới dạng dict thuần
        return msg
    for name in ("model_dump", "dict", "to_dict"):
        fn = getattr(msg, name, None)
        if callable(fn):
            try:
                d = fn()
                if isinstance(d, dict):
                    return d
            except Exception:
                pass
    if dataclasses.is_dataclass(msg) and not isinstance(msg, type):
        return dataclasses.asdict(msg)
    if hasattr(msg, "__dict__"):
        return {k: v for k, v in vars(msg).items() if not k.startswith("_")}
    return {"repr": repr(msg)}


class Sink:
    """Nhận thông điệp từ callback của SDK, gom lô rồi ghi SQLite (an toàn đa luồng)."""

    def __init__(self, db_path: str, keep_quotes: bool, batch: int = 100, every: float = 2.0):
        folder = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(folder, exist_ok=True)
        self.con = sqlite3.connect(db_path, check_same_thread=False)
        self._migrate_legacy_table()
        self.con.executescript(SCHEMA)
        self.keep_quotes = keep_quotes
        self.batch, self.every = batch, every
        self.buf: list[tuple] = []
        self.lock = threading.Lock()
        self.last_flush = time.time()
        self.received = Counter()   # đếm mọi thông điệp nhận được
        self.saved = 0
        self.heartbeats = 0
        self.errors = 0

    def _migrate_legacy_table(self) -> None:
        """Nếu DB đã có bảng stream_raw kiểu cũ (thiếu cột), đổi tên để giữ lại rồi tạo bảng mới."""
        cols = {r[1] for r in self.con.execute("PRAGMA table_info(stream_raw)")}
        if cols and not {"recv_time", "msg_type", "symbol", "payload"} <= cols:
            legacy = f"stream_raw_legacy_{int(time.time())}"
            self.con.execute(f"ALTER TABLE stream_raw RENAME TO {legacy}")
            self.con.commit()
            print(f"Bảng stream_raw cũ (cột: {sorted(cols)}) đã được đổi tên thành {legacy}.")

    def on_data(self, msg) -> None:
        try:
            is_ctl = isinstance(msg, dict)
            kind = "Control" if is_ctl else type(msg).__name__
            self.received[kind] += 1
            if kind == "QuoteMessage" and not self.keep_quotes:
                return
            row = (datetime.now(TZ).isoformat(timespec="milliseconds"), kind,
                   msg.get("symbol") if is_ctl else getattr(msg, "symbol", None),
                   json.dumps(to_dict(msg), ensure_ascii=False, default=str))
            with self.lock:
                self.buf.append(row)
                due = len(self.buf) >= self.batch or time.time() - self.last_flush >= self.every
            if due:
                self.flush()
        except Exception as e:  # không để lỗi ghi làm chết luồng nhận dữ liệu
            self.errors += 1
            print("Lỗi xử lý thông điệp:", type(e).__name__, e)

    def on_heartbeat(self, msg) -> None:
        self.heartbeats += 1
        self.flush()

    def flush(self) -> None:
        with self.lock:
            rows, self.buf = self.buf, []
            self.last_flush = time.time()
        if not rows:
            return
        try:
            with self.lock:
                self.con.executemany(
                    "INSERT INTO stream_raw(recv_time, msg_type, symbol, payload) VALUES (?,?,?,?)", rows)
                self.con.commit()
            self.saved += len(rows)
        except Exception as e:
            self.errors += 1
            print("Lỗi ghi DB:", type(e).__name__, e)

    def status_line(self) -> str:
        total = sum(self.received.values())
        return (f"[{datetime.now(TZ):%H:%M:%S}] nhận {total} | đã lưu {self.saved} | "
                f"heartbeat {self.heartbeats} | lỗi {self.errors}")

    def summary(self) -> None:
        self.flush()
        print("\n=== BẰNG CHỨNG STREAMING ĐÃ LƯU DB ===")
        with self.lock:
            n, t0, t1 = self.con.execute(
                "SELECT COUNT(*), MIN(recv_time), MAX(recv_time) FROM stream_raw").fetchone()
            by_type = self.con.execute(
                "SELECT msg_type, COUNT(*) FROM stream_raw GROUP BY msg_type ORDER BY 2 DESC").fetchall()
            by_sym = self.con.execute(
                "SELECT symbol, COUNT(*) FROM stream_raw GROUP BY symbol ORDER BY 2 DESC LIMIT 8").fetchall()
            sample = self.con.execute(
                "SELECT recv_time, msg_type, symbol, substr(payload,1,110) FROM stream_raw "
                "ORDER BY id DESC LIMIT 3").fetchall()
        print(f"Tổng dòng trong stream_raw: {n} (từ {t0} đến {t1})")
        print("Theo loại:", by_type)
        print("Theo mã:", by_sym)
        print("3 dòng mới nhất:")
        for r in sample:
            print("  ", r)
        print("Mỗi loại thông điệp, dòng mới nhất (xem các trường thật):")
        with self.lock:
            for (kind,) in self.con.execute("SELECT DISTINCT msg_type FROM stream_raw").fetchall():
                r = self.con.execute(
                    "SELECT recv_time, symbol, substr(payload,1,260) FROM stream_raw "
                    "WHERE msg_type=? ORDER BY id DESC LIMIT 1", (kind,)).fetchone()
                print(f"  [{kind}] {r}")
        print("Nhận trong lần chạy này:", dict(self.received), "| bỏ qua quote:", not self.keep_quotes)
        if n == 0:
            print("KHÔNG có dữ liệu: kiểm tra giờ giao dịch (9:00-11:30, 13:00-14:45) và cấu hình.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH", "market_stream.db"),
                    help="đường dẫn file SQLite (nên trỏ tới data/market.db của dự án)")
    ap.add_argument("--minutes", type=float, default=5, help="thời gian ghi (phút)")
    ap.add_argument("--symbols", default="SSI,HPG,VNM,VCB,FPT", help="mã cổ phiếu, cách nhau dấu phẩy")
    ap.add_argument("--index-ticks", default="VNINDEX", help="chỉ số lấy tick, cách nhau dấu phẩy")
    ap.add_argument("--index-members", default="VN30",
                    help="chỉ số mà ta nhận dữ liệu của CÁC MÃ THÀNH PHẦN (rỗng nếu không cần)")
    ap.add_argument("--keep-quotes", action="store_true", help="lưu cả bảng giá bid/ask (rất nhiều dòng)")
    args = ap.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    indexes = [s.strip().upper() for s in args.index_ticks.split(",") if s.strip()]
    members = [s.strip().upper() for s in args.index_members.split(",") if s.strip()]
    sink = Sink(args.db, args.keep_quotes)
    deadline = time.time() + args.minutes * 60

    with Auth(config) as auth:
        ensure_auth(auth)  # market data không cần OTP
        with Stream(auth) as stream:
            print("Đang kết nối WebSocket...")
            stream.streaming.connect()
            print("Đã kết nối!")
            stream.streaming.on_data = sink.on_data
            stream.streaming.on_heartbeat = sink.on_heartbeat
            if symbols:
                stream.streaming.subscribe_symbol(symbols)
            if members:
                stream.streaming.subscribe_index(members)
            if indexes:
                stream.streaming.subscribe_index_ticks(indexes)
            print(f"Ghi {args.minutes:g} phút: mã {symbols}, thành phần {members}, "
                  f"tick chỉ số {indexes} -> {args.db}")
            print("Ctrl+C để dừng sớm.\n")

            fails = 0
            try:
                while time.time() < deadline:
                    slice_ms = int(max(1.0, min(15.0, deadline - time.time())) * 1000)
                    try:
                        stream.streaming.wait(timeout_ms=slice_ms)
                        fails = 0
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        fails += 1
                        print("Lỗi khi chờ dữ liệu:", type(e).__name__, e)
                        if fails >= 10:
                            print("Quá nhiều lỗi liên tiếp, dừng.")
                            break
                        time.sleep(2)
                    sink.flush()
                    print(sink.status_line())
                    time.sleep(0.05)
            except KeyboardInterrupt:
                print("\nNgắt theo yêu cầu...")
            finally:
                sink.summary()


if __name__ == "__main__":
    main()
