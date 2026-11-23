"""Chạy phân tích trên dữ liệu đã lưu và in kết quả (kiểm tra nhanh trước khi làm giao diện).

    python -m src.analytics.run                      # ngày mới nhất, ngưỡng thanh khoản 1 tỷ đồng/phiên
    python -m src.analytics.run --min-value 5e8 --top 20
"""
import argparse

import pandas as pd

from src import config
from src.analytics import market_health as mh
from src.analytics import panel as pn
from src.analytics import stock_score as ss
from src.data import db


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-value", type=float, default=1e9, help="ngưỡng trung vị giá trị khớp 60 phiên (đồng)")
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args(argv)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    con = db.connect(config.DB_PATH)
    p = pn.load_panel(con)
    el = pn.eligible_mask(p.value, args.min_value)
    health = mh.market_health(p.index_close, p.index_value, p.close, el)
    valid = health.dropna(subset=["score"])
    if valid.empty:
        print("Chưa tính được điểm thị trường: cần ≥ 250 phiên dữ liệu và có mã đủ thanh khoản.\n"
              f"Số mã đủ thanh khoản ngày cuối: {int(el.iloc[-1].sum())} (ngưỡng {args.min_value:,.0f} đồng/phiên). "
              "Thử giảm --min-value hoặc kiểm tra dữ liệu bằng: python -m src.data.collector --report")
        return 1
    last = valid.iloc[-1]
    print(f"\n=== SỨC KHỎE THỊ TRƯỜNG ngày {last.name:%d/%m/%Y} ===")
    print(f"Điểm: {last['score']:.1f}/100 -> {last['regime']}")
    print(f"  Xu hướng {last['trend']:.0f} | Độ rộng {last['breadth']:.0f} | Thanh khoản {last['liquidity']:.0f}")
    ic = p.index_close.dropna()
    s50, s200 = ic.rolling(50).mean().iloc[-1], ic.rolling(200).mean().iloc[-1]
    print(f"  VN-Index {ic.iloc[-1]:,.1f} | SMA50 {s50:,.1f} ({ic.iloc[-1] / s50 - 1:+.1%}) | "
          f"SMA200 {s200:,.1f} ({ic.iloc[-1] / s200 - 1:+.1%})")
    newest = p.close.dropna(how="all").index.max()
    if newest > last.name:
        print(f"  CẢNH BÁO: dữ liệu cổ phiếu mới hơn chỉ số ({newest:%d/%m/%Y} so với {last.name:%d/%m/%Y}); "
              "chạy: python -m src.data.collector --update --boards HOSE")
    print(f"  {last['pct_above_50']:.0f}% mã trên SMA50 | {last['adv_share_10d']:.0f}% tăng/(tăng+giảm) 10 phiên "
          f"| {int(last['n_eligible'])} mã đủ thanh khoản")
    print("\n10 phiên gần nhất:")
    print(health[["index_close", "trend", "breadth", "liquidity", "score", "regime"]].tail(10).round(1))
    f = ss.factor_table(p.close, p.value, p.index_close, el)
    top = ss.score_on(last.name, f, el)
    top = top.join(p.board.rename("san"))
    print(f"\n=== TOP {args.top} CỔ PHIẾU (ngày {last.name:%d/%m/%Y}, {len(top)} mã được xếp hạng) ===")
    print(top.head(args.top).round(1))
    print("\nLưu ý: công cụ phân tích học thuật, không phải khuyến nghị đầu tư.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
