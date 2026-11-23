"""Kiểm thử các hàm phục vụ giao diện mới: giải thích thị trường, độ rộng, trạng thái WebSocket, áp lực mua/bán."""
from datetime import datetime

import numpy as np
import pandas as pd

from src.analytics import backtest as bt
from src.app import services as sv
from tests.test_services import add_trade, make_db


def test_market_summary_va_giai_thich(tmp_path):
    con, idx = make_db(tmp_path)
    a = sv.load_analysis(con, 1e6)
    m = sv.market_summary(a, idx[-1])
    assert 0 <= m["score"] <= 100 and 0 <= m["percentile"] <= 100
    assert np.isclose(sum(p["contribution"] for p in m["pillars"].values()), m["score"])   # 3 đóng góp cộng lại = điểm tổng
    lines = sv.explain_market(m)
    assert len(lines) >= 4 and any(m["regime"] in x for x in lines)
    assert not any(w in " ".join(lines).lower() for w in ("nên mua", "nên bán", "khuyến nghị mua"))


def test_dem_ma_tang_giam_dung_gia(tmp_path):
    con, idx = make_db(tmp_path)
    a = sv.load_analysis(con, 1e6)
    a["ret"].iloc[-1] = [0.01, -0.02]          # AAA tăng, BBB giảm
    a["ret"].iloc[-2] = [0.0, 0.0]             # cả hai đứng giá
    el = a["eligible"].copy()
    el.iloc[-2:] = True
    a["eligible"] = el
    bc = sv.breadth_counts(a)
    assert (bc.iloc[-1][["tang", "giam", "dung_gia"]] == [1, 1, 0]).all()
    assert (bc.iloc[-2][["tang", "giam", "dung_gia"]] == [0, 0, 2]).all()
    assert (bc["tong"] == bc[["tang", "giam", "dung_gia"]].sum(axis=1)).all()


def test_trang_thai_websocket(tmp_path):
    con, _ = make_db(tmp_path)
    assert sv.ws_state(sv.stream_status(con))["state"] == "Chưa khởi chạy"
    add_trade(con, "2026-11-23T10:00:00.000+07:00", "AAA", 100, 10, "B", 10, "2026/11/23 10:00:00.000")
    con.commit()
    st_ = lambda now: sv.ws_state(sv.stream_status(con, now), now)   # noqa: E731
    assert st_(datetime(2026, 11, 23, 10, 0, 30, tzinfo=sv.TZ))["state"] == "Đang nhận dữ liệu"
    lost = st_(datetime(2026, 11, 23, 10, 30, tzinfo=sv.TZ))           # thứ Hai, trong giờ giao dịch
    assert lost["state"] == "Mất kết nối" and lost["level"] == "warn"
    after = st_(datetime(2026, 11, 23, 16, 0, tzinfo=sv.TZ))           # sau giờ đóng cửa: bình thường
    assert after["state"] == "Đã dừng" and after["level"] == "info"
    weekend = st_(datetime(2026, 11, 29, 10, 30, tzinfo=sv.TZ))        # chủ nhật
    assert weekend["level"] == "info"


def test_mua_ban_rong_chi_hien_dung_chieu(tmp_path):
    p = pd.DataFrame({"rong": [5.0, 3.0, -1.0, -4.0, 0.0]}, index=list("ABCDE"))
    buy, sell = sv.top_pressure(p)
    assert list(buy.index) == ["A", "B"] and list(sell.index) == ["D", "C"]   # không lẫn mã bán ròng vào danh sách mua


def test_watchlist_va_dem_ban_tin(tmp_path):
    con, _ = make_db(tmp_path)
    add_trade(con, "2026-11-23T09:30:00.000+07:00", "AAA", 100, 10, "B", 10, "2026/11/23 09:30:00.000")
    add_trade(con, "2026-11-23T09:31:00.000+07:00", "AAA", 110, 10, "S", 20, "2026/11/23 09:31:00.000")
    con.commit()
    wl = sv.stream_watchlist(sv.stream_trades(con, "2026-11-23"))
    assert wl.loc["AAA", "gia_cuoi"] == 110 and np.isclose(wl.loc["AAA", "doi_%"], 10.0) and wl.loc["AAA", "so_lenh"] == 2
    assert sv.stream_counts(con, "2026-11-23").loc[0, "so_ban_tin"] == 2
    assert sv.stream_watchlist(sv.stream_trades(con, "2026-01-01")).empty


def test_ham_ho_tro_backtest():
    idx = pd.date_range("2019-06-30", periods=80, freq="ME")
    res = pd.DataFrame({"net": [0.01, 0.012] * 40, "gross": 0.012, "universe": 0.008, "index": 0.005, "cost": 0.002, "turnover": 0.4,
                        "n_held": 25, "n_ranked": 250, "regime": ["Yếu", "Trung tính", "Mạnh", "Trung tính"] * 20}, index=idx)
    ex = bt.excess_by_regime(res)
    assert np.isclose(ex.loc["Mạnh", "vuot_tb_pp"], 0.2) and ex["so_thang"].sum() == 80
    cs = bt.cost_summary(res)
    assert cs["so_lan_tai_can_bang"] == 80 and np.isclose(cs["vong_quay_tb"], 0.4)
    sp = bt.split_periods(res)
    assert all(v is not None for v in sp.values())
    short = bt.split_periods(res.iloc[:5])
    assert all(v is None for v in short.values())
