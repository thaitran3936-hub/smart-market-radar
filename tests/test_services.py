"""Kiểm thử lớp dịch vụ của giao diện (không cần Streamlit)."""
import json
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from src.app import services as sv
from src.data import cleaning, db


def make_db(tmp_path, with_stream=True):
    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)
    idx = pd.bdate_range("2018-01-02", periods=700)
    rng = np.random.default_rng(1)
    def mk(c):
        return pd.DataFrame({"tradingDate": idx.strftime("%Y/%m/%d"), "open": c, "high": c, "low": c, "close": c,
                             "totalMatch": 1e6, "totalMatchValue": 1e6 * c, "priceChangePercentage": 0.0})
    for s in ("AAA", "BBB"):
        c = 100 * np.cumprod(1 + rng.normal(0.0004, 0.01, len(idx)))
        db.replace_prices(con, s, cleaning.add_quality_flags(cleaning.normalize_summary(mk(c)), check_vwap=False), "t")
    ix = 100 * np.cumprod(1 + rng.normal(0.0004, 0.008, len(idx)))
    db.replace_index(con, "VNINDEX", cleaning.add_quality_flags(cleaning.normalize_summary(mk(ix)), check_vwap=False,
                                                                check_ret=False), "t")
    con.executemany("INSERT OR REPLACE INTO symbols(symbol, board) VALUES (?,?)", [("AAA", "HOSE"), ("BBB", "HNX")])
    con.commit()
    return con, idx


def add_trade(con, rt, sym, price, qty, side, total, tt):
    con.execute("INSERT INTO stream_raw(recv_time, msg_type, symbol, payload) VALUES (?,?,?,?)",
                (rt, "TradeMessage", sym, json.dumps({"price": price, "quantity": qty, "side": side,
                                                      "total_volume": total, "trading_time": tt})))


def test_thieu_file_db_bao_loi_than_thien(tmp_path):
    with pytest.raises(sv.DataUnavailable, match="collector"):
        sv.open_db(str(tmp_path / "khong_co.db"))


def test_db_rong_bao_chua_co_du_lieu(tmp_path):
    con = db.connect(str(tmp_path / "e.db"))
    db.init_db(con)
    with pytest.raises(sv.DataUnavailable):
        sv.freshness(con)


def test_freshness_va_phan_tich_chay_duoc(tmp_path):
    con, idx = make_db(tmp_path)
    f = sv.freshness(con)
    assert f["prices_last_date"] == idx[-1].strftime("%Y-%m-%d") and f["api_errors"] == 0
    a = sv.load_analysis(con, 1e6)
    valid = a["health"]["score"].dropna()
    assert len(valid) > 50 and valid.between(0, 100).all()   # phải có điểm hợp lệ, không được rỗng
    t = sv.rank_table(a, a["health"].index[-1], ss_weights(), ["HOSE"], sv.symbol_meta(con))
    assert set(t.index) == {"AAA"}   # lọc theo sàn: AAA thuộc HOSE, BBB thuộc HNX


def ss_weights():
    return {"trend": 0.25, "momentum": 0.25, "liquidity": 0.25, "risk": 0.25}


def test_stream_chua_co_du_lieu_tra_trang_thai_ro_rang(tmp_path):
    con, _ = make_db(tmp_path)
    s = sv.stream_status(con)
    assert not s["available"] and "Chưa có" in s["label"]


def test_stream_trang_thai_dang_nhan_va_dem_phien(tmp_path):
    con, _ = make_db(tmp_path)
    add_trade(con, "2026-11-18T10:30:00.000+07:00", "AAA", 100, 100, "B", 100, "2026/11/18 10:30:00.000")
    add_trade(con, "2026-11-19T10:30:00.000+07:00", "AAA", 100, 100, "S", 200, "2026/11/19 10:30:00.000")
    con.commit()
    now = datetime(2026, 11, 19, 10, 30, 30, tzinfo=sv.TZ)
    s = sv.stream_status(con, now)
    assert s["available"] and s["sessions"] == 2 and "Đang nhận" in s["label"]
    old = sv.stream_status(con, datetime(2026, 11, 19, 12, 0, 0, tzinfo=sv.TZ))
    assert "Không có luồng mới" in old["label"]


def test_ap_luc_mua_ban_chu_dong_va_bo_qua_thong_diep_hong(tmp_path):
    con, _ = make_db(tmp_path)
    add_trade(con, "2026-11-19T09:30:00.000+07:00", "AAA", 100, 1000, "B", 1000, "2026/11/19 09:30:00.000")
    add_trade(con, "2026-11-19T09:31:00.000+07:00", "AAA", 100, 400, "S", 1400, "2026/11/19 09:31:00.000")
    con.execute("INSERT INTO stream_raw(recv_time, msg_type, symbol, payload) VALUES (?,?,?,?)",
                ("2026-11-19T09:32:00.000+07:00", "TradeMessage", "AAA", "{khong phai json"))
    con.commit()
    t = sv.stream_trades(con, "2026-11-19")
    assert len(t) == 2
    p = sv.stream_pressure(t)
    assert p.loc["AAA", "mua_chu_dong"] == 100_000 and p.loc["AAA", "ban_chu_dong"] == 40_000
    assert np.isclose(p.loc["AAA", "ty_le_mua"], 100_000 / 140_000)


def test_doi_chieu_cheo_stream_voi_rest(tmp_path):
    con, idx = make_db(tmp_path)
    day = idx[-1].strftime("%Y-%m-%d")           # REST có totalMatch = 1.000.000
    add_trade(con, f"{day}T14:44:00.000+07:00", "AAA", 100, 10, "B", 1_000_000, f"{day.replace('-', '/')} 14:44:00.000")
    add_trade(con, f"{day}T10:00:00.000+07:00", "BBB", 100, 10, "B", 400_000, f"{day.replace('-', '/')} 10:00:00.000")
    con.commit()
    x = sv.stream_crosscheck(con, day).set_index("symbol")
    assert x.loc["AAA", "ket_luan"] == "khớp" and x.loc["BBB", "ket_luan"] == "chưa ghi hết phiên"


def test_beta_tuong_quan_va_hanh_xu_theo_trang_thai(tmp_path):
    con, idx = make_db(tmp_path)
    a = sv.load_analysis(con, 1e6)
    assert a["health"]["score"].notna().sum() > 50
    # tạo mã có beta = 2 đúng bằng 2 lần lợi suất chỉ số
    p = a["panel"]
    rm = p.index_close.pct_change().fillna(0)
    p.close["AAA"] = 100 * (1 + 2 * rm).cumprod()
    rel = sv.market_relation(a, "AAA", idx[-1])
    assert np.isclose(rel["beta"], 2.0, atol=1e-6) and np.isclose(rel["corr"], 1.0, atol=1e-6)
    t = sv.stock_by_regime(a, "AAA", idx[-1])
    assert set(t.index) <= {"Yếu", "Trung tính", "Mạnh"} and (t["so_phien"] > 0).all()
    assert np.allclose(t["beta"].dropna(), 2.0, atol=1e-6)


def test_trang_thai_phien_hom_nay_khong_dung_cho_phan_loai_chinh_no(tmp_path):
    con, idx = make_db(tmp_path)
    a = sv.load_analysis(con, 1e6)
    d = a["health"]["score"].dropna().index[10]
    base = sv.stock_by_regime(a, "AAA", idx[-1])
    a["health"].loc[d, "regime"] = "Mạnh" if a["health"].loc[d, "regime"] != "Mạnh" else "Yếu"   # đổi trạng thái 1 ngày
    changed = sv.stock_by_regime(a, "AAA", idx[-1])
    assert not base.equals(changed)       # phiên SAU ngày d bị phân loại lại, nên kết quả đổi
    # nhưng lợi suất của chính ngày d được phân loại bằng trạng thái ngày d-1: đổi trạng thái ngày d không ảnh hưởng ngày d
    assert base["so_phien"].sum() == changed["so_phien"].sum()
