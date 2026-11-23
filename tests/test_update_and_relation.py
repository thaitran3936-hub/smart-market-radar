"""Kiểm thử: lưu indexSummary hằng ngày, độ mới của dữ liệu, beta, và phân tích quan hệ thị trường - cổ phiếu (RQ3)."""
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

from src.analytics import backtest as bt
from src.analytics import panel as pn
from src.analytics import stock_score as ss
from src.app import services as sv
from src.data import collector, db
from src.finance import indicators as ind


ROW = {"tradingDate": "2026/11/18", "indexValue": "1703.01", "totalAdvanceStock": "120", "totalDeclineStock": "240",
       "totalNoChangeStock": "30", "totalCeilingStock": "5", "totalFloorStock": "9", "totalMatchValue": "1.7e13",
       "netPurchasesForeignValue": "-203400000000"}


def test_luu_index_summary_theo_ngay_va_ghi_de_khi_chay_lai(tmp_path):
    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)
    assert db.upsert_index_summary(con, "VNINDEX", ROW, "t1")
    assert db.upsert_index_summary(con, "VNINDEX", dict(ROW, totalAdvanceStock="125"), "t2")   # cùng ngày: ghi đè
    rows = con.execute("SELECT trade_date, advances, declines, ceilings, net_foreign_value FROM index_summary_daily").fetchall()
    assert rows == [("2026-11-18", 125, 240, 5, -203400000000.0)]
    assert not db.upsert_index_summary(con, "VNINDEX", {"indexValue": "1"}, "t")                # thiếu ngày: không lưu


def test_snapshot_loi_mot_chi_so_khong_lam_hong_ca_dot(tmp_path):
    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)

    class C:
        def index_summary(self, code):
            if code == "HNXINDEX":
                raise RuntimeError("HTTP 400")
            return ROW

        def drain_calls(self):
            return []

    saved, failed = collector.snapshot_index_summaries(con, C(), ["VNINDEX", "HNXINDEX"])
    assert saved == ["VNINDEX"] and failed[0][0] == "HNXINDEX"


def test_gio_cuoi_tuan_va_sau_15h_xac_dinh_dung_phien_ky_vong():
    f = lambda y, m, d, h, mi: sv.expected_last_session(datetime(y, m, d, h, mi, tzinfo=sv.TZ))  # noqa: E731
    assert str(f(2026, 11, 22, 14, 51)) == "2026-11-20"   # Chủ nhật -> thứ Sáu
    assert str(f(2026, 11, 23, 10, 0)) == "2026-11-20"    # thứ Hai trước 15h -> thứ Sáu
    assert str(f(2026, 11, 23, 15, 5)) == "2026-11-23"    # thứ Hai sau 15h -> chính hôm đó
    assert str(f(2026, 11, 21, 16, 0)) == "2026-11-20"    # thứ Bảy -> thứ Sáu


def test_canh_bao_du_lieu_cu_dem_dung_so_phien():
    now = datetime(2026, 11, 22, 14, 51, tzinfo=sv.TZ)       # Chủ nhật; phiên kỳ vọng: thứ Sáu 20/11
    assert sv.staleness("2026-11-20", now)["level"] == "ok"
    s = sv.staleness("2026-11-18", now)                      # thiếu 19 và 20
    assert s["level"] == "warn" and s["behind"] == 2 and "daily_update" in s["message"]
    assert sv.staleness(None, now)["level"] == "warn"


def test_beta_at_bang_he_so_hoi_quy_va_loai_ma_it_quan_sat():
    rng = np.random.default_rng(0)
    idx = pd.bdate_range("2020-01-01", periods=300)
    rm = pd.Series(rng.normal(0, 0.01, 300), index=idx)
    ret = pd.DataFrame({"A": 1.5 * rm + rng.normal(0, 0.002, 300), "B": -0.5 * rm, "C": np.nan}, index=idx)
    ret.loc[idx[:250], "B"] = np.nan                       # B chỉ có 50 quan sát trong cửa sổ 120
    b = ind.beta_at(ret, rm, idx[-1], window=120, min_obs=96)
    w = ret["A"].iloc[-120:]
    assert np.isclose(b["A"], np.polyfit(rm.iloc[-120:], w, 1)[0], atol=1e-9)
    assert np.isnan(b["B"]) and np.isnan(b["C"])


def _universe(n=1000, k=40, seed=4):
    idx = pd.bdate_range("2018-01-02", periods=n)
    rng = np.random.default_rng(seed)
    rm = pd.Series(rng.normal(0.0003, 0.01, n), index=idx)
    betas = np.linspace(0.3, 1.7, k)
    ret = pd.DataFrame({f"S{i:02d}": b * rm + rng.normal(0, 0.003, n) for i, b in enumerate(betas)}, index=idx)
    close = 100 * (1 + ret).cumprod()
    ix = 1000 * (1 + rm).cumprod()
    value = pd.DataFrame(5e9, index=idx, columns=close.columns)
    return close, value, ix, pd.DataFrame(True, index=idx, columns=close.columns)


def test_rq3_nhom_beta_cao_khuech_dai_bien_dong_thi_truong():
    close, value, ix, el = _universe()
    health = pd.DataFrame({"regime": "Trung tính"}, index=close.index)
    f = ss.factor_table(close, value, ix, el)
    rel = bt.relation_analysis(close, ix, el, f, health, start="2018-09-03")
    assert len(rel) > 20 and (rel["median_beta"].between(0.8, 1.2)).all()
    sr = bt.summarize_relation(rel)
    # nhóm beta cao hơn thấp ~ (1,5 - 0,5) lần thị trường: hệ số hồi quy gần 1 và rất có ý nghĩa
    assert 0.7 < sr["beta_slope"]["beta"] < 1.3 and sr["beta_slope"]["t"] > 5
    assert list(sr["by_regime"].index) == ["Trung tính"]


def test_rq3_khong_nhin_truoc_doi_gia_sau_tin_hieu_khong_doi_phan_nhom():
    close, value, ix, el = _universe()
    health = pd.DataFrame({"regime": "Trung tính"}, index=close.index)
    f = ss.factor_table(close, value, ix, el)
    a = bt.relation_analysis(close, ix, el, f, health, start="2018-09-03")
    d = a.index[5]
    c2 = close.copy()
    c2.loc[c2.index > d + pd.Timedelta(days=60), :] *= 3.0     # đổi giá rất xa sau ngày tín hiệu
    f2 = ss.factor_table(c2, value, ix, el)
    b = bt.relation_analysis(c2, ix, el, f2, health, start="2018-09-03")
    assert np.isclose(a.loc[d, "median_beta"], b.loc[d, "median_beta"])      # beta chỉ dùng quá khứ


def test_doi_chieu_do_rong_chinh_thuc_voi_tu_tinh(tmp_path):
    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)
    idx = pd.bdate_range("2026-11-16", periods=3)
    close = pd.DataFrame({"A": [10.0, 11, 12], "B": [10.0, 9, 8], "C": [10.0, 11, 10]}, index=idx)
    ret = ind.daily_returns(close)
    board = pd.Series({"A": "HOSE", "B": "HOSE", "C": "HOSE"})
    a = {"ret": ret, "panel": type("P", (), {"board": board})()}
    db.upsert_index_summary(con, "VNINDEX", dict(ROW, tradingDate="2026/11/17", totalAdvanceStock="2", totalDeclineStock="1"), "t")
    out = sv.breadth_crosscheck(con, a)
    assert len(out) == 1 and np.isclose(out["chinh_thuc"].iloc[0], 200 / 3) and np.isclose(out["tu_tinh"].iloc[0], 200 / 3)
