"""Kiểm thử các hàm tính toán chỉ báo và điểm (dữ liệu tổng hợp, kết quả biết trước)."""
import numpy as np
import pandas as pd

from src.analytics import market_health as mh
from src.analytics import panel as pn
from src.analytics import stock_score as ss
from src.finance import indicators as ind


def dates(n):
    return pd.bdate_range("2018-01-01", periods=n)


def test_return_va_sma_va_momentum_theo_cong_thuc():
    c = pd.DataFrame({"A": [100.0, 110.0, 121.0, 133.1]}, index=dates(4))
    assert np.allclose(ind.daily_returns(c)["A"].iloc[1:], 0.10)
    assert np.isclose(ind.price_vs_sma(c, 2)["A"].iloc[-1], 133.1 / ((121 + 133.1) / 2) - 1)
    assert np.isclose(ind.momentum(c, 3)["A"].iloc[-1], 0.331)
    # bỏ 1 phiên gần nhất: close[t-1]/close[t-3] - 1
    assert np.isclose(ind.momentum(c, 3, skip=1)["A"].iloc[-1], 121 / 100 - 1)


def test_max_drawdown_va_volatility():
    c = pd.DataFrame({"A": [100, 120, 90, 100, 110.0]}, index=dates(5))
    assert np.isclose(ind.rolling_max_drawdown(c, 5)["A"].iloc[-1], 90 / 120 - 1)
    flat = pd.DataFrame({"A": [0.0] * 70}, index=dates(70))
    assert ind.volatility(flat, 60)["A"].iloc[-1] == 0


def test_beta_bang_he_so_khi_ma_gap_doi_chi_so():
    rng = np.random.default_rng(0)
    rm = pd.Series(rng.normal(0, 0.01, 300), index=dates(300))
    r = pd.DataFrame({"A": 2 * rm, "B": -1 * rm})
    b = ind.rolling_beta(r, rm, 120).iloc[-1]
    assert np.isclose(b["A"], 2) and np.isclose(b["B"], -1)


def test_phan_vi_mo_rong_khong_nhin_truoc_tuong_lai():
    rng = np.random.default_rng(1)
    s = pd.Series(rng.normal(size=400), index=dates(400))
    a = ind.expanding_percentile(s, 250)
    s2 = s.copy()
    s2.iloc[300:] += 100  # đổi toàn bộ tương lai sau ngày 300
    b = ind.expanding_percentile(s2, 250)
    assert np.allclose(a.iloc[:300].dropna(), b.iloc[:300].dropna())
    inc = ind.expanding_percentile(pd.Series(np.arange(300.0), index=dates(300)), 250)
    assert inc.iloc[-1] > 0.99


def test_xep_hang_cheo_va_huong_tot_xau():
    x = pd.DataFrame({"A": [1.0], "B": [2.0], "C": [3.0]})
    assert ind.cross_section_pct(x).iloc[0].idxmax() == "C"
    assert ind.cross_section_pct(x, higher_is_better=False).iloc[0].idxmax() == "A"


def test_mat_na_thanh_khoan_loai_ma_it_giao_dich():
    n = 80
    v = pd.DataFrame({"LIQ": [5e9] * n, "ILLIQ": [1e6] * n}, index=dates(n))
    m = pn.eligible_mask(v, 1e9, 60)
    assert bool(m["LIQ"].iloc[-1]) and not bool(m["ILLIQ"].iloc[-1])


def test_trang_thai_thi_truong_theo_nguong():
    assert mh.label(20) == "Yếu" and mh.label(50) == "Trung tính" and mh.label(80) == "Mạnh"
    assert mh.label(float("nan")) == "Chưa đủ dữ liệu"


def _synthetic(n=600, up=True):
    idx = dates(n)
    rng = np.random.default_rng(2)
    drift = 0.0008 if up else -0.0008
    close = pd.DataFrame({f"S{i}": 100 * np.cumprod(1 + rng.normal(drift, 0.01, n)) for i in range(30)}, index=idx)
    value = pd.DataFrame(2e9, index=idx, columns=close.columns)
    ix = close.mean(axis=1)
    return close, value, ix, pd.Series(2e12, index=idx)


def test_diem_thi_truong_cao_khi_tang_deu_thap_khi_giam_deu():
    res = {}
    for up in (True, False):
        close, value, ix, ixv = _synthetic(up=up)
        el = pn.eligible_mask(value, 1e9)
        h = mh.market_health(ix, ixv, close, el)
        res[up] = h["score"].dropna().iloc[-1]
    assert res[True] > res[False]


def test_diem_co_phieu_xep_ma_manh_hon_len_tren():
    n = 400
    idx = dates(n)
    close = pd.DataFrame({
        "TOT": 100 * np.cumprod(np.full(n, 1.002)),     # tăng đều, ít biến động
        "XAU": 100 * np.cumprod(np.full(n, 0.998)),     # giảm đều
    }, index=idx)
    value = pd.DataFrame({"TOT": 5e9, "XAU": 5e9}, index=idx)
    ix = close.mean(axis=1)
    el = pn.eligible_mask(value, 1e9)
    f = ss.factor_table(close, value, ix, el)
    t = ss.score_on(idx[-1], f, el)
    assert list(t.index) == ["TOT", "XAU"] and t["score"].between(0, 100).all()


def test_loai_ma_it_giao_dich_du_trung_vi_cao():
    n = 80
    v = pd.DataFrame({"DEU": [5e9] * n, "THUA": [0.0, 1e11] * (n // 2)}, index=dates(n))  # giao dịch 50% số phiên
    m = pn.eligible_mask(v, 1e9, 60, min_traded_share=0.8)
    assert bool(m["DEU"].iloc[-1]) and not bool(m["THUA"].iloc[-1])


def test_trong_so_mac_dinh_bang_nhau():
    assert set(ss.DEFAULT_WEIGHTS.values()) == {0.25}
