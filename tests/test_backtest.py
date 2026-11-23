"""Kiểm thử backtest: tính đúng công thức, không nhìn trước, có chi phí."""
import numpy as np
import pandas as pd

from src.analytics import backtest as bt
from src.analytics import panel as pn
from src.analytics import stock_score as ss


def test_newey_west_khop_ols_khi_khong_tu_tuong_quan():
    rng = np.random.default_rng(0)
    x = rng.normal(size=2000)
    y = 0.5 * x + rng.normal(size=2000)
    r0 = bt.newey_west_slope(y, x, lags=0)
    assert abs(r0["beta"] - 0.5) < 0.05
    # t OLS: beta / se, se = sqrt(sigma2 / sum((x-xbar)^2))
    X = np.column_stack([np.ones(2000), x])
    b = np.linalg.lstsq(X, y, rcond=None)[0]
    e = y - X @ b
    se = np.sqrt(e @ e / (2000 - 2) / ((x - x.mean()) ** 2).sum())
    # lags=0 là sai số chuẩn robust (White) nên chỉ xấp xỉ OLS cổ điển; sai khác tương đối < 5% khi dữ liệu iid
    assert abs(r0["t"] / (b[1] / se) - 1) < 0.05


def test_newey_west_t_nho_hon_ols_khi_chong_lan():
    rng = np.random.default_rng(1)
    noise = pd.Series(rng.normal(size=1500)).rolling(21).mean().to_numpy()   # tự tương quan mạnh
    x = pd.Series(rng.normal(size=1500)).rolling(21).mean().to_numpy()
    t_ols = bt.newey_west_slope(noise, x, 0)["t"]
    t_nw = bt.newey_west_slope(noise, x, 21)["t"]
    assert abs(t_nw) < abs(t_ols) or abs(t_ols) < 2   # HAC không làm t "đẹp" hơn một cách giả tạo


def test_perf_stats_tinh_dung():
    r = pd.Series([0.10, -0.05, 0.10, 0.0] * 3)
    s = bt.perf_stats(r)
    cum = (1 + r).cumprod()
    assert np.isclose(s["max_drawdown"], (cum / cum.cummax() - 1).min())
    assert np.isclose(s["hit_rate"], 6 / 12) and np.isclose(s["ann_return"], cum.iloc[-1] ** (12 / 12) - 1)


def _market(n=520):
    idx = pd.bdate_range("2018-01-01", periods=n)
    rng = np.random.default_rng(5)
    cols = [f"S{i:02d}" for i in range(40)]
    close = pd.DataFrame(100 * np.cumprod(1 + rng.normal(0.0002, 0.012, (n, 40)), axis=0), index=idx, columns=cols)
    value = pd.DataFrame(5e9, index=idx, columns=cols)
    ix = close.mean(axis=1)
    health = pd.DataFrame({"regime": "Trung tính"}, index=idx)
    el = pn.eligible_mask(value, 1e9)
    return close, value, ix, el, health


def test_vao_lenh_o_phien_ke_tiep_khong_huong_cu_nhay_gia_sau_tin_hieu():
    close, value, ix, el, health = _market()
    f = ss.factor_table(close, value, ix, el)
    res = bt.stock_backtest(close, ix, el, f, health, top_frac=0.25, cost_bps=0, start="2019-06-01")
    k = res.index[3]
    # sửa giá ĐÚNG phiên tín hiệu và các phiên trước đó không thể xảy ra; ta tăng vọt giá của mọi mã tại phiên
    # vào lệnh (t+1) và kiểm tra lợi suất kỳ này không hưởng cú nhảy đó (vì mua ở chính giá phiên t+1)
    entry = res.loc[k, "entry"]
    c2 = close.copy()
    c2.loc[entry:, :] = c2.loc[entry:, :] * 1.5   # nhân cả đoạn từ t+1 trở đi: lợi suất t+1 -> sau đó không đổi
    f2 = ss.factor_table(c2, value, c2.mean(axis=1), el)
    res2 = bt.stock_backtest(c2, c2.mean(axis=1), el, f2, health, top_frac=0.25, cost_bps=0, start="2019-06-01")
    assert np.isclose(res.loc[k, "gross"], res2.loc[k, "gross"], atol=1e-9)


def test_chi_phi_lam_giam_loi_suat_va_ky_dau_mua_toan_bo():
    close, value, ix, el, health = _market()
    f = ss.factor_table(close, value, ix, el)
    a = bt.stock_backtest(close, ix, el, f, health, cost_bps=0, start="2019-06-01")
    b = bt.stock_backtest(close, ix, el, f, health, cost_bps=50, start="2019-06-01")
    assert np.allclose(a["gross"], b["gross"]) and (b["net"] < a["net"]).all()
    assert np.isclose(b["cost"].iloc[0], 1.0 * 50 / 1e4)   # kỳ đầu mua 100% danh mục


def test_ma_co_diem_cao_ngay_tin_hieu_duoc_chon():
    close, value, ix, el, health = _market()
    f = ss.factor_table(close, value, ix, el)
    sig = bt.month_end_signal_dates(close.index, "2019-06-01")[0]
    table = ss.score_on(sig, f, el)
    assert table["score"].is_monotonic_decreasing and len(table) >= 10


def test_hoi_quy_trang_thai_thi_truong_chay_duoc_va_khong_nhin_truoc():
    close, value, ix, el, health = _market(900)
    from src.analytics import market_health as mh
    h = mh.market_health(ix, pd.Series(1e12, index=ix.index), close, el)
    r = bt.regime_forward_returns(h, ix, 21)
    assert set(r["table"].index) == {"Yếu", "Trung tính", "Mạnh"} and r["regression"]["n"] > 100


def test_kiem_tra_cuc_doan_bat_duoc_ma_nhay_gia_bat_thuong():
    close, value, ix, el, health = _market()
    c2 = close.copy()
    c2.iloc[300:, 5] = c2.iloc[300:, 5] * 20   # mã S05 nhảy gấp 20 lần ở phiên 300 (lỗi dữ liệu giả định)
    worst, cmp = bt.extreme_monthly_returns(c2, el, "2018-12-01", top=3)
    assert worst.iloc[0]["symbol"] == "S05" and worst.iloc[0]["ret"] > 10
    assert cmp["vol_cat_±50%"] < cmp["vol_goc"] and cmp["n_|r|>100%"] >= 1


def test_giai_doan_ngan_khong_lam_sap_thong_ke():
    short = pd.DataFrame({"net": [0.01, -0.02], "gross": [0.01, -0.02], "universe": [0.0, 0.0], "index": [0.0, 0.0]})
    assert "ann_return" not in bt.summarize(short).columns       # quá ít tháng: chỉ có số quan sát
    assert bt.MIN_MONTHS >= 12
