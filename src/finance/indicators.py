"""Chỉ báo tài chính thuần túy (không đọc DB, không gọi API).

Quy ước: mọi bảng là DataFrame "rộng": index = ngày giao dịch (tăng dần), cột = mã.
KHÔNG nhìn trước tương lai: giá trị tại ngày t chỉ dùng dữ liệu tới hết ngày t.
Giá close đã điều chỉnh ngược (xem docs/api_survey.md) nên lợi suất tính từ close là lợi suất đã
điều chỉnh cổ tức/tách cổ phiếu; match_val (giá trị khớp) là số gốc, không điều chỉnh.
"""
import numpy as np
import pandas as pd

TRADING_DAYS = 252


def daily_returns(close: pd.DataFrame) -> pd.DataFrame:
    """Lợi suất đơn giản theo ngày từ giá đóng cửa."""
    return close.pct_change(fill_method=None)


def sma(x: pd.DataFrame | pd.Series, n: int):
    return x.rolling(n, min_periods=n).mean()


def price_vs_sma(close, n: int):
    """Khoảng cách giá so với trung bình động n phiên: close/SMA(n) - 1 (xu hướng)."""
    return close / sma(close, n) - 1.0


def momentum(close, n: int, skip: int = 0):
    """Động lượng n phiên, bỏ `skip` phiên gần nhất (đảo chiều ngắn hạn): close[t-skip]/close[t-n] - 1."""
    return close.shift(skip) / close.shift(n) - 1.0


def volatility(ret, n: int = 60):
    """Độ lệch chuẩn lợi suất n phiên, quy năm hóa. Yêu cầu tối thiểu 80% số quan sát."""
    return ret.rolling(n, min_periods=int(n * 0.8)).std() * np.sqrt(TRADING_DAYS)


def rolling_max_drawdown(close, n: int = 126):
    """Sụt giảm tối đa trong n phiên gần nhất (số âm): min(close / max chạy - 1)."""
    # Đỉnh tính trên cửa sổ n phiên tới t (min_periods=1 để các phiên đầu cửa sổ ngoài vẫn có đỉnh),
    # rồi lấy mức sụt sâu nhất trong n phiên; cần ≥ 80% số quan sát hợp lệ mới có giá trị.
    run_max = close.rolling(n, min_periods=1).max()
    return (close / run_max - 1.0).rolling(n, min_periods=int(n * 0.8)).min()


def median_traded_value(value, n: int = 60):
    """Trung vị giá trị khớp n phiên (đồng). Dùng trung vị để không bị ảnh hưởng bởi dòng lỗi lẻ."""
    return value.rolling(n, min_periods=int(n * 0.8)).median()


def relative_strength(close, bench_close: pd.Series, n: int = 126):
    """Sức mạnh tương đối: lợi suất n phiên của mã trừ lợi suất n phiên của chỉ số chuẩn (điểm %, dạng số)."""
    return momentum(close, n) - momentum(bench_close, n).values[:, None]


def rolling_beta(ret, bench_ret: pd.Series, n: int = 120):
    """Beta của mã so với chỉ số: Cov(r, rm) / Var(rm) trên n phiên gần nhất."""
    min_p = int(n * 0.8)
    rm = bench_ret.reindex(ret.index)
    var_m = rm.rolling(n, min_periods=min_p).var()
    out = {}
    for c in ret.columns:
        out[c] = ret[c].rolling(n, min_periods=min_p).cov(rm) / var_m
    return pd.DataFrame(out, index=ret.index)


def cross_section_pct(x: pd.DataFrame, higher_is_better: bool = True, mask: pd.DataFrame | None = None):
    """Thứ hạng phần trăm (0-1) giữa các mã trong cùng một ngày; chỉ xét các mã thuộc `mask`."""
    if mask is not None:
        x = x.where(mask)
    r = x.rank(axis=1, pct=True)
    return r if higher_is_better else (1.0 - r).where(r.notna())


def expanding_percentile(s: pd.Series, min_periods: int = 250) -> pd.Series:
    """Phân vị (0-1) của giá trị tại t so với TOÀN BỘ quá khứ tới t (gồm cả t); không nhìn trước.

    Dùng để chuẩn hóa chỉ báo về cùng thang mà không cần biết tương lai."""
    v = s.to_numpy(dtype=float)
    out = np.full(len(v), np.nan)
    for i in range(len(v)):
        if np.isnan(v[i]) or i + 1 < min_periods:
            continue
        past = v[: i + 1]
        past = past[~np.isnan(past)]
        if len(past) < min_periods:
            continue
        out[i] = ((past < v[i]).sum() + 0.5 * (past == v[i]).sum()) / len(past)
    return pd.Series(out, index=s.index)


def beta_at(ret: pd.DataFrame, bench_ret: pd.Series, date, window: int = 120, min_obs: int = 96) -> pd.Series:
    """Beta của từng mã so với chỉ số tại MỘT ngày: Cov(r, rm) / Var(rm) trên `window` phiên tới hết ngày đó.

    Tính theo cặp quan sát hợp lệ của từng mã (mã mới niêm yết có ít dữ liệu hơn); thiếu `min_obs` thì trả NaN."""
    w = ret.loc[:date].tail(window)
    m = bench_ret.reindex(w.index).to_numpy()[:, None]
    v = w.to_numpy(dtype=float)
    mask = ~np.isnan(v) & ~np.isnan(m)
    n = mask.sum(axis=0).astype(float)
    vz = np.where(mask, v, 0.0)
    mz = np.where(mask, m, 0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        sm, sv = mz.sum(axis=0) / n, vz.sum(axis=0) / n
        cov = (mz * vz).sum(axis=0) / n - sm * sv
        var = (mz * mz).sum(axis=0) / n - sm * sm
        beta = cov / var
    beta[(n < min_obs) | ~np.isfinite(beta)] = np.nan
    return pd.Series(beta, index=ret.columns)
