"""Điểm sức khỏe thị trường (0-100) từ 3 trụ cột: Xu hướng, Độ rộng, Thanh khoản.

Mỗi chỉ báo được chuẩn hóa bằng phân vị mở rộng (chỉ dùng quá khứ) rồi lấy trung bình; mỗi trụ cột có
trọng số bằng nhau (1/3) vì chưa có cơ sở lý thuyết để ưu tiên trụ nào. Trạng thái:
  < 33  -> "Yếu", 33-67 -> "Trung tính", > 67 -> "Mạnh"  (ba phần bằng nhau của thang phân vị).
Đây là công cụ mô tả trạng thái, không phải khuyến nghị đầu tư.
"""
import numpy as np
import pandas as pd

from src.finance import indicators as ind

LOW, HIGH = 33.0, 67.0


def label(score: float) -> str:
    if score is None or np.isnan(score):
        return "Chưa đủ dữ liệu"
    return "Yếu" if score < LOW else ("Mạnh" if score > HIGH else "Trung tính")


def trend_pillar(index_close: pd.Series, min_periods: int = 250) -> pd.Series:
    """Xu hướng của chỉ số: khoảng cách tới SMA50 và SMA200, và SMA50 so với SMA200."""
    parts = [ind.price_vs_sma(index_close, 50), ind.price_vs_sma(index_close, 200),
             ind.sma(index_close, 50) / ind.sma(index_close, 200) - 1.0]
    return pd.concat([ind.expanding_percentile(p, min_periods) for p in parts], axis=1).mean(axis=1, skipna=False)


def breadth_pillar(close: pd.DataFrame, eligible: pd.DataFrame, min_periods: int = 250) -> pd.DataFrame:
    """Độ rộng tự tính từ giá của các mã đủ thanh khoản.

    - pct_above_50: tỷ lệ mã có giá trên SMA50
    - adv_share: tỷ lệ mã tăng trong số mã tăng+giảm (bỏ mã đứng giá), trung bình 10 phiên
    """
    above = (close > ind.sma(close, 50)).where(eligible & ind.sma(close, 50).notna())
    pct_above = above.sum(axis=1) / above.notna().sum(axis=1).replace(0, np.nan)
    ret = ind.daily_returns(close).where(eligible)
    up, dn = (ret > 0).sum(axis=1), (ret < 0).sum(axis=1)
    adv_share = (up / (up + dn).replace(0, np.nan)).rolling(10, min_periods=5).mean()
    score = pd.concat([ind.expanding_percentile(pct_above, min_periods),
                       ind.expanding_percentile(adv_share, min_periods)], axis=1).mean(axis=1, skipna=False)
    return pd.DataFrame({"pct_above_50": pct_above, "adv_share_10d": adv_share, "breadth_score": score})


def liquidity_pillar(index_value: pd.Series, min_periods: int = 250) -> pd.Series:
    """Thanh khoản thị trường: trung vị giá trị khớp 20 phiên so với trung vị 250 phiên (tỷ số, không phụ thuộc đơn vị)."""
    ratio = index_value.rolling(20, min_periods=16).median() / index_value.rolling(250, min_periods=200).median()
    return ind.expanding_percentile(ratio, min_periods)


def market_health(index_close, index_value, close, eligible, min_periods: int = 250) -> pd.DataFrame:
    """Bảng theo ngày: 3 trụ cột (0-100), điểm tổng, trạng thái. Chỉ dùng dữ liệu tới từng ngày."""
    trend = trend_pillar(index_close, min_periods)
    br = breadth_pillar(close, eligible, min_periods)
    liq = liquidity_pillar(index_value, min_periods)
    out = pd.DataFrame({
        "index_close": index_close,
        "trend": trend * 100, "breadth": br["breadth_score"] * 100, "liquidity": liq * 100,
        "pct_above_50": br["pct_above_50"] * 100, "adv_share_10d": br["adv_share_10d"] * 100,
    }).dropna(subset=["index_close"])
    out["score"] = out[["trend", "breadth", "liquidity"]].mean(axis=1, skipna=False)
    out["regime"] = [label(s) for s in out["score"]]
    out["n_eligible"] = eligible.sum(axis=1).reindex(out.index)
    return out
