"""Điểm cổ phiếu (0-100) tại một ngày, xếp hạng giữa các mã đủ thanh khoản.

4 nhóm yếu tố, mỗi nhóm là trung bình thứ hạng phần trăm của các chỉ báo con (hướng "tốt" ghi rõ):
  Xu hướng (25%):  giá/SMA50 - 1 (cao tốt), giá/SMA200 - 1 (cao tốt)
  Động lượng (25%): động lượng 126 phiên bỏ 21 phiên gần nhất (cao tốt), sức mạnh tương đối 63 phiên so với VN-Index (cao tốt)
  Thanh khoản (25%): trung vị giá trị khớp 60 phiên (cao tốt)
  Rủi ro (25%):    biến động 60 phiên (thấp tốt), sụt giảm tối đa 126 phiên (nông tốt)
Trọng số mặc định bằng nhau (25% mỗi nhóm), theo lập luận 1/N của DeMiguel, Garlappi & Uppal (2009): không
tối ưu hóa trọng số trên dữ liệu. Người dùng chỉnh được; báo cáo kiểm tra độ nhạy.
"""
import pandas as pd

from src.finance import indicators as ind

DEFAULT_WEIGHTS = {"trend": 0.25, "momentum": 0.25, "liquidity": 0.25, "risk": 0.25}


def factor_table(close, value, index_close, eligible, asof=None) -> dict[str, pd.DataFrame]:
    """Các chỉ báo con (bảng rộng) chưa xếp hạng, tính tới hết ngày."""
    ret = ind.daily_returns(close)
    return {
        "trend_50": ind.price_vs_sma(close, 50), "trend_200": ind.price_vs_sma(close, 200),
        "mom_126_21": ind.momentum(close, 126, skip=21),
        "rs_63": ind.relative_strength(close, index_close.reindex(close.index), 63),
        "liq_med": ind.median_traded_value(value, 60),
        "vol_60": ind.volatility(ret, 60), "mdd_126": ind.rolling_max_drawdown(close, 126),
    }


def score_on(date, f: dict[str, pd.DataFrame], eligible, weights=None) -> pd.DataFrame:
    """Bảng điểm một ngày (mỗi hàng một mã), sắp theo điểm giảm dần."""
    w = weights or DEFAULT_WEIGHTS
    el = eligible.loc[date]
    row = {k: v.loc[date].where(el) for k, v in f.items()}
    pct = lambda s, up=True: s.rank(pct=True) if up else (1 - s.rank(pct=True)).where(s.notna())  # noqa: E731
    groups = {
        "trend": pd.concat([pct(row["trend_50"]), pct(row["trend_200"])], axis=1).mean(axis=1, skipna=False),
        "momentum": pd.concat([pct(row["mom_126_21"]), pct(row["rs_63"])], axis=1).mean(axis=1, skipna=False),
        "liquidity": pct(row["liq_med"]),
        "risk": pd.concat([pct(row["vol_60"], up=False), pct(row["mdd_126"])], axis=1).mean(axis=1, skipna=False),
    }
    out = pd.DataFrame({k: v * 100 for k, v in groups.items()})
    out["score"] = sum(out[k] * w[k] for k in w) / sum(w.values())
    out = out.join(pd.DataFrame({"vol_60_%": row["vol_60"] * 100, "mdd_126_%": row["mdd_126"] * 100,
                                 "mom_126_21_%": row["mom_126_21"] * 100, "rs_63_pp": row["rs_63"] * 100,
                                 "liq_median_ty": row["liq_med"] / 1e9}))
    return out.dropna(subset=["score"]).sort_values("score", ascending=False)
